"""
Bronze: data/sales.csv -> iceberg.bronze.sales_daily

Doanh thu/giá vốn TỔNG HỢP SẴN theo ngày, do nguồn cung cấp — không phải bảng giao dịch.
Tên bảng là sales_daily (không phải sales) để không ai nhầm nó với order_items.

LƯU Ý: số ở đây KHÔNG khớp với doanh thu tính từ order_items (xem docs/them-bang-moi.md).
Đừng "sửa" cho khớp — đây là hai nguồn số độc lập, chênh lệch giữa chúng là thứ cần điều tra.

Cột nguồn viết hoa (Date, Revenue, COGS) và `Date` trùng từ khoá SQL nên phải đổi tên khi
đọc — xem giải thích ở SCHEMA bên dưới.

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_sales_daily.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-sales-daily"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.sales_daily"
SOURCE_CSV = "/opt/spark/data/sales.csv"


def partition_columns():
    """Bảng rất nhỏ (~3.8k dòng, 128KB) — mỗi ngày đúng 1 dòng. Để nguyên một file."""
    return None


# --- Business logic của bảng sales_daily ------------------------------------

# Tên cột ở đây KHÁC header CSV (Date, Revenue, COGS) một cách có chủ ý:
#   - `Date` trùng từ khoá SQL, dùng làm tên cột sẽ phải quote ở mọi câu query.
#   - Repo dùng snake_case cho mọi cột; giữ PascalCase riêng bảng này sẽ thành ngoại lệ khó nhớ.
# Spark áp schema theo THỨ TỰ CỘT và bỏ qua tên trong header (enforceSchema mặc định true),
# nên đổi tên ở đây là đủ — nhưng cũng có nghĩa: nếu nguồn đổi thứ tự cột, dữ liệu sẽ vào
# nhầm cột mà không có lỗi nào báo. Đó là cái giá của việc đổi tên, chấp nhận được với file
# tĩnh 3 cột như thế này.
SCHEMA = StructType([
    StructField("sale_date", DateType(), False),
    StructField("revenue", DoubleType(), True),
    StructField("cogs", DoubleType(), True),
])


def transform(df: DataFrame) -> DataFrame:
    """Gắn cờ chất lượng. Bảng không có cột text nào cần chuẩn hoá.
    Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("sale_date").isNull(), F.lit("sale_date_missing")),
        F.when(F.col("revenue").isNull() | (F.col("revenue") < 0), F.lit("revenue_invalid")),
        F.when(F.col("cogs").isNull() | (F.col("cogs") < 0), F.lit("cogs_invalid")),
    )

    # KHÔNG gắn cờ lỗi cho cogs > revenue: 382/3833 ngày (10%) bán lỗ. Với tỷ lệ đó thì đây
    # là sự thật kinh doanh, không phải lỗi dữ liệu. Thay vì loại chúng, cho ra một cột riêng
    # để phân tích được — đây là thông tin, không phải rác.
    return (
        df
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
        .withColumn("_margin_negative", F.col("cogs") > F.col("revenue"))
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest sales CSV vào bronze layer")
    parser.add_argument("--source-csv", default=SOURCE_CSV)
    parser.add_argument("--table", default=TABLE)
    return parser.parse_args()


def run(spark: SparkSession, source_csv: str, table: str, logger) -> None:
    logger.info("Đọc %s", source_csv)
    df = read_csv(spark, source_csv, SCHEMA)

    bronze_df = add_audit_columns(transform(df), source_csv)

    create_namespace(spark, NAMESPACE)
    logger.info("Ghi bảng %s", table)
    write_iceberg_table(bronze_df, table, partition_columns())

    total = count_table_rows(spark, table)
    invalid = spark.table(table).filter("not _is_valid").count()
    loss_days = spark.table(table).filter("_margin_negative").count()
    logger.info("%s: %s dòng (hợp lệ=%s, lỗi=%s), %s ngày bán lỗ",
                table, total, total - invalid, invalid, loss_days)


def main():
    args = parse_args()
    logger = get_logger("bronze.sales_daily")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
