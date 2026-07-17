"""
Bronze: data/inventory.csv -> iceberg.bronze.inventory

Ảnh chụp tồn kho theo tháng cho từng sản phẩm. Nguồn đã phi chuẩn hoá sẵn (product_name,
category, segment lặp lại từ products) và có sẵn cột dẫn xuất (year, month, các cờ) —
bronze giữ nguyên như nguồn, việc bỏ cột thừa là chuyện của silver.

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_inventory.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-inventory"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.inventory"
SOURCE_CSV = "/opt/spark/data/inventory.csv"


def partition_columns():
    """Partition theo năm: 60k dòng trải 2012-2022. Đây vốn là snapshot theo tháng nên
    F.months() sẽ ra ~130 partition với file ~40KB (quá nhỏ). Gọi trong hàm vì F.years()
    cần SparkContext đã khởi tạo."""
    return [F.years("snapshot_date")]


# --- Business logic của bảng inventory --------------------------------------

SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("stock_on_hand", IntegerType(), True),
    StructField("units_received", IntegerType(), True),
    StructField("units_sold", IntegerType(), True),
    StructField("stockout_days", IntegerType(), True),
    StructField("days_of_supply", DoubleType(), True),
    StructField("fill_rate", DoubleType(), True),
    StructField("stockout_flag", IntegerType(), True),
    StructField("overstock_flag", IntegerType(), True),
    StructField("reorder_flag", IntegerType(), True),
    StructField("sell_through_rate", DoubleType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
])

VALID_CATEGORIES = ["casual", "genz", "outdoor", "streetwear"]
VALID_SEGMENTS = [
    "activewear", "all-weather", "balanced", "everyday",
    "performance", "premium", "standard", "trendy",
]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("stock_on_hand") < 0, F.lit("stock_on_hand_negative")),
        F.when(F.col("units_received") < 0, F.lit("units_received_negative")),
        F.when(F.col("units_sold") < 0, F.lit("units_sold_negative")),
        F.when(F.col("stockout_days") < 0, F.lit("stockout_days_negative")),
        # fill_rate và sell_through_rate là tỷ lệ -> bắt buộc nằm trong [0, 1]
        F.when(~F.col("fill_rate").between(0, 1), F.lit("fill_rate_out_of_range")),
        F.when(~F.col("sell_through_rate").between(0, 1), F.lit("sell_through_out_of_range")),
        # Các cờ chỉ được nhận 0 hoặc 1
        F.when(~F.col("stockout_flag").isin(0, 1), F.lit("stockout_flag_invalid")),
        F.when(~F.col("overstock_flag").isin(0, 1), F.lit("overstock_flag_invalid")),
        F.when(~F.col("reorder_flag").isin(0, 1), F.lit("reorder_flag_invalid")),
        # year/month là cột dẫn xuất từ snapshot_date. Lệch nhau nghĩa là nguồn tính sai,
        # và mọi report nhóm theo year/month sẽ ra số sai mà không ai biết.
        F.when(F.col("year") != F.year("snapshot_date"), F.lit("year_mismatch")),
        F.when(F.col("month") != F.month("snapshot_date"), F.lit("month_mismatch")),
        F.when(~F.col("category").isin(*VALID_CATEGORIES), F.lit("category_unknown")),
        F.when(~F.col("segment").isin(*VALID_SEGMENTS), F.lit("segment_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest inventory CSV vào bronze layer")
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
    logger.info("%s: %s dòng (hợp lệ=%s, lỗi=%s)", table, total, total - invalid, invalid)


def main():
    args = parse_args()
    logger = get_logger("bronze.inventory")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
