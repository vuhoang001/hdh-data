"""
Bronze: data/shipments.csv -> iceberg.bronze.shipments

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_shipments.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-shipments"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.shipments"
SOURCE_CSV = "/opt/spark/data/shipments.csv"


def partition_columns():
    """Partition theo tháng như orders: 566k dòng trải 2012-2023, theo ngày sẽ ra ~3800
    partition với file vài chục KB và làm writer OOM. Gọi trong hàm vì F.months() cần
    SparkContext đã khởi tạo."""
    return [F.months("ship_date")]


# --- Business logic của bảng shipments --------------------------------------

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("ship_date", DateType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("shipping_fee", DoubleType(), True),
])


def transform(df: DataFrame) -> DataFrame:
    """Gắn cờ chất lượng. Bảng không có cột text nào cần chuẩn hoá.
    Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("ship_date").isNull(), F.lit("ship_date_missing")),
        # Giao trước khi gửi là bất khả thi về mặt vật lý -> chắc chắn lỗi dữ liệu
        F.when(F.col("delivery_date") < F.col("ship_date"), F.lit("delivery_before_ship")),
        F.when(F.col("shipping_fee").isNull() | (F.col("shipping_fee") < 0),
               F.lit("shipping_fee_invalid")),
    )

    return (
        df
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest shipments CSV vào bronze layer")
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
    logger = get_logger("bronze.shipments")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
