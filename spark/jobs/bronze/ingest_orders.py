"""
Bronze: data/orders.csv -> iceberg.bronze.orders

Mọi logic riêng của bảng orders (schema, chuẩn hoá, rule chất lượng) nằm trong file này.
Phần hạ tầng (SparkSession, đọc CSV, ghi Iceberg lên MinIO) lấy từ package common/.

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_orders.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-orders"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.orders"
SOURCE_CSV = "/opt/spark/data/orders.csv"


def partition_columns():
    """Partition theo tháng, không theo ngày: dữ liệu trải 2012-2023 nên partition theo ngày
    sẽ tạo ~3800 partition cho ~650k dòng (mỗi file vài chục KB) và làm writer OOM.
    Gọi trong hàm vì F.months() cần SparkContext đã khởi tạo."""
    return [F.months("order_date")]

# --- Business logic của bảng orders -----------------------------------------

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("zip", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("order_source", StringType(), True),
])

VALID_STATUS_VALUES = ["created", "paid", "shipped", "delivered", "returned", "cancelled"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("zip", F.trim(F.col("zip")))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
        .withColumn("order_source", F.lower(F.trim(F.col("order_source"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("customer_id").isNull(), F.lit("customer_id_missing")),
        F.when(F.col("order_date").isNull(), F.lit("order_date_missing")),
        F.when(~F.col("order_status").isin(*VALID_STATUS_VALUES), F.lit("status_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest orders CSV vào bronze layer")
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
    logger = get_logger("bronze.orders")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
