"""
Bronze: data/payments.csv -> iceberg.bronze.payments

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_payments.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-payments"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.payments"
SOURCE_CSV = "/opt/spark/data/payments.csv"


def partition_columns():
    """Không có cột ngày. Bucket theo order_id giữ file cân đối và gom các dòng cùng order_id
    vào chung nhóm nên join với orders đỡ shuffle. Gọi trong hàm vì F.bucket() cần SparkContext."""
    return [F.bucket(16, "order_id")]


# --- Business logic của bảng payments ---------------------------------------

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("payment_method", StringType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("installments", IntegerType(), True),
])

VALID_PAYMENT_METHODS = ["apple_pay", "bank_transfer", "cod", "credit_card", "paypal"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = df.withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("payment_value").isNull() | (F.col("payment_value") <= 0),
               F.lit("payment_value_invalid")),
        F.when(F.col("installments").isNull() | (F.col("installments") < 1),
               F.lit("installments_invalid")),
        F.when(~F.col("payment_method").isin(*VALID_PAYMENT_METHODS), F.lit("method_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest payments CSV vào bronze layer")
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
    logger = get_logger("bronze.payments")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
