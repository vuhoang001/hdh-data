"""
Bronze: data/returns.csv -> iceberg.bronze.returns

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_returns.py
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

APP_NAME = "hdh-bronze-returns"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.returns"
SOURCE_CSV = "/opt/spark/data/returns.csv"


def partition_columns():
    """Partition theo năm: 40k dòng trải 2012-2023, theo tháng sẽ ra ~140 partition với mỗi
    file ~15KB (quá nhỏ). Gọi trong hàm vì F.years() cần SparkContext đã khởi tạo."""
    return [F.years("return_date")]


# --- Business logic của bảng returns ----------------------------------------

SCHEMA = StructType([
    StructField("return_id", StringType(), False),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("return_date", DateType(), True),
    StructField("return_reason", StringType(), True),
    StructField("return_quantity", IntegerType(), True),
    StructField("refund_amount", DoubleType(), True),
])

VALID_RETURN_REASONS = [
    "changed_mind", "defective", "late_delivery", "not_as_described", "wrong_size",
]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("return_id", F.trim(F.col("return_id")))
        .withColumn("return_reason", F.lower(F.trim(F.col("return_reason"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("order_id").isNull(), F.lit("order_id_missing")),
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("return_date").isNull(), F.lit("return_date_missing")),
        F.when(F.col("return_quantity").isNull() | (F.col("return_quantity") <= 0),
               F.lit("return_quantity_invalid")),
        F.when(F.col("refund_amount").isNull() | (F.col("refund_amount") < 0),
               F.lit("refund_amount_invalid")),
        F.when(~F.col("return_reason").isin(*VALID_RETURN_REASONS), F.lit("reason_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest returns CSV vào bronze layer")
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
    logger = get_logger("bronze.returns")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
