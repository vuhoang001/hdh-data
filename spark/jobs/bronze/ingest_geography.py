"""
Bronze: data/geography.csv -> iceberg.bronze.geography

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_geography.py
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import (
    add_audit_columns,
    build_spark_session,
    count_table_rows,
    create_namespace,
    get_logger,
    read_csv,
    write_iceberg_table,
)

APP_NAME = "hdh-bronze-geography"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.geography"
SOURCE_CSV = "/opt/spark/data/geography.csv"


def partition_columns():
    """Bảng dimension nhỏ (~40k dòng, 1.4MB) và không có cột ngày. Partition chỉ tạo ra
    nhiều file tí hon, chậm hơn để nguyên một file."""
    return None


# --- Business logic của bảng geography --------------------------------------

SCHEMA = StructType([
    StructField("zip", StringType(), False),
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("district", StringType(), True),
])

VALID_REGIONS = ["central", "east", "west"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("zip", F.trim(F.col("zip")))
        # city/district giữ nguyên hoa/thường: danh từ riêng, không phải mã phân loại
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("district", F.trim(F.col("district")))
        .withColumn("region", F.lower(F.trim(F.col("region"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("zip").isNull(), F.lit("zip_missing")),
        F.when(F.col("city").isNull(), F.lit("city_missing")),
        F.when(~F.col("region").isin(*VALID_REGIONS), F.lit("region_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest geography CSV vào bronze layer")
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
    logger = get_logger("bronze.geography")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
