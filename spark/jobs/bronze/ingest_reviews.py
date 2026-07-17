"""
Bronze: data/reviews.csv -> iceberg.bronze.reviews

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_reviews.py
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

APP_NAME = "hdh-bronze-reviews"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.reviews"
SOURCE_CSV = "/opt/spark/data/reviews.csv"

MIN_RATING = 1
MAX_RATING = 5


def partition_columns():
    """Partition theo năm: 114k dòng trải 2012-2023, theo tháng sẽ ra ~140 partition với mỗi
    file ~45KB (quá nhỏ). Gọi trong hàm vì F.years() cần SparkContext đã khởi tạo."""
    return [F.years("review_date")]


# --- Business logic của bảng reviews ----------------------------------------

SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("review_date", DateType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_title", StringType(), True),
])


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("review_id", F.trim(F.col("review_id")))
        # review_title giữ nguyên hoa/thường: đây là text người dùng viết, không phải mã phân loại
        .withColumn("review_title", F.nullif(F.trim(F.col("review_title")), F.lit("")))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("order_id").isNull(), F.lit("order_id_missing")),
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("customer_id").isNull(), F.lit("customer_id_missing")),
        F.when(F.col("review_date").isNull(), F.lit("review_date_missing")),
        F.when(
            F.col("rating").isNull() | ~F.col("rating").between(MIN_RATING, MAX_RATING),
            F.lit("rating_out_of_range"),
        ),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest reviews CSV vào bronze layer")
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
    logger = get_logger("bronze.reviews")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
