"""
Bronze: data/products.csv -> iceberg.bronze.products

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_products.py
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

APP_NAME = "hdh-bronze-products"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.products"
SOURCE_CSV = "/opt/spark/data/products.csv"


def partition_columns():
    """Bảng dimension rất nhỏ (~2.4k dòng, 192KB), không có cột ngày. Để nguyên một file."""
    return None


# --- Business logic của bảng products ---------------------------------------

SCHEMA = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("size", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("cogs", DoubleType(), True),
])

VALID_CATEGORIES = ["casual", "genz", "outdoor", "streetwear"]
VALID_SEGMENTS = [
    "activewear", "all-weather", "balanced", "everyday",
    "performance", "premium", "standard", "trendy",
]
VALID_SIZES = ["s", "m", "l", "xl"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        # product_name giữ nguyên hoa/thường: tên thương mại, không phải mã phân loại
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("size", F.lower(F.trim(F.col("size"))))
        .withColumn("color", F.lower(F.trim(F.col("color"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("product_name").isNull(), F.lit("product_name_missing")),
        F.when(F.col("price").isNull() | (F.col("price") <= 0), F.lit("price_invalid")),
        F.when(F.col("cogs").isNull() | (F.col("cogs") < 0), F.lit("cogs_invalid")),
        # Giá vốn cao hơn giá bán = bán lỗ. Có thể là thật (xả hàng) nhưng thường là lỗi nhập liệu,
        # nên gắn cờ để người dùng silver tự quyết định.
        F.when(F.col("cogs") > F.col("price"), F.lit("cogs_above_price")),
        F.when(~F.col("category").isin(*VALID_CATEGORIES), F.lit("category_unknown")),
        F.when(~F.col("segment").isin(*VALID_SEGMENTS), F.lit("segment_unknown")),
        F.when(~F.col("size").isin(*VALID_SIZES), F.lit("size_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest products CSV vào bronze layer")
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
    logger = get_logger("bronze.products")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
