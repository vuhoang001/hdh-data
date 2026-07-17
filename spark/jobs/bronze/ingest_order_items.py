import argparse


from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import (
    add_audit_columns, 
    build_spark_session,
    count_table_rows, 
    create_namespace,
    get_logger, 
    read_csv, 
    write_iceberg_table
)

APP_NAME = "hdh-bronze-order-items"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.order_items"
SOURCE_CSV = "/opt/spark/data/order_items.csv"

def partition_columns(): 
    """order_items không có cột ngày để partition. Bucket theo order_id giữ file cân đối và giúp join với ordersorders. Gọi trong hàm vì F.bucket() cần SparkContext để khởi tạo."""
    return [F.bucket(16, "order_id")]

    
SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("promo_id", StringType(), True),
    StructField("promo_id_2", StringType(), True),
])


def transform(df: DataFrame) -> DataFrame: 
    """Chuẩn hóa text + gắn cờ chất lượng. Không lọc bỏ dòng - bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
            .withColumn("promo_id", F.nullif(F.trim(F.col("promo_id")), F.lit("")))
            .withColumn("promo_id_2", F.nullif(F.trim(F.col("promo_id_2")), F.lit("")))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity_invalid")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("unit_price_invalid")),
        F.when(F.col("discount_amount") < 0, F.lit("discount_negative")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest order_items CSV vào bronze layer")
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
    logger = get_logger("bronze.order_items")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

    
