"""
Bronze: data/promotions.csv -> iceberg.bronze.promotions

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_promotions.py
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

APP_NAME = "hdh-bronze-promotions"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.promotions"
SOURCE_CSV = "/opt/spark/data/promotions.csv"

MAX_PERCENTAGE = 100


def partition_columns():
    """Bảng dimension cực nhỏ (50 dòng). Partition ở đây hoàn toàn vô nghĩa."""
    return None


# --- Business logic của bảng promotions -------------------------------------

SCHEMA = StructType([
    StructField("promo_id", StringType(), False),
    StructField("promo_name", StringType(), True),
    StructField("promo_type", StringType(), True),
    StructField("discount_value", DoubleType(), True),
    StructField("start_date", DateType(), True),
    StructField("end_date", DateType(), True),
    StructField("applicable_category", StringType(), True),
    StructField("promo_channel", StringType(), True),
    StructField("stackable_flag", IntegerType(), True),
    StructField("min_order_value", DoubleType(), True),
])

VALID_PROMO_TYPES = ["fixed", "percentage"]
VALID_CHANNELS = ["all_channels", "email", "in_store", "online", "social_media"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("promo_id", F.trim(F.col("promo_id")))
        # promo_name giữ nguyên hoa/thường: tên chiến dịch, không phải mã phân loại
        .withColumn("promo_name", F.trim(F.col("promo_name")))
        .withColumn("promo_type", F.lower(F.trim(F.col("promo_type"))))
        .withColumn("promo_channel", F.lower(F.trim(F.col("promo_channel"))))
        # applicable_category rỗng (80% số dòng) nghĩa là "áp dụng mọi category", KHÔNG phải
        # thiếu dữ liệu. Quy về NULL để chỉ có một cách biểu diễn "không giới hạn category".
        .withColumn("applicable_category",
                    F.nullif(F.lower(F.trim(F.col("applicable_category"))), F.lit("")))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("start_date").isNull(), F.lit("start_date_missing")),
        F.when(F.col("end_date").isNull(), F.lit("end_date_missing")),
        # Kết thúc trước khi bắt đầu -> khoảng thời gian rỗng, khuyến mãi không bao giờ chạy
        F.when(F.col("end_date") < F.col("start_date"), F.lit("end_before_start")),
        F.when(F.col("discount_value").isNull() | (F.col("discount_value") <= 0),
               F.lit("discount_value_invalid")),
        # Giảm giá quá 100% nghĩa là trả tiền cho khách để họ mua hàng
        F.when(
            (F.col("promo_type") == "percentage") & (F.col("discount_value") > MAX_PERCENTAGE),
            F.lit("percentage_above_100"),
        ),
        F.when(F.col("min_order_value") < 0, F.lit("min_order_value_negative")),
        F.when(~F.col("promo_type").isin(*VALID_PROMO_TYPES), F.lit("promo_type_unknown")),
        F.when(~F.col("promo_channel").isin(*VALID_CHANNELS), F.lit("promo_channel_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest promotions CSV vào bronze layer")
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
    logger = get_logger("bronze.promotions")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
