"""
Bronze: data/web_traffic.csv -> iceberg.bronze.web_traffic

Lưu lượng web tổng hợp theo ngày + nguồn traffic.

Chạy:
    docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_web_traffic.py
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

APP_NAME = "hdh-bronze-web-traffic"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.web_traffic"
SOURCE_CSV = "/opt/spark/data/web_traffic.csv"


def partition_columns():
    """Bảng nhỏ (~3.7k dòng, 204KB). Để nguyên một file."""
    return None


# --- Business logic của bảng web_traffic ------------------------------------

# `date` ở header nguồn được đổi thành `traffic_date`: `date` vừa là tên kiểu dữ liệu vừa là
# từ khoá SQL, dùng làm tên cột sẽ phải quote ở mọi câu query. Spark áp schema theo thứ tự cột
# và bỏ qua tên header, nên đổi tên ở đây là đủ.
SCHEMA = StructType([
    StructField("traffic_date", DateType(), False),
    StructField("sessions", IntegerType(), True),
    StructField("unique_visitors", IntegerType(), True),
    StructField("page_views", IntegerType(), True),
    StructField("bounce_rate", DoubleType(), True),
    StructField("avg_session_duration_sec", DoubleType(), True),
    StructField("traffic_source", StringType(), True),
])

VALID_TRAFFIC_SOURCES = [
    "direct", "email_campaign", "organic_search", "paid_search", "referral", "social_media",
]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = df.withColumn("traffic_source", F.lower(F.trim(F.col("traffic_source"))))

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("traffic_date").isNull(), F.lit("traffic_date_missing")),
        F.when(F.col("sessions").isNull() | (F.col("sessions") < 0), F.lit("sessions_invalid")),
        F.when(F.col("unique_visitors") < 0, F.lit("unique_visitors_negative")),
        F.when(F.col("page_views") < 0, F.lit("page_views_negative")),
        # Một người có thể vào nhiều phiên, nhưng một phiên không thể có nhiều người.
        # Khách duy nhất > số phiên là bất khả thi -> chắc chắn lỗi đo lường.
        F.when(F.col("unique_visitors") > F.col("sessions"), F.lit("visitors_above_sessions")),
        # Số trang xem không thể ít hơn số phiên: mỗi phiên xem ít nhất 1 trang.
        F.when(F.col("page_views") < F.col("sessions"), F.lit("page_views_below_sessions")),
        F.when(~F.col("bounce_rate").between(0, 1), F.lit("bounce_rate_out_of_range")),
        F.when(F.col("avg_session_duration_sec") < 0, F.lit("duration_negative")),
        F.when(~F.col("traffic_source").isin(*VALID_TRAFFIC_SOURCES), F.lit("source_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# --- Orchestration ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Ingest web_traffic CSV vào bronze layer")
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
    logger = get_logger("bronze.web_traffic")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
