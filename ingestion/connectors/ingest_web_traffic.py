"""
Bronze: data/web_traffic.csv -> bronze.web_traffic

Lưu lượng web tổng hợp theo ngày + nguồn traffic.

Chạy:
    make lake-ingest-web_traffic
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

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


# Bảng nhỏ (~3.7k dòng, 204KB) -> để nguyên một file.
JOB = BronzeJob(
    table="web_traffic",
    source_csv="web_traffic.csv",
    schema=SCHEMA,
    transform=transform,
)

if __name__ == "__main__":
    run_job(JOB)
