"""
Bronze: data/promotions.csv -> bronze.promotions

Chạy:
    make lake-ingest-promotions
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

MAX_PERCENTAGE = 100

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


# Bảng dimension cực nhỏ (50 dòng) — partition ở đây hoàn toàn vô nghĩa.
JOB = BronzeJob(
    table="promotions",
    source_csv="promotions.csv",
    schema=SCHEMA,
    transform=transform,
)

if __name__ == "__main__":
    run_job(JOB)
