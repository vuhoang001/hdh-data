"""
Bronze: data/customers.csv -> bronze.customers

Chạy:
    make lake-ingest-customers
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("zip", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("acquisition_channel", StringType(), True),
])

VALID_GENDERS = ["female", "male", "non-binary"]
VALID_AGE_GROUPS = ["18-24", "25-34", "35-44", "45-54", "55+"]
VALID_CHANNELS = [
    "direct", "email_campaign", "organic_search", "paid_search", "referral", "social_media",
]


def partition_columns():
    """Partition theo năm, không theo tháng: 122k dòng trải 2012-2023 nên theo tháng sẽ ra
    ~140 partition với mỗi file ~50KB (quá nhỏ). Theo năm còn ~11 partition, mỗi file ~600KB.
    Gọi trong hàm vì F.years() cần SparkContext đã khởi tạo."""
    return [F.years("signup_date")]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("zip", F.trim(F.col("zip")))
        # city giữ nguyên hoa/thường: đây là danh từ riêng ("Hai Phong"), không phải mã phân loại
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("gender", F.lower(F.trim(F.col("gender"))))
        .withColumn("age_group", F.lower(F.trim(F.col("age_group"))))
        .withColumn("acquisition_channel", F.lower(F.trim(F.col("acquisition_channel"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("signup_date").isNull(), F.lit("signup_date_missing")),
        F.when(F.col("zip").isNull(), F.lit("zip_missing")),
        F.when(~F.col("gender").isin(*VALID_GENDERS), F.lit("gender_unknown")),
        F.when(~F.col("age_group").isin(*VALID_AGE_GROUPS), F.lit("age_group_unknown")),
        F.when(~F.col("acquisition_channel").isin(*VALID_CHANNELS), F.lit("channel_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="customers",
    source_csv="customers.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
