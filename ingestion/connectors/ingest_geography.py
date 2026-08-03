"""
Bronze: data/geography.csv -> bronze.geography

Chạy:
    make lake-ingest-geography
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import BronzeJob, run_job

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


# Bảng dimension nhỏ (~40k dòng, 1.4MB) và không có cột ngày. Partition chỉ tạo ra nhiều
# file tí hon, chậm hơn để nguyên một file -> bỏ partition_by (mặc định None).
JOB = BronzeJob(
    table="geography",
    source_csv="geography.csv",
    schema=SCHEMA,
    transform=transform,
)

if __name__ == "__main__":
    run_job(JOB)
