"""
Bronze: data/reviews.csv -> bronze.reviews

Chạy:
    make lake-ingest-reviews
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

MIN_RATING = 1
MAX_RATING = 5

SCHEMA = StructType([
    StructField("review_id", StringType(), False),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("review_date", DateType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_title", StringType(), True),
])


def partition_columns():
    """Partition theo năm: 114k dòng trải 2012-2023, theo tháng sẽ ra ~140 partition với mỗi
    file ~45KB (quá nhỏ). Gọi trong hàm vì F.years() cần SparkContext đã khởi tạo."""
    return [F.years("review_date")]


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


JOB = BronzeJob(
    table="reviews",
    source_csv="reviews.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
