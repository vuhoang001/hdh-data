"""
Bronze: data/returns.csv -> bronze.returns

Chạy:
    make lake-ingest-returns
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("return_id", StringType(), False),
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("return_date", DateType(), True),
    StructField("return_reason", StringType(), True),
    StructField("return_quantity", IntegerType(), True),
    StructField("refund_amount", DoubleType(), True),
])

VALID_RETURN_REASONS = [
    "changed_mind", "defective", "late_delivery", "not_as_described", "wrong_size",
]


def partition_columns():
    """Partition theo năm: 40k dòng trải 2012-2023, theo tháng sẽ ra ~140 partition với mỗi
    file ~15KB (quá nhỏ). Gọi trong hàm vì F.years() cần SparkContext đã khởi tạo."""
    return [F.years("return_date")]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("return_id", F.trim(F.col("return_id")))
        .withColumn("return_reason", F.lower(F.trim(F.col("return_reason"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("order_id").isNull(), F.lit("order_id_missing")),
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("return_date").isNull(), F.lit("return_date_missing")),
        F.when(F.col("return_quantity").isNull() | (F.col("return_quantity") <= 0),
               F.lit("return_quantity_invalid")),
        F.when(F.col("refund_amount").isNull() | (F.col("refund_amount") < 0),
               F.lit("refund_amount_invalid")),
        F.when(~F.col("return_reason").isin(*VALID_RETURN_REASONS), F.lit("reason_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="returns",
    source_csv="returns.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
