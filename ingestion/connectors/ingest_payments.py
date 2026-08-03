"""
Bronze: data/payments.csv -> bronze.payments

Chạy:
    make lake-ingest-payments
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("payment_method", StringType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("installments", IntegerType(), True),
])

VALID_PAYMENT_METHODS = ["apple_pay", "bank_transfer", "cod", "credit_card", "paypal"]


def partition_columns():
    """Không có cột ngày. Bucket theo order_id giữ file cân đối và gom các dòng cùng order_id
    vào chung nhóm nên join với orders đỡ shuffle. Gọi trong hàm vì F.bucket() cần SparkContext."""
    return [F.bucket(16, "order_id")]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = df.withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("payment_value").isNull() | (F.col("payment_value") <= 0),
               F.lit("payment_value_invalid")),
        F.when(F.col("installments").isNull() | (F.col("installments") < 1),
               F.lit("installments_invalid")),
        F.when(~F.col("payment_method").isin(*VALID_PAYMENT_METHODS), F.lit("method_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="payments",
    source_csv="payments.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
