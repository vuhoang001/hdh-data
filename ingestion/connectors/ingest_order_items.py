"""
Bronze: data/order_items.csv -> bronze.order_items

Chạy:
    make lake-ingest-order_items
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("promo_id", StringType(), True),
    StructField("promo_id_2", StringType(), True),
])


def partition_columns():
    """order_items không có cột ngày để partition. Bucket theo order_id giữ file cân đối và
    giúp join với orders. Gọi trong hàm vì F.bucket() cần SparkContext đã khởi tạo."""
    return [F.bucket(16, "order_id")]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("promo_id", F.nullif(F.trim(F.col("promo_id")), F.lit("")))
        .withColumn("promo_id_2", F.nullif(F.trim(F.col("promo_id_2")), F.lit("")))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity_invalid")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0),
               F.lit("unit_price_invalid")),
        F.when(F.col("discount_amount") < 0, F.lit("discount_negative")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="order_items",
    source_csv="order_items.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
