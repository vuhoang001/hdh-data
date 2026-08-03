"""
Bronze: data/orders.csv -> bronze.orders

File này chỉ chứa business logic của bảng orders (schema, chuẩn hoá, rule chất lượng).
Phần hạ tầng — SparkSession, đọc CSV, cột audit, ghi Iceberg, log — nằm ở common/job.py.
Tên catalog/namespace/thư mục dữ liệu lấy từ .env qua common/config.py.

Chạy:
    make lake-ingest-orders
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("zip", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("order_source", StringType(), True),
])

VALID_STATUS_VALUES = ["created", "paid", "shipped", "delivered", "returned", "cancelled"]


def partition_columns():
    """Partition theo tháng, không theo ngày: dữ liệu trải 2012-2023 nên partition theo ngày
    sẽ tạo ~3800 partition cho ~650k dòng (mỗi file vài chục KB) và làm writer OOM.
    Gọi trong hàm vì F.months() cần SparkContext đã khởi tạo."""
    return [F.months("order_date")]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("zip", F.trim(F.col("zip")))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
        .withColumn("device_type", F.lower(F.trim(F.col("device_type"))))
        .withColumn("order_source", F.lower(F.trim(F.col("order_source"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("customer_id").isNull(), F.lit("customer_id_missing")),
        F.when(F.col("order_date").isNull(), F.lit("order_date_missing")),
        F.when(~F.col("order_status").isin(*VALID_STATUS_VALUES), F.lit("status_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="orders",
    source_csv="orders.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
