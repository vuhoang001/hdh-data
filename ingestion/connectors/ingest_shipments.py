"""
Bronze: data/shipments.csv -> bronze.shipments

Chạy:
    make lake-ingest-shipments
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("ship_date", DateType(), True),
    StructField("delivery_date", DateType(), True),
    StructField("shipping_fee", DoubleType(), True),
])


def partition_columns():
    """Partition theo tháng như orders: 566k dòng trải 2012-2023, theo ngày sẽ ra ~3800
    partition với file vài chục KB và làm writer OOM. Gọi trong hàm vì F.months() cần
    SparkContext đã khởi tạo."""
    return [F.months("ship_date")]


def transform(df: DataFrame) -> DataFrame:
    """Gắn cờ chất lượng. Bảng không có cột text nào cần chuẩn hoá.
    Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("ship_date").isNull(), F.lit("ship_date_missing")),
        # Giao trước khi gửi là bất khả thi về mặt vật lý -> chắc chắn lỗi dữ liệu
        F.when(F.col("delivery_date") < F.col("ship_date"), F.lit("delivery_before_ship")),
        F.when(F.col("shipping_fee").isNull() | (F.col("shipping_fee") < 0),
               F.lit("shipping_fee_invalid")),
    )

    return (
        df
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


JOB = BronzeJob(
    table="shipments",
    source_csv="shipments.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,
)

if __name__ == "__main__":
    run_job(JOB)
