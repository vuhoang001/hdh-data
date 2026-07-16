"""
Pipeline ETL (bước Ingest bằng Spark):
    CSV thô  ->  bảng Iceberg  iceberg.raw.orders

Chạy:
    docker compose exec spark \
        /opt/spark/bin/spark-submit /opt/spark/jobs/ingest_orders.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType,
)

RAW_NAMESPACE = "iceberg.raw"
RAW_TABLE = "iceberg.raw.orders"
SOURCE_CSV = "/opt/spark/data/orders.csv"


def main():
    spark = (
        SparkSession.builder
        .appName("hdh-ingest-orders")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("order_ts", TimestampType(), True),
    ])

    print(">> Doc CSV nguon:", SOURCE_CSV)
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(SOURCE_CSV)
        .withColumn("amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    print(">> Tao namespace neu chua co:", RAW_NAMESPACE)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {RAW_NAMESPACE}")

    print(">> Ghi (overwrite) vao bang Iceberg:", RAW_TABLE)
    (
        df.writeTo(RAW_TABLE)
        .using("iceberg")
        .partitionedBy("order_date")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    total = spark.table(RAW_TABLE).count()
    print(f">> Xong. Bang {RAW_TABLE} co {total} dong.")
    spark.table(RAW_TABLE).orderBy("order_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
