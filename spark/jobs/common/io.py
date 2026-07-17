"""Đọc file nguồn. Path có thể là local (/opt/spark/data/x.csv) hoặc S3 (s3a://...)."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_csv(
    spark: SparkSession,
    path: str,
    schema: StructType,
    header: bool = True,
    date_format: str = "yyyy-MM-dd",
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss",
) -> DataFrame:
    """Đọc CSV với schema tường minh. Job gọi hàm này tự khai báo schema của mình."""
    return (
        spark.read
        .option("header", header)
        .option("dateFormat", date_format)
        .option("timestampFormat", timestamp_format)
        .schema(schema)
        .csv(path)
    )
