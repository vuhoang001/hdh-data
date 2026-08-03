"""Ghi/đọc bảng Iceberg trên MinIO qua REST catalog."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_ICEBERG_FORMAT_VERSION = "2"


def create_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def add_audit_columns(df: DataFrame, source_file: str) -> DataFrame:
    """Metadata kỹ thuật gắn cho mọi bảng bronze, không phụ thuộc nội dung bảng."""
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_iceberg_table(df: DataFrame, table_name: str, partition_columns=None) -> None:
    """Ghi đè (createOrReplace) bảng Iceberg. partition_columns=None -> bảng không partition."""
    writer = df.writeTo(table_name).using("iceberg")
    if partition_columns:
        writer = writer.partitionedBy(*partition_columns)
    writer.tableProperty("format-version", DEFAULT_ICEBERG_FORMAT_VERSION).createOrReplace()


def count_table_rows(spark: SparkSession, table_name: str) -> int:
    return spark.table(table_name).count()
