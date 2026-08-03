"""Ghi/đọc bảng Iceberg trên MinIO qua REST catalog."""
from pyspark.sql import DataFrame, SparkSession

DEFAULT_ICEBERG_FORMAT_VERSION = "2"


def create_namespace(spark: SparkSession, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


# Cột audit (_source_file, _ingested_at) trước đây gắn ở đây bằng add_audit_columns().
# Giờ chúng do chính model SQL sinh ra, cùng chỗ với phần logic còn lại, nên cả hai engine
# tạo ra bộ cột giống hệt nhau mà không cần ai đồng bộ với ai.


def write_iceberg_table(df: DataFrame, table_name: str, partition_columns=None) -> None:
    """Ghi đè (createOrReplace) bảng Iceberg. partition_columns=None -> bảng không partition."""
    writer = df.writeTo(table_name).using("iceberg")
    if partition_columns:
        writer = writer.partitionedBy(*partition_columns)
    writer.tableProperty("format-version", DEFAULT_ICEBERG_FORMAT_VERSION).createOrReplace()


def count_table_rows(spark: SparkSession, table_name: str) -> int:
    return spark.table(table_name).count()
