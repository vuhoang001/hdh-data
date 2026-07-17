"""
Hạ tầng dùng chung cho mọi Spark job.

Package này CHỈ chứa phần kỹ thuật: tạo SparkSession, đọc file từ MinIO/local,
ghi bảng Iceberg, logging. Không chứa business logic của bất kỳ bảng nào —
schema, rule làm sạch, cột dẫn xuất... thuộc về từng file job trong bronze/.
"""
from common.iceberg import (
    add_audit_columns,
    count_table_rows,
    create_namespace,
    write_iceberg_table,
)
from common.io import read_csv
from common.session import build_spark_session, get_logger

__all__ = [
    "add_audit_columns",
    "build_spark_session",
    "count_table_rows",
    "create_namespace",
    "get_logger",
    "read_csv",
    "write_iceberg_table",
]
