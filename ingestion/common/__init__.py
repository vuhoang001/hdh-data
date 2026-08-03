"""
Hạ tầng dùng chung cho mọi Spark job.

Package này CHỈ chứa phần kỹ thuật: đọc cấu hình từ env, tạo SparkSession, đọc file từ
MinIO/local, ghi bảng Iceberg, logging, và khung chạy chung của bronze job. Không chứa
business logic của bất kỳ bảng nào — schema, rule làm sạch, cột dẫn xuất... thuộc về
từng file job trong bronze/.

    config   : nơi duy nhất biết tên catalog / namespace / thư mục dữ liệu (đọc từ .env)
    job      : BronzeJob + run_job — khung chạy chung, xoá boilerplate khỏi 13 job
    session  : SparkSession + logger
    io       : đọc file nguồn
    iceberg  : cột audit + ghi bảng Iceberg
"""
from common import config
from common.iceberg import (
    add_audit_columns,
    count_table_rows,
    create_namespace,
    write_iceberg_table,
)
from common.io import SOURCE_READERS, read_csv, read_source
from common.job import BronzeJob, run_job
from common.session import build_spark_session, get_logger

__all__ = [
    "BronzeJob",
    "SOURCE_READERS",
    "add_audit_columns",
    "build_spark_session",
    "config",
    "count_table_rows",
    "create_namespace",
    "get_logger",
    "read_csv",
    "read_source",
    "run_job",
    "write_iceberg_table",
]
