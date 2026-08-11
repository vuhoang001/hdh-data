"""
Execution engine của tầng ingestion.

Đây là chỗ DUY NHẤT trong repo mà môi trường quyết định chương trình nào chạy — và nó
chỉ chọn RUNTIME, không chọn logic:

    dev   INGESTION_ENGINE=duckdb  ->  DuckDBIngestionEngine
    prod  INGESTION_ENGINE=spark   ->  SparkIngestionEngine

Cả hai chạy CHÍNH transforms/models/bronze/bronze_<bảng>.sql — cùng file mà dbt đọc.
Không engine nào chứa business logic của bất kỳ bảng nào.

Import ĐỘNG trong create(): pyspark chỉ được nạp khi thật sự dùng Spark, nhờ vậy môi
trường dev không phải cài Spark 300MB, và ngược lại.
"""
from engines.base import IngestionEngine, UnknownEngineError

__all__ = ["IngestionEngine", "UnknownEngineError", "create"]

ENGINES = ("duckdb", "spark")


def create(name: str) -> IngestionEngine:
    """Dựng engine theo tên. Tên lạ thì báo lỗi ngay, không đoán."""
    if name == "duckdb":
        from engines.duckdb_engine import DuckDBIngestionEngine

        return DuckDBIngestionEngine()
    if name == "spark":
        from engines.spark_engine import SparkIngestionEngine

        return SparkIngestionEngine()
    raise UnknownEngineError(
        f"Không biết engine ingestion '{name}'. Hợp lệ: {list(ENGINES)}. "
        f"Đặt bằng INGESTION_ENGINE trong config/.env.<môi trường>."
    )
