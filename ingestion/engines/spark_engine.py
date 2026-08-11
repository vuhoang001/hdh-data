"""
Engine ingestion cho PRODUCTION: Spark ghi vào Iceberg trên object storage.

Đây là code đã chạy từ trước, được CHUYỂN VÀO interface chứ không viết lại — hành vi
giữ nguyên: cùng cách đọc nguồn, cùng `spark.sql()` chạy model bronze dùng chung, cùng
`createOrReplace` khi ghi.

Khác biệt duy nhất so với bản cũ: nguồn không còn là file CSV trên filesystem của
container mà là landing Parquet trên object storage — giống hệt engine DuckDB đọc.
Nhờ vậy hai engine đọc CÙNG dữ liệu, không chỉ chạy cùng SQL.

pyspark CỐ Ý chỉ import bên trong hàm: file này phải nạp được ở nơi không có Spark
(pytest, CI lint) để test kiểm được khai báo mà không phải cài cả bộ Spark 300MB.
"""
from typing import List, Optional, Tuple

from common import config
from common.types import SPARK_TYPE_NAMES, base_type, check_supported, is_required
from engines.base import IngestionEngine, IngestionError

DEFAULT_LOG_LEVEL = "WARN"
DEFAULT_ICEBERG_FORMAT_VERSION = "2"

# Hàm partition Iceberg nhận đúng một cột
_TIME_TRANSFORMS = ("years", "months", "days", "hours")


class SparkIngestionEngine(IngestionEngine):
    name = "spark"

    def __init__(self) -> None:
        self._spark = None

    # ---- Vòng đời -----------------------------------------------------------
    def start(self) -> None:
        from pyspark.sql import SparkSession

        # Cấu hình Iceberg/S3 đến từ spark-defaults.conf, do entrypoint render từ env.
        self._spark = SparkSession.builder.appName("hdh-ingestion").getOrCreate()
        self._spark.sparkContext.setLogLevel(DEFAULT_LOG_LEVEL)

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    @property
    def spark(self):
        if self._spark is None:
            raise IngestionError("Engine chưa start(). Dùng `with engines.create(...)`.")
        return self._spark

    # ---- Thao tác -----------------------------------------------------------
    def create_namespace(self, namespace: str) -> None:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    def register_landing_source(self, view_name: str, table: str, columns: dict) -> None:
        """Đọc landing Parquet với schema TƯỜNG MINH khai trong model SQL.

        Không dùng inferSchema kể cả với Parquet: schema khai trong model là hợp đồng,
        và áp nó tường minh là cách đảm bảo Spark với DuckDB cho ra bronze giống nhau.
        """
        schema = _spark_schema(table, columns)
        (
            self.spark.read.schema(schema)
            .parquet(_s3a(config.landing_uri(table)))
            .createOrReplaceTempView(view_name)
        )

    def write_table_from_sql(
        self, sql: str, target_table: str, partition: Optional[Tuple[str, List[str]]]
    ) -> None:
        """Chạy SQL dùng chung rồi ghi đè (createOrReplace) bảng Iceberg."""
        df = self.spark.sql(sql)
        writer = df.writeTo(target_table).using("iceberg")

        columns = _partition_columns(target_table, partition)
        if columns:
            writer = writer.partitionedBy(*columns)
        writer.tableProperty("format-version", DEFAULT_ICEBERG_FORMAT_VERSION).createOrReplace()

    def count(self, table: str) -> int:
        return self.spark.table(table).count()

    def count_where(self, table: str, condition: str) -> int:
        return self.spark.table(table).filter(condition).count()


def _s3a(uri: str) -> str:
    """Đổi s3:// -> s3a:// cho Hadoop FileSystem của Spark.

    config.landing_uri() trả về `s3://` vì đó là dạng chuẩn mà Iceberg và DuckDB dùng.
    Spark đọc file thô qua Hadoop FileSystem, và ở đó scheme phải là `s3a` — `s3` không
    có implementation nào đăng ký. Đây là đặc thù của Spark nên quy đổi nằm ở engine
    này, không đẩy ngược lên config.
    """
    return "s3a://" + uri[len("s3://"):] if uri.startswith("s3://") else uri


def _spark_schema(table: str, columns: dict):
    """Dựng StructType từ khai báo kiểu trung lập. Hậu tố '!' -> nullable=False."""
    from pyspark.sql import types as T

    try:
        check_supported(table, columns, SPARK_TYPE_NAMES)
    except ValueError as exc:
        raise IngestionError(str(exc)) from exc
    return T.StructType(
        [
            T.StructField(
                name,
                getattr(T, SPARK_TYPE_NAMES[base_type(dtype)])(),
                not is_required(dtype),
            )
            for name, dtype in columns.items()
        ]
    )


def _partition_columns(table: str, partition: Optional[Tuple[str, List[str]]]):
    """Dịch ("months", ["order_date"]) -> [F.months("order_date")].

    Gọi lúc CHẠY chứ không lúc import: F.months()/F.bucket() cần SparkContext đã khởi
    tạo mới dựng được biểu thức.
    """
    if partition is None:
        return None

    from pyspark.sql import functions as F

    fn, args = partition
    if fn in _TIME_TRANSFORMS:
        return [getattr(F, fn)(args[0])]
    if fn == "bucket":
        return [F.bucket(int(args[0]), args[1])]
    raise IngestionError(f"{table}: Spark không hỗ trợ hàm partition '{fn}'.")
