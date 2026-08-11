"""
Engine ingestion cho DEV: DuckDB ghi thẳng vào Iceberg trên MinIO.

Đây là mảnh trước đây KHÔNG tồn tại. Môi trường dev cũ không có đường nào tạo bronze
trên Iceberg — bronze chỉ là dbt model đọc CSV local. Nhờ module này, dev dùng ĐÚNG
storage và ĐÚNG table format như prod, chỉ khác engine.

Ba ràng buộc của DuckDB-Iceberg mà code dưới đây phải đi vòng (đã kiểm chứng bằng
thực nghiệm, không phải suy đoán từ tài liệu):

  1. Catalog mặc định dùng oauth2 -> phải ATTACH kèm AUTHORIZATION_TYPE 'none'
     (iceberg-rest trong stack này chạy không auth).
  2. `CREATE OR REPLACE TABLE` KHÔNG được hỗ trợ -> phải DROP rồi CREATE.
  3. Không rename được bảng đã bị sửa trong cùng transaction -> mọi lệnh chạy ở chế
     độ autocommit, không gom vào transaction.
"""
from typing import List, Optional, Tuple

from common import config
from common.types import DUCKDB_TYPES, base_type, check_supported
from engines.base import IngestionEngine, IngestionError

# Hàm partition của Iceberg: sources.yml dùng dạng SỐ NHIỀU (chuẩn Iceberg/Spark),
# DuckDB nhận dạng SỐ ÍT. Dịch ở đây thay vì bắt sources.yml phải biết engine nào
# đang đọc nó.
DUCKDB_PARTITION_FN = {
    "years": "year",
    "months": "month",
    "days": "day",
    "hours": "hour",
    "bucket": "bucket",
}


class DuckDBIngestionEngine(IngestionEngine):
    name = "duckdb"

    def __init__(self) -> None:
        self._con = None

    # ---- Vòng đời -----------------------------------------------------------
    def start(self) -> None:
        import duckdb

        # In-memory là ĐÚNG ở đây: mọi thứ engine này tạo ra đều nằm trên Iceberg/MinIO.
        # Không có state nào đáng giữ lại trong file DuckDB giữa các lần ingest.
        self._con = duckdb.connect(":memory:")
        self._sql("INSTALL httpfs; LOAD httpfs;")
        self._sql("INSTALL iceberg; LOAD iceberg;")

        # Secret S3 trỏ vào MinIO. url_style=path và use_ssl=false là bắt buộc với
        # MinIO local; endpoint KHÔNG có scheme (khác S3_ENDPOINT của Spark/Trino).
        self._sql(
            f"""
            CREATE OR REPLACE SECRET minio_s3 (
                TYPE S3,
                KEY_ID '{config.AWS_ACCESS_KEY_ID}',
                SECRET '{config.AWS_SECRET_ACCESS_KEY}',
                ENDPOINT '{config.MINIO_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL false,
                REGION '{config.AWS_REGION}'
            )"""
        )

        # ATTACH catalog. AUTHORIZATION_TYPE 'none' là BẮT BUỘC: thiếu nó DuckDB mặc
        # định oauth2 và báo "no 'secret' was provided, and no client_id+client_secret".
        self._sql(
            f"""
            ATTACH IF NOT EXISTS '{config.WAREHOUSE_BUCKET}' AS {config.CATALOG} (
                TYPE ICEBERG,
                ENDPOINT '{config.ICEBERG_REST_URI}',
                AUTHORIZATION_TYPE 'none'
            )"""
        )

    def stop(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None

    # ---- Nội bộ -------------------------------------------------------------
    def _sql(self, sql: str):
        if self._con is None:
            raise IngestionError("Engine chưa start(). Dùng `with engines.create(...)`.")
        return self._con.execute(sql)

    def _scalar(self, sql: str):
        row = self._sql(sql).fetchone()
        return row[0] if row else None

    # ---- Thao tác -----------------------------------------------------------
    def create_namespace(self, namespace: str) -> None:
        self._sql(f"CREATE SCHEMA IF NOT EXISTS {namespace}")

    def register_landing_source(self, view_name: str, table: str, columns: dict) -> None:
        """Đọc landing Parquet và ÁP schema khai trong model SQL.

        Cast tường minh chứ không tin kiểu sẵn có của file: đó là cách duy nhất đảm bảo
        DuckDB và Spark tạo ra bronze cùng kiểu dữ liệu. Cột khai trong model mà thiếu
        trong file sẽ lỗi ngay ở đây — đúng lúc, thay vì trôi xuống silver.
        """
        try:
            check_supported(table, columns, DUCKDB_TYPES)
        except ValueError as exc:
            raise IngestionError(str(exc)) from exc
        projection = ",\n                ".join(
            f'cast("{name}" as {DUCKDB_TYPES[base_type(dtype)]}) as "{name}"'
            for name, dtype in columns.items()
        )
        self._sql(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT
                {projection}
            FROM read_parquet('{config.landing_glob(table)}')"""
        )

    def write_table_from_sql(
        self, sql: str, target_table: str, partition: Optional[Tuple[str, List[str]]]
    ) -> None:
        """Ghi đè bảng Iceberg bằng kết quả của `sql`.

        DROP + CREATE chứ không CREATE OR REPLACE: DuckDB-Iceberg báo thẳng
        "CREATE OR REPLACE not supported... Please use separate Drop and Create".
        """
        self._sql(f"DROP TABLE IF EXISTS {target_table}")

        if partition is None:
            # Không partition -> CTAS một phát, để engine tự suy schema.
            self._sql(f"CREATE TABLE {target_table} AS {sql}")
            return

        # Có partition -> phải khai schema TRƯỚC trong CREATE TABLE, vì mệnh đề
        # PARTITIONED BY không đi kèm được với CTAS. Lấy schema bằng DESCRIBE để
        # không phải tự đoán kiểu của các cột audit do chính SQL sinh ra
        # (_is_valid, _invalid_reason, _source_file, _ingested_at).
        described = self._sql(f"DESCRIBE {sql}").fetchall()
        if not described:
            raise IngestionError(f"{target_table}: DESCRIBE không trả về cột nào.")
        column_ddl = ",\n                ".join(f'"{row[0]}" {row[1]}' for row in described)

        self._sql(
            f"""
            CREATE TABLE {target_table} (
                {column_ddl}
            ) PARTITIONED BY ({_partition_expr(target_table, partition)})"""
        )
        self._sql(f"INSERT INTO {target_table} {sql}")

    def count(self, table: str) -> int:
        return int(self._scalar(f"SELECT count(*) FROM {table}") or 0)

    def count_where(self, table: str, condition: str) -> int:
        return int(self._scalar(f"SELECT count(*) FROM {table} WHERE {condition}") or 0)



def _partition_expr(table: str, partition: Tuple[str, List[str]]) -> str:
    """Dịch ("months", ["order_date"]) -> "month(order_date)" cho DuckDB."""
    fn, args = partition
    duck_fn = DUCKDB_PARTITION_FN.get(fn)
    if duck_fn is None:
        raise IngestionError(
            f"{table}: DuckDB không hỗ trợ hàm partition '{fn}'. "
            f"Hợp lệ: {sorted(DUCKDB_PARTITION_FN)}."
        )
    if fn == "bucket":
        return f"bucket({args[0]}, {args[1]})"
    return f"{duck_fn}({args[0]})"
