"""
Cấu hình runtime của tầng ingestion — đọc từ biến môi trường, nguồn là config/.env.*

Đây là NƠI DUY NHẤT trong code ingestion biết tên catalog, tên namespace, endpoint và
đường dẫn. Không nơi nào tự ghép chuỗi "iceberg.bronze.orders" hay "s3://.../landing".

QUAN TRỌNG: module này KHÔNG biết mình đang chạy ở dev hay prod — nó chỉ đọc biến.
Chính vì vậy cùng một đoạn code chạy được ở cả hai môi trường: env quyết định GIÁ TRỊ,
không quyết định NHÁNH CODE.

Giá trị mặc định ở đây chỉ để chạy được khi thiếu env (vd chạy thử ngoài container);
trong container docker compose luôn truyền đủ.
"""
import os


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


# ---- Môi trường & engine -----------------------------------------------------
ENVIRONMENT = _env("ENVIRONMENT", "dev")

# Engine chạy ingestion: "duckdb" (dev) hoặc "spark" (prod, khi cần scale).
# Đây là lựa chọn RUNTIME, không phải logic — cả hai chạy chính bronze_<bảng>.sql.
ENGINE = _env("INGESTION_ENGINE", "duckdb")

# ---- Iceberg catalog ---------------------------------------------------------
CATALOG = _env("ICEBERG_CATALOG_NAME", "iceberg")
BRONZE_SCHEMA = _env("BRONZE_NAMESPACE", "bronze")
BRONZE_NAMESPACE = f"{CATALOG}.{BRONZE_SCHEMA}"
ICEBERG_REST_URI = _env("ICEBERG_REST_URI", "http://iceberg-rest:8181")

# ---- Object storage ----------------------------------------------------------
WAREHOUSE_BUCKET = _env("WAREHOUSE_BUCKET", "warehouse")
LANDING_PREFIX = _env("LANDING_PREFIX", "landing")

# S3_ENDPOINT có scheme (Spark/Iceberg dùng), MINIO_ENDPOINT không có scheme
# (DuckDB `CREATE SECRET` yêu cầu dạng host:port). Hai biến, cùng một máy chủ.
S3_ENDPOINT = _env("S3_ENDPOINT", "http://minio:9000")
MINIO_ENDPOINT = _env("MINIO_ENDPOINT", "minio:9000")
AWS_ACCESS_KEY_ID = _env("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = _env("AWS_SECRET_ACCESS_KEY", "change-me")
AWS_REGION = _env("AWS_REGION", "us-east-1")

# ---- Đường dẫn ---------------------------------------------------------------
# Model bronze DÙNG CHUNG với dbt: ingestion đọc chính các file .sql mà dbt đọc.
MODELS_DIR = _env("INGEST_MODELS_DIR", "/usr/app/dbt/bronze")

# Đăng ký nguồn — cấu hình ingest mà dbt không cần (xem common/spec.py)
SOURCES_FILE = _env("INGEST_SOURCES_FILE", "/opt/ingestion/config/sources.yml")

# Thư mục file nguồn thô. CHỈ load_landing.py dùng — ingest.py không bao giờ đọc
# filesystem, nó luôn đọc landing zone trên object storage.
RAW_DATA_DIR = _env("DBT_DATA_DIR", _env("SPARK_DATA_DIR", "/data"))

# ---- Sampling (chỉ có ý nghĩa ở dev) -----------------------------------------
SAMPLE_ENABLED = _env("SAMPLE_ENABLED", "false").lower() in ("1", "true", "yes")
SAMPLE_MONTHS = int(_env("SAMPLE_MONTHS", "6") or "6")


def bronze_table(name: str) -> str:
    """Tên bảng bronze đầy đủ: bronze_table("orders") -> "iceberg.bronze.orders"."""
    return f"{BRONZE_NAMESPACE}.{name}"


def landing_uri(table: str) -> str:
    """Thư mục landing của một bảng: "s3://warehouse/landing/orders"."""
    return f"s3://{WAREHOUSE_BUCKET}/{LANDING_PREFIX}/{table}"


def landing_glob(table: str) -> str:
    """Pattern đọc mọi file Parquet trong landing của một bảng."""
    return f"{landing_uri(table)}/*.parquet"


def raw_path(filename: str) -> str:
    """Đường dẫn file nguồn thô, chỉ dùng bởi loader: raw_path("orders.csv")."""
    return f"{RAW_DATA_DIR}/{filename}"
