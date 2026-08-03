"""
Cấu hình runtime của mọi Spark job — đọc từ biến môi trường, nguồn gốc là .env ở gốc repo.

Đây là NƠI DUY NHẤT trong code Spark biết tên catalog, tên namespace bronze và thư mục
dữ liệu. Job không tự ghép chuỗi "iceberg.bronze.orders" hay "/opt/spark/data/orders.csv"
nữa — đổi tên catalog hay đổi mount point chỉ cần sửa .env.

Giá trị mặc định ở đây chỉ để chạy được khi thiếu env (vd chạy thử ngoài container);
trong container docker compose luôn truyền đủ.
"""
import os

# Tên catalog Iceberg, phải khớp spark-defaults.conf và catalog của Trino
CATALOG = os.environ.get("ICEBERG_CATALOG_NAME", "iceberg")

# Namespace chứa các bảng bronze bên trong catalog
BRONZE_SCHEMA = os.environ.get("BRONZE_NAMESPACE", "bronze")

# Namespace đầy đủ, vd "iceberg.bronze"
BRONZE_NAMESPACE = f"{CATALOG}.{BRONZE_SCHEMA}"

# Thư mục CSV nguồn nhìn từ bên trong container spark
DATA_DIR = os.environ.get("SPARK_DATA_DIR", "/opt/spark/data")

# Thư mục chứa model bronze DÙNG CHUNG với dbt (transforms/models/bronze).
# Spark đọc chính các file .sql mà dbt build — xem common/sql_model.py.
# compose mount ./transforms vào đây; giá trị mặc định chỉ dùng khi chạy ngoài container.
MODELS_DIR = os.environ.get("SPARK_BRONZE_MODELS_DIR", "/opt/spark/transforms/models/bronze")

# Đăng ký nguồn — cấu hình ingest mà dbt không cần (xem common/spec.py)
SOURCES_FILE = os.environ.get("SPARK_SOURCES_FILE", "/opt/spark/jobs/config/sources.yml")


def bronze_table(name: str) -> str:
    """Tên bảng bronze đầy đủ: bronze_table("orders") -> "iceberg.bronze.orders"."""
    return f"{BRONZE_NAMESPACE}.{name}"


def source_path(filename: str) -> str:
    """Đường dẫn CSV nguồn: source_path("orders.csv") -> "/opt/spark/data/orders.csv"."""
    return f"{DATA_DIR}/{filename}"
