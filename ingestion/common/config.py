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


def bronze_table(name: str) -> str:
    """Tên bảng bronze đầy đủ: bronze_table("orders") -> "iceberg.bronze.orders"."""
    return f"{BRONZE_NAMESPACE}.{name}"


def source_path(filename: str) -> str:
    """Đường dẫn CSV nguồn: source_path("orders.csv") -> "/opt/spark/data/orders.csv"."""
    return f"{DATA_DIR}/{filename}"
