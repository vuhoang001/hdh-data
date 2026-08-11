"""
Bảng map KIỂU DỮ LIỆU — nguồn sự thật DUY NHẤT.

Model bronze khai kiểu bằng tên TRUNG LẬP (`integer`, `string`, `date`, `double`) chứ
không phải tên riêng của engine nào, vì cùng một khai báo phải map được sang mọi engine.
Hậu tố `!` đánh dấu cột BẮT BUỘC:

    'order_id': 'integer!'   -> Iceberg `required`, Spark nullable=False

Vì sao module này tồn tại: trước đây bảng map nằm ở HAI nơi
(transforms/macros/bronze_helpers.sql và common/sql_model.py), kèm comment cảnh báo
"thêm kiểu mới phải sửa CẢ HAI". Sau khi thêm engine DuckDB và loader, nó suýt thành BỐN
nơi. Một comment cảnh báo không phải là cơ chế — gom về một file mới là.

Thêm một kiểu mới = thêm một dòng vào MỖI dict dưới đây, trong CÙNG file này.
"""
from typing import Dict

REQUIRED_SUFFIX = "!"

#: Tên kiểu trung lập hợp lệ, dùng trong `{% set columns = {...} %}` của model bronze.
NEUTRAL_TYPES = ("integer", "string", "date", "double")

#: Trung lập -> kiểu SQL của DuckDB (dùng khi đọc nguồn và khi cast landing).
DUCKDB_TYPES: Dict[str, str] = {
    "integer": "INTEGER",
    "string": "VARCHAR",
    "date": "DATE",
    "double": "DOUBLE",
}

#: Trung lập -> tên class trong pyspark.sql.types (tra bằng getattr lúc chạy, để module
#: này nạp được ở nơi không cài pyspark).
SPARK_TYPE_NAMES: Dict[str, str] = {
    "integer": "IntegerType",
    "string": "StringType",
    "date": "DateType",
    "double": "DoubleType",
}


def base_type(dtype: str) -> str:
    """Bỏ hậu tố '!' để lấy tên kiểu.

    Không dùng str.removesuffix: image Spark (apache/spark:3.5.6-python3) chạy
    Python 3.8, mà removesuffix cần 3.9+.
    """
    return dtype[: -len(REQUIRED_SUFFIX)] if dtype.endswith(REQUIRED_SUFFIX) else dtype


def is_required(dtype: str) -> bool:
    return dtype.endswith(REQUIRED_SUFFIX)


def check_supported(table: str, columns: Dict[str, str], mapping: Dict[str, str]) -> None:
    """Dừng NGAY nếu có kiểu chưa hỗ trợ, thay vì để sai kiểu trôi xuống gold."""
    unknown = {n: t for n, t in columns.items() if base_type(t) not in mapping}
    if unknown:
        raise ValueError(
            f"{table}: kiểu không hỗ trợ {unknown}. "
            f"Hợp lệ: {sorted(mapping)} (thêm '{REQUIRED_SUFFIX}' vào cuối để đánh dấu "
            f"cột bắt buộc). Thêm kiểu mới: sửa ingestion/common/types.py."
        )
