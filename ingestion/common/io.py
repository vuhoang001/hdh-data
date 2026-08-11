"""
Đọc dữ liệu NGUỒN — seam giữa local và production.

ĐÂY LÀ CHỖ DUY NHẤT PHẢI ĐỔI KHI NGUỒN THAY ĐỔI. Hiện nguồn là file CSV trong data/;
ở production thường là bảng Postgres, một API, hay Parquet trên S3.

Kiến trúc sau refactor làm seam này SẠCH hơn bản cũ:

    nguồn ──(module này)──► LANDING (Parquet trên MinIO) ──► bronze Iceberg
            ^^^^^^^^^^^^                                     ^^^^^^^^^^^^^
            chỗ DUY NHẤT                                     luôn đọc Parquet,
            biết nguồn là gì                                 không cần biết nguồn
                                                             gốc là gì

Vì mọi thứ đều hạ cánh xuống Parquet trước, engine ingestion (DuckDB hay Spark) chỉ cần
biết đọc Parquet. Thêm một loại nguồn = viết một hàm rồi đăng ký vào SOURCE_READERS, và
khai `type: <loại>` cho bảng đó trong ingestion/config/sources.yml. Model SQL, silver,
gold, test không đụng một dòng.

Reader trả về một ĐOẠN SQL của DuckDB chứ không phải DataFrame: loader là job đổi định
dạng chạy trên một máy, và SQL là cách gọn nhất diễn đạt nó. Đừng nhầm việc này với việc
chọn execution engine cho pipeline — đó là chuyện của ingestion/engines/.
"""
from typing import Dict

from common.types import DUCKDB_TYPES, base_type, check_supported


def read_csv(source: str, columns: Dict[str, str]) -> str:
    """Đọc CSV với schema TƯỜNG MINH, trả về đoạn SQL dùng được sau FROM.

    KHÔNG dùng auto-detect: nó đoán sai lặng lẽ (cột zip toàn số thành integer, `01234`
    mất số 0 đầu), đắt (quét file hai lượt), và che mất việc nguồn đổi cột.

    Schema áp theo VỊ TRÍ, bỏ qua tên ở header (header=true để nhảy dòng đầu), nên đổi
    được tên cột. Mặt trái: nguồn đổi THỨ TỰ cột thì dữ liệu vào nhầm cột mà không có
    lỗi nào báo — đó là cái giá của CSV, và là một lý do nữa để landing dùng Parquet.
    """
    spec = ",\n            ".join(
        f"'{name}': '{DUCKDB_TYPES[base_type(dtype)]}'"
        for name, dtype in columns.items()
    )
    return f"""read_csv(
        '{source}',
        header = true,
        columns = {{
            {spec}
        }}
    )"""


# Đăng ký các loại nguồn đọc được. Test kiểm sources.yml không khai loại nào ngoài đây.
#
# Khi production đọc từ Postgres, thêm vào đây:
#
#     def read_postgres(source, columns):
#         return f"postgres_scan('{source}', 'public', 'orders')"
#
#     SOURCE_READERS = {"csv": read_csv, "postgres": read_postgres}
#
# rồi đổi `type: csv` thành `type: postgres` trong sources.yml cho bảng tương ứng.
# Không có file nào khác phải sửa.
SOURCE_READERS = {
    "csv": read_csv,
}


def source_sql(source_type: str, source: str, columns: Dict[str, str]) -> str:
    """Sinh SQL đọc nguồn theo loại đã khai. Loại lạ thì báo lỗi ngay, không đoán."""
    reader = SOURCE_READERS.get(source_type)
    if reader is None:
        raise ValueError(
            f"Không đọc được nguồn loại '{source_type}'. "
            f"Các loại đã đăng ký: {sorted(SOURCE_READERS)}. "
            f"Thêm reader mới vào ingestion/common/io.py."
        )
    check_supported(source_type, columns, DUCKDB_TYPES)
    return reader(source, columns)
