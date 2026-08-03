"""
Đọc model bronze DÙNG CHUNG (transforms/models/bronze/bronze_<bảng>.sql) cho phía Spark.

Cùng MỘT file SQL được hai engine chạy:

    dbt-duckdb : dbt render Jinja  -> bronze_source() nở thành read_csv(...)
    Spark      : module này render -> bronze_source() thay bằng tên temp view

Nhờ vậy logic bronze (schema, chuẩn hoá, luật chất lượng, cột audit) chỉ tồn tại một bản.
Trước đây nó có hai bản — 13 file .sql và 13 file ingest_*.py — và phải có một test canh
cho chúng khỏi lệch nhau.

ĐÂY KHÔNG PHẢI MỘT JINJA ENGINE. Nó chỉ hiểu đúng ba cấu trúc mà file bronze được phép
chứa; gặp bất kỳ cấu trúc Jinja nào khác thì BÁO LỖI thay vì đoán — vì render sai một
đoạn SQL sẽ tạo ra bảng bronze sai mà không có gì báo:

    {% set source_file = '<tên file>' %}
    {% set columns = { '<cột>': '<kiểu>', ... } %}
    {{ bronze_source(source_file, columns) }}      -> tên temp view
    '{{ source_file }}'                            -> tên file, cho cột _source_file
"""
import ast
import re
from dataclasses import dataclass
from typing import Dict, List

# Kiểu TRUNG LẬP hợp lệ. Bản đối xứng cho DuckDB nằm ở transforms/macros/bronze_helpers.sql
# — thêm kiểu mới phải sửa CẢ HAI.
#
# pyspark CỐ Ý không import ở cấp module: module này phải nạp được ở nơi không có Spark
# (CI lint, pytest) để test kiểm được khuôn model mà không phải cài cả bộ Spark 300MB.
# Chỉ property .schema mới cần pyspark, và nó import lúc gọi.
NEUTRAL_TYPES = ("integer", "string", "date", "double")

# Hậu tố '!' đánh dấu cột bắt buộc -> StructField(nullable=False) -> Iceberg `required`.
REQUIRED_SUFFIX = "!"


def base_type(dtype: str) -> str:
    """Bỏ hậu tố '!' để lấy tên kiểu. Không dùng str.removesuffix vì image Spark
    (apache/spark:3.5.6-python3) chạy Python 3.8, mà removesuffix cần 3.9+."""
    return dtype[: -len(REQUIRED_SUFFIX)] if dtype.endswith(REQUIRED_SUFFIX) else dtype

_SET_SOURCE_FILE = re.compile(r"\{%-?\s*set\s+source_file\s*=\s*'([^']+)'\s*-?%\}")
_SET_COLUMNS = re.compile(r"\{%-?\s*set\s+columns\s*=\s*(\{[^{}]*\})\s*-?%\}", re.S)
_BRONZE_SOURCE = re.compile(
    r"\{\{-?\s*bronze_source\(\s*source_file\s*,\s*columns\s*\)\s*-?\}\}"
)
_SOURCE_FILE_REF = re.compile(r"\{\{-?\s*source_file\s*-?\}\}")

# Bất kỳ thẻ Jinja nào còn sót sau khi render = file dùng cấu trúc module này không hiểu.
_ANY_JINJA = re.compile(r"\{\{|\{%")


class BronzeModelError(ValueError):
    """File model bronze không đúng khuôn mà cả hai engine cùng hiểu được."""


@dataclass(frozen=True)
class BronzeModel:
    """Một model bronze đã đọc xong, chưa gắn tên temp view."""

    table: str
    source_file: str
    columns: Dict[str, str]
    _template: str

    @property
    def schema(self):
        """Schema Spark tường minh để đọc nguồn. Spark áp theo THỨ TỰ cột, bỏ qua tên header.

        Import pyspark ở trong hàm, không ở đầu module — xem ghi chú tại NEUTRAL_TYPES.
        """
        from pyspark.sql.types import (
            DateType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        spark_types = {
            "integer": IntegerType,
            "string": StringType,
            "date": DateType,
            "double": DoubleType,
        }
        return StructType([
            StructField(
                name,
                spark_types[base_type(dtype)](),
                not dtype.endswith(REQUIRED_SUFFIX),
            )
            for name, dtype in self.columns.items()
        ])

    def render(self, source_view: str) -> str:
        """Sinh SQL chạy được, với quan hệ nguồn trỏ vào `source_view`."""
        sql = _BRONZE_SOURCE.sub(source_view, self._template)
        sql = _SOURCE_FILE_REF.sub(self.source_file, sql)

        leftover = _ANY_JINJA.search(sql)
        if leftover:
            line = sql[: leftover.start()].count("\n") + 1
            raise BronzeModelError(
                f"{self.table}: còn thẻ Jinja chưa render ở dòng {line}. "
                f"Model bronze chỉ được dùng bronze_source(source_file, columns) và "
                f"{{{{ source_file }}}} — mọi macro khác Spark không hiểu. "
                f"Đoạn lỗi: {sql[leftover.start():leftover.start() + 60]!r}"
            )
        return sql


def _parse_columns(raw: str, table: str) -> Dict[str, str]:
    try:
        columns = ast.literal_eval(raw)
    except (ValueError, SyntaxError) as exc:
        raise BronzeModelError(f"{table}: không đọc được khối `set columns`: {exc}") from exc

    if not isinstance(columns, dict) or not columns:
        raise BronzeModelError(f"{table}: `columns` phải là dict không rỗng.")

    unknown = {
        name: dtype
        for name, dtype in columns.items()
        if base_type(dtype) not in NEUTRAL_TYPES
    }
    if unknown:
        raise BronzeModelError(
            f"{table}: kiểu không hỗ trợ {unknown}. Kiểu hợp lệ: {sorted(NEUTRAL_TYPES)} "
            f"(thêm '{REQUIRED_SUFFIX}' vào cuối để đánh dấu cột bắt buộc). "
            f"Thêm kiểu mới phải sửa CẢ đây VÀ transforms/macros/bronze_helpers.sql."
        )
    return columns


def load(table: str, models_dir: str) -> BronzeModel:
    """Đọc transforms/models/bronze/bronze_<table>.sql thành BronzeModel."""
    path = f"{models_dir}/bronze_{table}.sql"
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except FileNotFoundError as exc:
        raise BronzeModelError(
            f"{table}: không thấy model bronze tại {path}. Bảng khai trong "
            f"ingestion/config/sources.yml phải có file SQL tương ứng."
        ) from exc

    m_file = _SET_SOURCE_FILE.search(text)
    if not m_file:
        raise BronzeModelError(f"{table}: thiếu khối {{% set source_file = '...' %}}.")

    m_cols = _SET_COLUMNS.search(text)
    if not m_cols:
        raise BronzeModelError(f"{table}: thiếu khối {{% set columns = {{...}} %}}.")

    if not _BRONZE_SOURCE.search(text):
        raise BronzeModelError(
            f"{table}: thiếu {{{{ bronze_source(source_file, columns) }}}} — "
            f"không biết cắm quan hệ nguồn vào đâu."
        )

    # Bỏ hai khối `set` khỏi SQL: chúng là khai báo cho Jinja, không phải câu lệnh SQL.
    template = _SET_COLUMNS.sub("", _SET_SOURCE_FILE.sub("", text)).lstrip("\n")

    return BronzeModel(
        table=table,
        source_file=m_file.group(1),
        columns=_parse_columns(m_cols.group(1), table),
        _template=template,
    )


def load_all(tables: List[str], models_dir: str) -> Dict[str, BronzeModel]:
    """Đọc nhiều model một lượt — dùng cho test kiểm cả 13 bảng cùng lúc."""
    return {t: load(t, models_dir) for t in tables}
