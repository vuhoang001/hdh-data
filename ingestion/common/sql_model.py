"""
Đọc model bronze DÙNG CHUNG (ingestion/bronze_specs/bronze_<bảng>.sql) cho phía Spark.

Cùng MỘT file SQL được hai engine chạy:

    DuckDB (dev)  : bronze_source() thay bằng view đọc landing Parquet
    Spark  (prod) : bronze_source() thay bằng temp view đọc landing Parquet

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

# Kiểu dữ liệu: bảng map sống ở common/types.py — MỘT nguồn sự thật cho mọi engine.
from common.types import NEUTRAL_TYPES, REQUIRED_SUFFIX, base_type  # noqa: F401

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
    """Một model bronze đã đọc xong, chưa gắn tên quan hệ nguồn.

    CỐ Ý không có property .schema: mỗi engine tự dựng schema theo kiểu của mình từ
    `columns` (xem engines/*_engine.py). Nếu để ở đây, module này sẽ phải import
    pyspark và mất khả năng nạp trong môi trường không có Spark.
    """

    table: str
    source_file: str
    columns: Dict[str, str]
    _template: str

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
            f"Thêm kiểu mới: sửa ingestion/common/types.py."
        )
    return columns


def load(table: str, models_dir: str) -> BronzeModel:
    """Đọc ingestion/bronze_specs/bronze_<table>.sql thành BronzeModel."""
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
