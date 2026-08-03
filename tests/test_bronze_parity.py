"""
Bronze được cài ĐẶT HAI LẦN — và test này giữ cho hai bản không lệch nhau.

Cùng 13 bảng, cùng rule chất lượng, nhưng hai engine:
    ingestion/connectors/ingest_<bảng>.py       PySpark  -> môi trường lakehouse
    transforms/models/bronze/bronze_<bảng>.sql  SQL      -> môi trường DuckDB

Trùng lặp này là lựa chọn có ý thức: đổi lại được dev loop và CI chạy trong vài giây thay
vì phải dựng MinIO+Spark+Trino. Cái giá là mỗi lần đổi rule phải sửa hai chỗ.

**Đây chính là chỗ nguy hiểm mà test này bịt.** Không có nó, sửa rule `orders` ở một bên
mà quên bên kia sẽ khiến hai môi trường cho hai kết quả khác nhau — im lặng, không lỗi nào
báo, và chỉ lộ ra khi ai đó đối chiếu số.

Test so sánh **tập nhãn `_invalid_reason`** của từng cặp file. Nhãn là thứ biểu diễn trực
tiếp rule chất lượng, nên hai bên khai khác tập nhãn nghĩa là chúng đã lệch.

Test KHÔNG kiểm được logic bên trong mỗi nhãn (ví dụ một bên `< 0` còn bên kia `<= 0`).
Đó là giới hạn thật của cách tiếp cận tĩnh này — muốn bắt tới mức đó thì phải chạy cả hai
engine trên cùng bộ dữ liệu rồi so kết quả, tức là quay lại chi phí mà trùng lặp này né.

    pytest tests -q
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CONNECTORS_DIR = REPO_ROOT / "ingestion" / "connectors"
BRONZE_MODELS_DIR = REPO_ROOT / "transforms" / "models" / "bronze"

# PySpark: F.when(<điều kiện>, F.lit("nhãn"))  — F.lit("") của nullif không khớp \w+
PY_LABEL = re.compile(r'F\.lit\("(\w+)"\)')

# SQL: {{ invalid_reason([ ["<điều kiện>", "nhãn"], ... ]) }}
SQL_LABEL = re.compile(r',\s*"(\w+)"\s*\]')


def table_names():
    return sorted(p.stem[len("ingest_"):] for p in CONNECTORS_DIR.glob("ingest_*.py"))


def spark_labels(table):
    return set(PY_LABEL.findall((CONNECTORS_DIR / f"ingest_{table}.py").read_text(encoding="utf-8")))


def duckdb_labels(table):
    return set(SQL_LABEL.findall((BRONZE_MODELS_DIR / f"bronze_{table}.sql").read_text(encoding="utf-8")))


def test_moi_bang_co_ca_hai_ban_cai_dat():
    """Bảng có connector Spark thì phải có model bronze DuckDB tương ứng, và ngược lại."""
    spark = set(table_names())
    duckdb = {p.stem[len("bronze_"):] for p in BRONZE_MODELS_DIR.glob("bronze_*.sql")}
    assert spark == duckdb, (
        f"Chỉ có bản Spark: {sorted(spark - duckdb)} | "
        f"Chỉ có bản DuckDB: {sorted(duckdb - spark)}"
    )


def test_rule_chat_luong_khop_nhau():
    """Hai bản cài đặt phải khai cùng tập nhãn _invalid_reason."""
    lech = {}
    for table in table_names():
        py, sql = spark_labels(table), duckdb_labels(table)
        if py != sql:
            lech[table] = {"chỉ có ở Spark": sorted(py - sql), "chỉ có ở DuckDB": sorted(sql - py)}

    assert not lech, (
        "Rule bronze đã lệch giữa hai môi trường — sửa một bên mà quên bên kia:\n"
        + "\n".join(f"  {t}: {d}" for t, d in lech.items())
    )


def test_moi_bang_co_it_nhat_mot_rule():
    """Bảng bronze không có rule chất lượng nào là dấu hiệu quên viết, không phải dữ liệu sạch."""
    trong = [t for t in table_names() if not spark_labels(t)]
    assert not trong, f"Không khai rule chất lượng nào: {trong}"
