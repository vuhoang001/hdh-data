"""
Kiểm KHUÔN của tầng bronze — thay cho test_bronze_parity.py cũ.

Trước đây bronze có HAI bản cài đặt (13 file .sql cho DuckDB + 13 file ingest_*.py cho
Spark) và test cũ có nhiệm vụ canh cho chúng khỏi lệch nhau. Nhưng nó chỉ so được TẬP NHÃN
`_invalid_reason`, không so được điều kiện bên trong — sửa `< 0` thành `<= 0` ở một bên thì
test vẫn xanh.

Giờ chỉ còn MỘT bản: transforms/models/bronze/bronze_<bảng>.sql, và cả dbt lẫn Spark cùng
chạy nó. Lệch không còn khả năng xảy ra — không phải "được phát hiện sớm hơn" mà là không
tồn tại. Nên test đổi nhiệm vụ: từ *dò lệch* sang *kiểm khai báo đúng khuôn mà cả hai
engine cùng đọc được*.

Test dùng thẳng ingestion/common/{spec,sql_model}.py chứ không tự parse lại bằng regex —
tức nó kiểm đúng đoạn code mà Spark chạy lúc thật, không phải một bản mô phỏng.

Hai module đó cố ý không import pyspark ở cấp module, nên test này chạy được trong CI nhẹ
(chỉ cần pytest + pyyaml, không cần cài Spark 300MB):

    pytest tests -q
"""
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "ingestion"))

from common import spec, sql_model  # noqa: E402  (sau khi đã chỉnh sys.path)

SOURCES_FILE = REPO_ROOT / "ingestion" / "config" / "sources.yml"
MODELS_DIR = REPO_ROOT / "transforms" / "models" / "bronze"
DATA_DIR = REPO_ROOT / "data"
IO_FILE = REPO_ROOT / "ingestion" / "common" / "io.py"

SPECS = spec.load_all(str(SOURCES_FILE))
TABLES = sorted(SPECS)


def load_model(table):
    return sql_model.load(table, str(MODELS_DIR))


# ---------------------------------------------------------------------------
# Đăng ký nguồn <-> model SQL: thiếu một bên thì hoặc bảng không bao giờ được
# ingest, hoặc `make lake-ingest` chết giữa chừng sau khi đã dựng stack vài phút.
# ---------------------------------------------------------------------------
def test_dang_ky_khong_rong():
    assert TABLES, "sources.yml không khai báo bảng nào"


def test_moi_bang_co_model_sql():
    thieu = [t for t in TABLES if not (MODELS_DIR / f"bronze_{t}.sql").exists()]
    assert not thieu, f"Khai trong sources.yml nhưng thiếu model SQL: {thieu}"


def test_moi_model_sql_duoc_khai_bao():
    """Model mồ côi sẽ được dbt build nhưng không bao giờ được Spark ingest."""
    tren_dia = {p.stem[len("bronze_"):] for p in MODELS_DIR.glob("bronze_*.sql")}
    mo_coi = sorted(tren_dia - set(TABLES))
    assert not mo_coi, f"Có model SQL nhưng chưa khai trong sources.yml: {mo_coi}"


# ---------------------------------------------------------------------------
# Từng model phải đọc và render được — đây là chính đoạn code Spark chạy lúc thật.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table", TABLES)
def test_model_doc_duoc(table):
    m = load_model(table)
    assert m.columns, f"{table}: không khai cột nào"
    assert m.source_file, f"{table}: thiếu source_file"


@pytest.mark.parametrize("table", TABLES)
def test_model_render_khong_con_jinja(table):
    """render() tự raise nếu còn thẻ Jinja; gọi ở đây để lỗi lộ ra lúc test, không phải
    lúc Spark đã chạy được nửa job."""
    sql = load_model(table).render("src_view")
    assert "src_view" in sql, f"{table}: quan hệ nguồn chưa được thay"


@pytest.mark.parametrize("table", TABLES)
def test_model_co_cot_audit(table):
    """_source_file và _ingested_at do SQL sinh ra (không còn gắn ở Python), nên chúng
    phải có mặt trong chính model — thiếu là bảng bronze mất khả năng truy vết nguồn."""
    sql = re.sub(r"\s+", " ", load_model(table).render("src_view"))
    for cot in ("as _source_file", "as _ingested_at", "as _is_valid"):
        assert cot in sql, f"{table}: thiếu cột `{cot}`"


@pytest.mark.parametrize("table", TABLES)
def test_model_co_it_nhat_mot_luat_chat_luong(table):
    """Bảng bronze không có luật nào là dấu hiệu quên viết, không phải dữ liệu sạch."""
    sql = load_model(table).render("src_view")
    luat = re.findall(r"case when .+? then '(\w+)' end", sql, re.S)
    assert luat, f"{table}: không khai luật chất lượng nào"


@pytest.mark.parametrize("table", TABLES)
def test_source_file_khop_dang_ky(table):
    """`file:` trong sources.yml chỉ để tra cứu; giá trị thật dùng lúc chạy nằm trong model
    SQL. Hai bên lệch nhau nghĩa là bảng tra cứu đang nói dối người đọc."""
    text = SOURCES_FILE.read_text(encoding="utf-8")
    block = re.search(rf"-\s*table:\s*{table}\b(.*?)(?=\n\s*-\s*table:|\Z)", text, re.S)
    assert block, f"{table}: không thấy mục trong sources.yml"
    khai = re.search(r"^\s*file:\s*(\S+)", block.group(1), re.M)
    assert khai, f"{table}: thiếu trường `file:` trong sources.yml"
    assert khai.group(1) == load_model(table).source_file, (
        f"{table}: sources.yml ghi file={khai.group(1)} nhưng model SQL khai "
        f"source_file={load_model(table).source_file}"
    )


# ---------------------------------------------------------------------------
# Cấu hình ingest (sources.yml)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("table", TABLES)
def test_partition_hop_le(table):
    """Khai sai partition thì bảng Iceberg ra sai layout — và chỉ lộ ra SAU KHI đã ghi
    xong vài trăm nghìn dòng. parse_partition() kiểm được mà không cần Spark."""
    SPECS[table].parse_partition()


@pytest.mark.parametrize("table", TABLES)
def test_file_csv_ton_tai(table):
    """Chỉ áp dụng cho type=csv — nguồn khác (postgres, api) không phải file trên đĩa."""
    if SPECS[table].type != "csv":
        pytest.skip(f"{table}: nguồn loại {SPECS[table].type}, không phải file")
    f = DATA_DIR / load_model(table).source_file
    assert f.exists(), f"{table}: không thấy file nguồn {f}"


def test_loai_nguon_da_co_reader():
    """Khai `type: postgres` mà chưa viết reader thì job chết lúc chạy Spark — bắt ở đây.

    Đọc thẳng SOURCE_READERS trong io.py thay vì hardcode danh sách, để thêm reader mới
    là test tự nới theo. Bỏ dòng comment trước khi tìm: docstring trong io.py có ví dụ
    minh hoạ `SOURCE_READERS = {..., "postgres": ...}` mà regex sẽ khớp nhầm.
    """
    src = "\n".join(
        line for line in IO_FILE.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    block = re.search(r"^SOURCE_READERS\s*=\s*\{(.*?)\}", src, re.S | re.M)
    assert block, "Không tìm thấy SOURCE_READERS trong ingestion/common/io.py"
    da_dang_ky = set(re.findall(r'"(\w+)"\s*:', block.group(1)))

    la = sorted({s.type for s in SPECS.values()} - da_dang_ky)
    assert not la, (
        f"sources.yml khai loại nguồn chưa có reader: {la}. "
        f"Đã đăng ký: {sorted(da_dang_ky)}. Thêm vào ingestion/common/io.py."
    )


def test_khong_con_connector_rieng_le():
    """Chốt chặn tái phát: nếu ai đó thêm lại ingestion/connectors/ingest_<bảng>.py thì
    bronze lại có hai bản cài đặt, và toàn bộ lý do của lần refactor này biến mất."""
    connectors = REPO_ROOT / "ingestion" / "connectors"
    assert not connectors.exists(), (
        f"{connectors} đã xuất hiện trở lại. Logic bronze phải nằm DUY NHẤT trong "
        f"transforms/models/bronze/bronze_<bảng>.sql — Spark chạy chính file đó qua "
        f"ingestion/ingest.py."
    )
