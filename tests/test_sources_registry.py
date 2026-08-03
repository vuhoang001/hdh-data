"""
Kiểm tra ĐĂNG KÝ NGUỒN khớp với thực tế trên đĩa.

ingestion/config/sources.yml là nơi duy nhất liệt kê bảng bronze, và Makefile sinh
target `make lake-ingest-<bảng>` từ đó. Nếu đăng ký lệch với file connector hoặc file CSV,
lỗi chỉ lộ ra lúc chạy Spark — lúc đó đã tốn vài phút dựng stack. Test này bắt ngay.

Không phụ thuộc thư viện ngoài (không cần pyyaml/pyspark) để chạy được trong CI nhẹ:
    pytest tests -q
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES_FILE = REPO_ROOT / "ingestion" / "config" / "sources.yml"
CONNECTORS_DIR = REPO_ROOT / "ingestion" / "connectors"
DATA_DIR = REPO_ROOT / "data"


def read_registry():
    """Đọc các cặp (table, file) từ sources.yml.

    Parse bằng regex thay vì pyyaml để test chạy được ở môi trường tối giản. Cấu trúc
    file cố ý phẳng và đơn giản nên cách này đủ dùng; nếu sau này file phức tạp hơn thì
    đổi sang pyyaml.
    """
    text = SOURCES_FILE.read_text(encoding="utf-8")
    tables = re.findall(r"^\s*-\s*table:\s*(\S+)", text, re.MULTILINE)
    files = re.findall(r"^\s*file:\s*(\S+)", text, re.MULTILINE)
    assert len(tables) == len(files), (
        f"sources.yml lệch: {len(tables)} mục 'table' nhưng {len(files)} mục 'file'"
    )
    return list(zip(tables, files))


def test_registry_khong_rong():
    assert read_registry(), "sources.yml không khai báo bảng nào"


def test_moi_bang_co_file_connector():
    """Mỗi bảng trong đăng ký phải có connectors/ingest_<bảng>.py tương ứng."""
    thieu = [
        table for table, _ in read_registry()
        if not (CONNECTORS_DIR / f"ingest_{table}.py").exists()
    ]
    assert not thieu, f"Khai báo trong sources.yml nhưng thiếu file connector: {thieu}"


def test_moi_connector_duoc_khai_bao():
    """Chiều ngược lại: connector mồ côi sẽ không bao giờ được `make lake-ingest` gọi."""
    da_khai_bao = {table for table, _ in read_registry()}
    mo_coi = [
        path.stem[len("ingest_"):]
        for path in sorted(CONNECTORS_DIR.glob("ingest_*.py"))
        if path.stem[len("ingest_"):] not in da_khai_bao
    ]
    assert not mo_coi, f"Có file connector nhưng chưa khai báo trong sources.yml: {mo_coi}"


def test_moi_file_csv_ton_tai():
    """Chỉ áp dụng cho nguồn type=csv — nguồn khác (postgres, api) không phải file trên đĩa."""
    thieu = [
        f for (_, f), t in zip(read_registry(), read_source_types())
        if t == "csv" and not (DATA_DIR / f).exists()
    ]
    assert not thieu, f"sources.yml trỏ tới file CSV không có trong data/: {thieu}"


def read_source_types():
    return re.findall(r"^\s*type:\s*(\S+)", SOURCES_FILE.read_text(encoding="utf-8"), re.MULTILINE)


def test_moi_bang_khai_loai_nguon():
    assert len(read_source_types()) == len(read_registry()), (
        "Mỗi bảng trong sources.yml phải khai `type:` (loại nguồn)"
    )


def test_loai_nguon_da_co_reader():
    """Khai `type: postgres` mà chưa viết reader thì job sẽ chết lúc chạy Spark — bắt ở đây.

    Đọc thẳng SOURCE_READERS trong io.py thay vì hardcode danh sách, để thêm reader mới
    là test tự nới theo, không phải sửa hai chỗ.
    """
    # Bỏ dòng comment trước khi tìm: docstring/comment trong io.py có ví dụ minh hoạ
    # `SOURCE_READERS = {..., "postgres": ...}` và regex sẽ khớp nhầm vào đó.
    io_src = "\n".join(
        line for line in (REPO_ROOT / "ingestion" / "common" / "io.py")
        .read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("#")
    )
    block = re.search(r"^SOURCE_READERS\s*=\s*\{(.*?)\}", io_src, re.DOTALL | re.MULTILINE)
    assert block, "Không tìm thấy SOURCE_READERS trong ingestion/common/io.py"
    da_dang_ky = set(re.findall(r'"(\w+)"\s*:', block.group(1)))

    la = sorted(set(read_source_types()) - da_dang_ky)
    assert not la, (
        f"sources.yml khai loại nguồn chưa có reader: {la}. "
        f"Đã đăng ký: {sorted(da_dang_ky)}. Thêm vào ingestion/common/io.py."
    )


def test_connector_dung_khung_chung():
    """Mọi connector phải khai báo BronzeJob + gọi run_job — không tự viết lại boilerplate."""
    sai = [
        path.name for path in sorted(CONNECTORS_DIR.glob("ingest_*.py"))
        if "BronzeJob(" not in path.read_text(encoding="utf-8")
        or "run_job(" not in path.read_text(encoding="utf-8")
    ]
    assert not sai, f"Connector không dùng khung chung common/job.py: {sai}"
