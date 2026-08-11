"""
Phân loại lỗi của tầng ingestion.

Vì sao cần phân loại thay vì dùng chung RuntimeError: ba loại lỗi dưới đây đòi ba
phản ứng KHÁC nhau từ phía gọi (orchestrator, CI, con người trực ca):

    ContractError   khai báo sai -> retry vô ích, phải sửa code/cấu hình rồi mới chạy lại
    SourceError     nguồn chưa sẵn sàng -> retry sau có thể thành công
    IngestionFailed lỗi lúc chạy -> retry có thể thành công, nhưng cần nhìn log

Exit code riêng cho từng loại để orchestrator quyết định retry hay báo động mà không
phải parse log.

Nguyên tắc: KHÔNG `except Exception: pass` ở bất kỳ đâu. Nuốt lỗi ở tầng dữ liệu nghĩa
là báo cáo sai mà không ai biết — tệ hơn hẳn so với pipeline đứng.
"""


class PipelineError(Exception):
    """Gốc của mọi lỗi có chủ đích trong pipeline."""

    exit_code = 1


class ContractError(PipelineError):
    """Khai báo sai: thiếu model SQL, sai kiểu, sai cú pháp partition, thiếu bảng.

    Retry KHÔNG giúp gì. Phải sửa sources.yml hoặc model SQL.
    """

    exit_code = 2


class SourceError(PipelineError):
    """Không đọc được nguồn: thiếu file, landing rỗng, không kết nối được object storage.

    Thường là lỗi tạm thời hoặc do chưa chạy bước landing.
    """

    exit_code = 3


class IngestionFailed(PipelineError):
    """Lỗi trong lúc ingest: engine chết, ghi Iceberg thất bại, hết bộ nhớ."""

    exit_code = 4
