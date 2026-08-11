"""
Logging cho tầng ingestion.

Hai kênh, hai mục đích khác nhau — cố ý không gộp:

    logger.info(...)   người đọc, khi đang ngồi nhìn terminal
    event(...)         máy đọc, một dòng JSON mỗi sự kiện

Có JSON là vì câu hỏi "job tối qua ingest bao nhiêu dòng, mất bao lâu" không trả lời
được bằng cách grep log dạng câu chữ. Ghi ra stdout chứ không đẩy sang hệ thống metrics
nào — ở quy mô này, `docker logs | jq` là đủ, và thêm hạ tầng metrics lúc chưa cần chính
là thứ làm dự án phình ra vô ích.
"""
import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager

_JSON_STREAM = sys.stdout


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def new_batch_id() -> str:
    """Định danh một lần chạy. Cho phép truy vết và (sau này) reprocess theo batch."""
    return uuid.uuid4().hex[:12]


def event(name: str, **fields) -> None:
    """Ghi một sự kiện dạng JSON lines ra stdout."""
    payload = {"event": name}
    payload.update({k: v for k, v in fields.items() if v is not None})
    _JSON_STREAM.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    _JSON_STREAM.flush()


@contextmanager
def timed(event_name: str, **fields):
    """Phát <event>.started / .completed / .failed kèm thời lượng.

    Sự kiện `.failed` mang theo loại exception và thông điệp, rồi ném tiếp lỗi ra
    ngoài — quan sát được không có nghĩa là nuốt lỗi.
    """
    started = time.monotonic()
    event(f"{event_name}.started", **fields)
    try:
        yield
    except Exception as exc:
        event(
            f"{event_name}.failed",
            duration_ms=round((time.monotonic() - started) * 1000),
            error_type=type(exc).__name__,
            error=str(exc).splitlines()[0][:500] if str(exc) else None,
            **fields,
        )
        raise
    else:
        event(
            f"{event_name}.completed",
            duration_ms=round((time.monotonic() - started) * 1000),
            **fields,
        )
