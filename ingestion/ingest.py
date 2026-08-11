"""
Điểm vào DUY NHẤT của tầng ingestion: landing -> bronze Iceberg.

    python ingest.py --table orders                 # dev  (DuckDB)
    spark-submit ingest.py --table orders           # prod (Spark)

Cùng một file, cùng một luồng, hai engine. Engine chọn bằng INGESTION_ENGINE trong
config/.env.<môi trường>, hoặc ghi đè bằng --engine.

Trước đây mỗi bảng có một file ingest_<bảng>.py viết lại bằng PySpark đúng những gì
ingestion/bronze_specs/bronze_<bảng>.sql đã tả bằng SQL. Giờ chỉ còn file này: nó đọc
chính model SQL đó rồi giao cho engine chạy.

Thêm một bảng mới = thêm một mục trong ingestion/config/sources.yml + một file SQL trong
ingestion/bronze_specs/. Không phải viết Python.

Chạy qua Makefile:
    make ingest-orders              # một bảng, môi trường đang chọn
    make ingest                     # tất cả các bảng khai trong sources.yml
    make ENV=prod ingest            # cùng lệnh, engine Spark
"""
import argparse
import sys

import engines
from common import config, job, spec
from common.errors import PipelineError
from common.logging import get_logger

logger = get_logger("ingest")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--table", help="Tên bảng bronze, vd: orders")
    parser.add_argument(
        "--engine",
        choices=engines.ENGINES,
        help=f"Ghi đè engine (mặc định lấy từ INGESTION_ENGINE={config.ENGINE})",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Chỉ liệt kê các bảng đã khai rồi thoát (không cần --table)",
    )
    args = parser.parse_args()

    try:
        if args.list:
            print("\n".join(sorted(spec.load_all(config.SOURCES_FILE))))
            return 0

        if not args.table:
            parser.error("cần --table (hoặc --list để xem các bảng đã khai)")

        job.run(args.table, engine_name=args.engine)
    except PipelineError as exc:
        # Exit code phân loại theo loại lỗi để orchestrator biết có nên retry không,
        # mà không phải parse log. Xem common/errors.py.
        logger.error("%s: %s", type(exc).__name__, exc)
        return exc.exit_code
    except Exception as exc:  # noqa: BLE001 — biên ngoài cùng, phải báo rồi thoát
        logger.exception("Lỗi không lường trước: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
