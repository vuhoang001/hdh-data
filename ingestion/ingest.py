"""
Điểm vào DUY NHẤT của tầng ingestion: nạp một bảng bronze từ nguồn vào Iceberg.

    spark-submit ingest.py --table orders

Trước đây mỗi bảng có một file ingest_<bảng>.py riêng, và mỗi file viết lại bằng PySpark
đúng những gì transforms/models/bronze/bronze_<bảng>.sql đã tả bằng SQL. Giờ chỉ còn file
này: nó đọc chính model SQL đó rồi chạy bằng spark.sql().

Thêm một bảng mới = thêm một mục trong ingestion/config/sources.yml + một file SQL trong
transforms/models/bronze/. Không phải viết Python.

Chạy qua Makefile:
    make lake-ingest-orders     # một bảng
    make lake-ingest            # tất cả các bảng khai trong sources.yml
"""
import argparse
import sys

from common import config, job, spec


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("--table", help="Tên bảng bronze, vd: orders")
    parser.add_argument(
        "--list",
        action="store_true",
        help="Chỉ liệt kê các bảng đã khai rồi thoát (không cần --table)",
    )
    args = parser.parse_args()

    if args.list:
        print("\n".join(sorted(spec.load_all(config.SOURCES_FILE))))
        return 0

    if not args.table:
        parser.error("cần --table (hoặc --list để xem các bảng đã khai)")

    job.run(args.table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
