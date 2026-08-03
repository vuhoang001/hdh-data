"""
Khung chạy chung cho mọi bronze job.

Trước đây 13 file ingest_*.py đều lặp y hệt nhau phần: khai báo APP_NAME/NAMESPACE/TABLE/
SOURCE_CSV, parse_args(), run(), main(). Chỉ SCHEMA và transform() là khác nhau thật.

Giờ mỗi job chỉ khai báo phần riêng của nó qua BronzeJob rồi gọi run_job(). Luồng
đọc -> transform -> gắn cột audit -> ghi Iceberg -> log số dòng nằm gọn ở đây, sửa một
lần là cả 13 job đổi theo.
"""
import argparse
from dataclasses import dataclass, field
from typing import Callable, List, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from common import config
from common.iceberg import (
    add_audit_columns,
    count_table_rows,
    create_namespace,
    write_iceberg_table,
)
from common.io import read_source
from common.session import build_spark_session, get_logger


@dataclass(frozen=True)
class BronzeJob:
    """Mô tả một bảng bronze. Chỉ chứa phần RIÊNG của bảng đó.

    table        : tên bảng trong namespace bronze, vd "orders" -> iceberg.bronze.orders
    source_csv   : tên file trong thư mục data, vd "orders.csv"
    schema       : schema tường minh của nguồn (Spark áp theo THỨ TỰ cột, bỏ qua header)
    transform    : chuẩn hoá + gắn cờ chất lượng. Không được lọc bỏ dòng — bronze giữ
                   nguyên số dòng nguồn.
    partition_by : hàm trả về danh sách cột partition, hoặc None nếu để một file.
                   Phải là HÀM vì F.months()/F.years()/F.bucket() cần SparkContext đã khởi tạo.
    extra_metrics: {nhãn: điều kiện SQL} để log thêm số liệu riêng của bảng,
                   vd {"ngày bán lỗ": "_margin_negative"}.
    source_type  : loại nguồn, phải khớp `type:` khai trong sources.yml và một reader đã
                   đăng ký ở common/io.py. Đổi giá trị này là chuyển bảng sang nguồn khác
                   (vd "postgres" ở production) mà không đụng transform/silver/gold.
    """

    table: str
    source_csv: str
    schema: StructType
    transform: Callable[[DataFrame], DataFrame]
    partition_by: Optional[Callable[[], List]] = None
    extra_metrics: Mapping[str, str] = field(default_factory=dict)
    source_type: str = "csv"


def _parse_args(job: BronzeJob) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Ingest {job.source_csv} vào bronze layer"
    )
    parser.add_argument("--source-csv", default=config.source_path(job.source_csv))
    parser.add_argument("--table", default=config.bronze_table(job.table))
    return parser.parse_args()


def _ingest(spark: SparkSession, job: BronzeJob, source_csv: str, table: str, logger) -> None:
    logger.info("Đọc %s (nguồn: %s)", source_csv, job.source_type)
    df = read_source(spark, job.source_type, source_csv, job.schema)

    bronze_df = add_audit_columns(job.transform(df), source_csv)

    create_namespace(spark, config.BRONZE_NAMESPACE)
    logger.info("Ghi bảng %s", table)
    write_iceberg_table(bronze_df, table, job.partition_by() if job.partition_by else None)

    written = spark.table(table)
    total = count_table_rows(spark, table)
    invalid = written.filter("not _is_valid").count()

    report = [f"{total} dòng (hợp lệ={total - invalid}, lỗi={invalid})"]
    for label, condition in job.extra_metrics.items():
        report.append(f"{written.filter(condition).count()} {label}")
    logger.info("%s: %s", table, ", ".join(report))


def run_job(job: BronzeJob) -> None:
    """Điểm vào chuẩn của mọi bronze job: dựng Spark, chạy ingest, luôn dọn session."""
    args = _parse_args(job)
    logger = get_logger(f"bronze.{job.table}")
    spark = build_spark_session(f"hdh-bronze-{job.table.replace('_', '-')}")
    try:
        _ingest(spark, job, args.source_csv, args.table, logger)
    finally:
        spark.stop()
