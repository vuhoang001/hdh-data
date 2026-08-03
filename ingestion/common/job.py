"""
Khung chạy chung của MỌI bảng bronze — không còn code riêng cho từng bảng.

Luồng: đọc nguồn -> chạy SQL dùng chung -> gắn cột audit -> ghi Iceberg -> log số dòng.

Điều đáng chú ý là những gì KHÔNG còn ở đây: schema, chuẩn hoá text, luật chất lượng, cột
dẫn xuất. Toàn bộ phần đó nằm trong transforms/models/bronze/bronze_<bảng>.sql — chính file
mà dbt build ở môi trường DuckDB. Trước đây chúng được viết lại lần thứ hai bằng PySpark
trong 13 file ingest_*.py, và hai bản có thể lệch nhau mà không ai biết.

Spark chạy được đoạn SQL đó nguyên văn nhờ spark.sql(): thứ duy nhất phải thay là quan hệ
nguồn — {{ bronze_source(...) }} trở thành tên temp view mà job này vừa đăng ký.
"""
from typing import Optional

from pyspark.sql import SparkSession

from common import config, spec, sql_model
from common.iceberg import count_table_rows, create_namespace, write_iceberg_table
from common.io import read_source
from common.session import build_spark_session, get_logger

# Tên temp view mà model SQL đọc vào. Chỉ sống trong phiên Spark của job này.
SOURCE_VIEW = "bronze_source_input"


def _resolve_source(source_spec: "spec.SourceSpec", model: "sql_model.BronzeModel") -> str:
    """Định danh nguồn, tuỳ loại: CSV là đường dẫn file, DB là tên bảng.

    `source_file` được khai trong model SQL vì dbt cũng cần nó (dbt không đọc được YAML).
    Ở đây nó được diễn giải theo `type` khai trong sources.yml.
    """
    if source_spec.type == "csv":
        return config.source_path(model.source_file)
    # Loại nguồn khác (postgres, ...) tự diễn giải định danh trong reader của mình.
    return model.source_file


def run(table: str) -> None:
    """Ingest một bảng bronze từ nguồn vào Iceberg."""
    logger = get_logger(f"bronze.{table}")

    # Đọc cấu hình TRƯỚC khi dựng Spark: khai sai thì hỏng ngay trong vài mili giây,
    # thay vì sau 30 giây chờ JVM khởi động.
    source_spec = spec.load(table, config.SOURCES_FILE)
    model = sql_model.load(table, config.MODELS_DIR)
    target_table = config.bronze_table(table)

    spark: Optional[SparkSession] = None
    try:
        spark = build_spark_session(f"hdh-bronze-{table.replace('_', '-')}")

        source = _resolve_source(source_spec, model)
        logger.info("Đọc %s (nguồn: %s)", source, source_spec.type)
        read_source(spark, source_spec.type, source, model.schema) \
            .createOrReplaceTempView(SOURCE_VIEW)

        # Đây là chỗ Spark chạy CHÍNH đoạn SQL mà dbt build ở môi trường DuckDB.
        # Cột audit (_source_file, _ingested_at) do chính SQL sinh ra, không gắn thêm ở
        # Python nữa — nếu không sẽ có hai cột trùng tên.
        bronze_df = spark.sql(model.render(SOURCE_VIEW))

        create_namespace(spark, config.BRONZE_NAMESPACE)
        logger.info("Ghi bảng %s", target_table)
        write_iceberg_table(bronze_df, target_table, source_spec.partition_columns())

        written = spark.table(target_table)
        total = count_table_rows(spark, target_table)
        invalid = written.filter("not _is_valid").count()

        report = [f"{total} dòng (hợp lệ={total - invalid}, lỗi={invalid})"]
        for label, condition in source_spec.extra_metrics.items():
            report.append(f"{written.filter(condition).count()} {label}")
        logger.info("%s: %s", target_table, ", ".join(report))
    finally:
        if spark is not None:
            spark.stop()
