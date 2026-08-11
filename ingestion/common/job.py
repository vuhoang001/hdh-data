"""
Khung chạy chung của MỌI bảng bronze, trên MỌI engine.

Luồng: đọc landing -> chạy SQL dùng chung -> ghi Iceberg -> đếm -> log.

Điều đáng chú ý là những gì KHÔNG có ở đây:

  - Không có schema, chuẩn hoá, luật chất lượng, cột dẫn xuất. Toàn bộ nằm trong
    transforms/models/bronze/bronze_<bảng>.sql — chính file mà dbt đọc.
  - Không có `if env == "dev"`. Môi trường chọn ENGINE (một object), không chọn nhánh
    code. Hàm run() dưới đây không biết mình đang chạy DuckDB hay Spark.

Đó là toàn bộ ý tưởng: một bản logic, một luồng điều phối, hai engine thực thi.
"""
from typing import Optional

import engines
from common import config, spec, sql_model
from common.errors import ContractError, IngestionFailed
from common.logging import event, get_logger, new_batch_id, timed

# Tên quan hệ nguồn mà model SQL đọc vào. Chỉ sống trong phiên của job này.
SOURCE_VIEW = "bronze_source_input"


def run(table: str, engine_name: Optional[str] = None) -> None:
    """Ingest một bảng bronze từ landing zone vào Iceberg."""
    logger = get_logger(f"bronze.{table}")
    engine_name = engine_name or config.ENGINE
    batch_id = new_batch_id()

    # Đọc cấu hình TRƯỚC khi dựng engine: khai sai thì hỏng ngay trong vài mili giây,
    # thay vì sau 30 giây chờ JVM khởi động.
    try:
        source_spec = spec.load(table, config.SOURCES_FILE)
        model = sql_model.load(table, config.MODELS_DIR)
        partition = source_spec.parse_partition()
    except (spec.SourceSpecError, sql_model.BronzeModelError) as exc:
        raise ContractError(str(exc)) from exc

    target_table = config.bronze_table(table)
    context = dict(
        table=table,
        engine=engine_name,
        env=config.ENVIRONMENT,
        batch_id=batch_id,
        target=target_table,
    )

    with timed("ingestion", **context):
        try:
            with engines.create(engine_name) as engine:
                logger.info(
                    "Đọc landing %s (engine=%s)", config.landing_uri(table), engine_name
                )
                engine.register_landing_source(SOURCE_VIEW, table, model.columns)

                # Đây là chỗ engine chạy CHÍNH đoạn SQL mà dbt cũng đọc. Cột audit
                # (_source_file, _ingested_at) do bản thân SQL sinh ra, không gắn thêm
                # ở Python — nếu không sẽ có hai cột trùng tên.
                sql = model.render(SOURCE_VIEW)

                engine.create_namespace(config.BRONZE_NAMESPACE)
                logger.info("Ghi bảng %s", target_table)
                engine.write_table_from_sql(sql, target_table, partition)

                total = engine.count(target_table)
                invalid = engine.count_where(target_table, "not _is_valid")
                extra = {
                    label: engine.count_where(target_table, condition)
                    for label, condition in source_spec.extra_metrics.items()
                }
        except (ContractError, IngestionFailed):
            raise
        except Exception as exc:
            # Bọc lại để phía gọi phân biệt được "lỗi lúc chạy" với "khai báo sai",
            # nhưng GIỮ nguyên nhân gốc trong chuỗi exception để còn debug được.
            raise IngestionFailed(f"{table}: ingest thất bại — {exc}") from exc

    event(
        "ingestion.metrics",
        rows_written=total,
        rows_valid=total - invalid,
        rows_rejected=invalid,
        **context,
        **{f"metric.{k}": v for k, v in extra.items()},
    )

    report = [f"{total} dòng (hợp lệ={total - invalid}, lỗi={invalid})"]
    report += [f"{value} {label}" for label, value in extra.items()]
    logger.info("%s: %s", target_table, ", ".join(report))
