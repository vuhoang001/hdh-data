"""
Đẩy nguồn lên LANDING ZONE: <nguồn> -> Parquet trên object storage.

    python load_landing.py              # tất cả các bảng khai trong sources.yml
    python load_landing.py --table orders

Vì sao có bước này (trước đây không có):

  1. Nguồn cũ là bind mount ./data — thứ production không bao giờ có. Landing zone biến
     nguồn thành object storage ở CẢ HAI môi trường.
  2. Parquet giữ được kiểu dữ liệu thật. Nếu để CSV, mọi engine đọc sau đó phải tự đoán
     hoặc cast; Trino qua Hive connector còn trả về toàn VARCHAR.
  3. Nó tách "biết nguồn là gì" (module này + common/io.py) khỏi "biết chạy SQL thế nào"
     (ingestion/engines/). Đổi CSV sang Postgres chỉ đụng io.py.

Ở production thật, landing zone thường do một connector (Airbyte/Fivetran/CDC) đổ vào.
Script này là bản tối giản làm đúng việc đó cho repo học tập — và vì nó chỉ là job đổi
định dạng chạy trên một máy, DuckDB là công cụ đúng bất kể môi trường nào. Đừng nhầm
việc đó với việc chọn execution engine cho pipeline.

SAMPLING (chỉ dev): sample PHẢI cascade theo khoá ngoại. Lấy ngẫu nhiên 10% mỗi bảng
độc lập sẽ tạo ra hàng loạt dòng mồ côi, làm test `relationships` ở silver đỏ ở dev
trong khi prod xanh — đúng loại lệch dev/prod mà cả lần refactor này đang xoá bỏ.
"""
import argparse
import sys
from typing import Dict, List, Optional, Tuple

from common import config, io, spec, sql_model
from common.errors import ContractError, PipelineError, SourceError
from common.logging import event, get_logger, timed

logger = get_logger("landing")

# ---------------------------------------------------------------------------
# QUY TẮC SAMPLE — thứ tự QUAN TRỌNG: mỗi bảng chỉ được tham chiếu bảng đã sample
# trước nó. `{s}` là tiền tố view đã sample.
#
# Neo là `orders` (cắt theo cửa sổ thời gian), mọi thứ khác bám theo khoá ngoại.
# Bảng nhỏ (promotions 50 dòng) giữ NGUYÊN CẢ BẢNG: thu nhỏ chúng không tiết kiệm
# gì đáng kể mà chỉ tăng rủi ro mồ côi.
# ---------------------------------------------------------------------------
SAMPLE_ORDER: List[str] = [
    "orders", "order_items", "payments", "shipments", "returns", "reviews",
    "customers", "geography", "products", "inventory",
    "promotions", "sales_daily", "web_traffic",
]

SAMPLE_RULES: Dict[str, str] = {
    # Neo thời gian, CỘNG THÊM các đơn mang hiện tượng hiếm mà test có kiểm.
    # promo_id_2 chỉ xuất hiện ở 206/714.669 dòng (0,03%): cắt theo thời gian thuần tuý
    # thì sample gần như chắc chắn không còn dòng nào, và test `at_least_one` sẽ đỏ ở dev
    # trong khi prod xanh. Sample phải giữ lại được các hiện tượng hiếm mà pipeline có
    # khẳng định điều gì đó về chúng — nếu không, dev mất đúng khả năng bắt lỗi.
    "orders":      ("order_date >= (SELECT cutoff FROM _sample_window) "
                    "OR order_id IN (SELECT order_id FROM _raw_order_items "
                    "                WHERE promo_id_2 IS NOT NULL)"),
    "order_items": "order_id   IN (SELECT order_id FROM {s}orders)",
    "payments":    "order_id   IN (SELECT order_id FROM {s}orders)",
    "shipments":   "order_id   IN (SELECT order_id FROM {s}orders)",
    "returns":     "order_id   IN (SELECT order_id FROM {s}orders)",
    "reviews":     "order_id   IN (SELECT order_id FROM {s}orders)",
    "customers":   "customer_id IN (SELECT customer_id FROM {s}orders)",
    # zip xuất hiện ở CẢ orders lẫn customers -> hợp hai tập, nếu không sẽ có đơn trỏ
    # tới zip không tồn tại trong geography.
    "geography":   ("zip IN (SELECT zip FROM {s}orders UNION "
                    "SELECT zip FROM {s}customers)"),
    "products":    "product_id IN (SELECT product_id FROM {s}order_items)",
    "inventory":   "product_id IN (SELECT product_id FROM {s}products)",
    "sales_daily": "sale_date    >= (SELECT cutoff FROM _sample_window)",
    "web_traffic": "traffic_date >= (SELECT cutoff FROM _sample_window)",
    # promotions: không có luật -> giữ nguyên cả bảng
}

SAMPLE_VIEW_PREFIX = "_s_"


def _connect():
    """DuckDB đã nạp httpfs và có secret trỏ vào MinIO."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio_s3 (
            TYPE S3,
            KEY_ID '{config.AWS_ACCESS_KEY_ID}',
            SECRET '{config.AWS_SECRET_ACCESS_KEY}',
            ENDPOINT '{config.MINIO_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{config.AWS_REGION}'
        )"""
    )
    return con


def _load_specs(tables: Optional[List[str]]) -> List[Tuple[str, "spec.SourceSpec", "sql_model.BronzeModel"]]:
    try:
        specs = spec.load_all(config.SOURCES_FILE)
    except spec.SourceSpecError as exc:
        raise ContractError(str(exc)) from exc

    wanted = tables or [t for t in SAMPLE_ORDER if t in specs] + [
        t for t in sorted(specs) if t not in SAMPLE_ORDER
    ]
    unknown = [t for t in wanted if t not in specs]
    if unknown:
        raise ContractError(
            f"Bảng chưa khai trong {config.SOURCES_FILE}: {unknown}. "
            f"Đã khai: {sorted(specs)}"
        )

    out = []
    for table in wanted:
        try:
            model = sql_model.load(table, config.MODELS_DIR)
        except sql_model.BronzeModelError as exc:
            raise ContractError(str(exc)) from exc
        out.append((table, specs[table], model))
    return out


def _register_raw(con, table: str, source_spec, model) -> None:
    """Tạo view đọc thẳng nguồn (chưa sample)."""
    if source_spec.type == "csv":
        source = config.raw_path(model.source_file)
    else:
        # Nguồn khác (postgres, api, ...) tự diễn giải định danh trong reader của mình.
        source = model.source_file
    relation = io.source_sql(source_spec.type, source, model.columns)
    con.execute(f"CREATE OR REPLACE VIEW _raw_{table} AS SELECT * FROM {relation}")


def _register_sampled(con, table: str) -> str:
    """Tạo view đã sample (hoặc trỏ thẳng vào raw nếu không sample). Trả về tên view."""
    view = f"{SAMPLE_VIEW_PREFIX}{table}"
    rule = SAMPLE_RULES.get(table) if config.SAMPLE_ENABLED else None
    if rule is None:
        con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM _raw_{table}")
    else:
        where = rule.format(s=SAMPLE_VIEW_PREFIX)
        con.execute(
            f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM _raw_{table} WHERE {where}"
        )
    return view


def _write_landing(con, table: str, view: str) -> int:
    target = f"{config.landing_uri(table)}/data.parquet"
    con.execute(
        f"COPY (SELECT * FROM {view}) TO '{target}' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true)"
    )
    return int(con.execute(f"SELECT count(*) FROM {view}").fetchone()[0])


def run(tables: Optional[List[str]] = None) -> None:
    entries = _load_specs(tables)
    con = _connect()

    try:
        for table, source_spec, model in entries:
            _register_raw(con, table, source_spec, model)

        # Cửa sổ thời gian của sample tính từ chính dữ liệu, không hardcode ngày —
        # bộ dữ liệu này kết thúc ở quá khứ nên "N tháng gần đây so với hôm nay" sẽ
        # ra bảng rỗng.
        if config.SAMPLE_ENABLED:
            if not any(t == "orders" for t, _, _ in entries):
                raise ContractError(
                    "SAMPLE_ENABLED=true nhưng không load bảng `orders` — orders là neo "
                    "của toàn bộ cascade. Chạy không kèm --table, hoặc tắt sampling."
                )
            con.execute(
                f"""
                CREATE OR REPLACE VIEW _sample_window AS
                SELECT (max(order_date) - INTERVAL '{config.SAMPLE_MONTHS} months')::DATE AS cutoff
                FROM _raw_orders"""
            )
            cutoff = con.execute("SELECT cutoff FROM _sample_window").fetchone()[0]
            logger.info(
                "Sampling BẬT: %d tháng gần nhất, mốc >= %s",
                config.SAMPLE_MONTHS, cutoff,
            )

        total = 0
        for table, _, _ in entries:
            with timed("landing", table=table, env=config.ENVIRONMENT):
                view = _register_sampled(con, table)
                rows = _write_landing(con, table, view)
                total += rows
            event("landing.metrics", table=table, rows=rows, sampled=config.SAMPLE_ENABLED)
            logger.info("%-14s -> %s (%d dòng)", table, config.landing_uri(table), rows)

        logger.info("Xong %d bảng, %d dòng vào %s", len(entries), total,
                    f"s3://{config.WAREHOUSE_BUCKET}/{config.LANDING_PREFIX}/")
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Đẩy nguồn lên landing zone (Parquet).")
    parser.add_argument("--table", action="append", help="Chỉ load bảng này (lặp lại được)")
    args = parser.parse_args()

    try:
        run(args.table)
    except PipelineError as exc:
        logger.error("%s: %s", type(exc).__name__, exc)
        return exc.exit_code
    except Exception as exc:  # noqa: BLE001 — biên ngoài cùng, phải báo rồi thoát
        logger.exception("Lỗi không lường trước: %s", exc)
        return SourceError.exit_code
    return 0


if __name__ == "__main__":
    sys.exit(main())
