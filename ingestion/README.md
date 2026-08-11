# ingestion/

Đưa dữ liệu thô vào **bronze layer** dưới dạng bảng Iceberg, chỉ dùng ở môi trường lakehouse
(`make lake-*`).

```text
ingestion/
├── ingest.py             # ĐIỂM VÀO DUY NHẤT — chạy chung cho mọi bảng
├── config/sources.yml    # ĐĂNG KÝ NGUỒN — hạ tầng ingest của 13 bảng
└── common/               # hạ tầng dùng chung, không biết gì về bảng cụ thể
```

Không có thư mục `connectors/`, và đó là điểm chính của thư mục này.

## Logic bronze KHÔNG nằm ở đây

Schema, chuẩn hoá text, luật chất lượng, cột dẫn xuất và cột audit của mọi bảng nằm ở
**`ingestion/bronze_specs/bronze_<bảng>.sql`** — chính file mà dbt build ở môi trường
DuckDB. Spark chạy nguyên văn file đó qua `spark.sql()`.

```text
        ingestion/bronze_specs/bronze_orders.sql      ← MỘT bản logic
                        │
        ┌───────────────┴────────────────┐
        ▼                                ▼
   dbt-duckdb                       ingestion/ingest.py
   render Jinja                     common/sql_model.py render
   bronze_source() → read_csv(...)  bronze_source() → tên temp view
        ▼                                ▼
   bảng trong hdh.duckdb            bảng Iceberg trên MinIO
```

Thứ duy nhất khác nhau giữa hai engine là **quan hệ nguồn**. Mọi thứ còn lại — kể cả
`_source_file` và `_ingested_at` — do chính đoạn SQL đó sinh ra, nên hai môi trường tạo
ra bộ cột giống hệt nhau mà không cần ai đồng bộ với ai.

> **Trước đây bronze có hai bản cài đặt** (13 file `connectors/ingest_*.py` bằng PySpark +
> 13 file `.sql` cho DuckDB) và phải có `tests/test_bronze_parity.py` canh cho chúng khỏi
> lệch. Test đó chỉ so được *tập nhãn* `_invalid_reason`, không so được điều kiện bên
> trong — đổi `< 0` thành `<= 0` ở một bên thì nó vẫn xanh. Giờ chỉ còn một bản, nên lệch
> không còn khả năng xảy ra.

**Đổi lại:** mọi biểu thức trong model bronze phải là SQL portable, chạy giống nhau trên
DuckDB lẫn Spark SQL. Các hàm đang dùng đều đạt: `trim`, `lower`, `nullif`, `concat_ws`,
`case/when`, `is null`, `not in`, `not between`, `year()`, `month()`, `current_timestamp`.
Cần cú pháp riêng của một engine thì hoặc viết lại cho portable, hoặc dùng SQLGlot để
transpile — **đừng tách thành hai bản cài đặt lần nữa**.

## Ranh giới hai file cấu hình

| File | Chứa gì | Ai đọc |
| --- | --- | --- |
| `ingestion/bronze_specs/bronze_<bảng>.sql` | **Logic** — schema, chuẩn hoá, luật chất lượng, cột dẫn xuất | dbt **và** Spark |
| `ingestion/config/sources.yml` | **Hạ tầng ingest** — nguồn lấy ở đâu, partition Iceberg kiểu gì | chỉ Spark |

Cố ý không chồng lấn. Schema nằm trong file SQL chứ không phải YAML vì **dbt không đọc
được YAML** — để trong YAML là lại có hai nơi khai schema.

## Các module trong `common/`

| Module | Vai trò | Cần pyspark? |
| --- | --- | --- |
| `config.py` | Nơi duy nhất biết tên catalog, namespace, thư mục dữ liệu — đọc từ `.env` | không |
| `sql_model.py` | Đọc model bronze dùng chung với dbt, render cho Spark | chỉ `.schema` |
| `spec.py` | Đọc `sources.yml`, phân tích chuỗi `partition_by` | chỉ `partition_columns()` |
| `job.py` | Khung chạy: đọc nguồn → chạy SQL → ghi Iceberg → log | có |
| `session.py` | SparkSession + logger | có |
| `io.py` | Đọc nguồn theo loại (`SOURCE_READERS`) | có |
| `iceberg.py` | Ghi bảng Iceberg | có |

`sql_model.py` và `spec.py` **cố ý không import pyspark ở cấp module** để `tests/` và job
lint trên CI chạy được mà không phải cài cả bộ Spark 300MB.

## Chạy

```bash
make lake-ingest-orders     # một bảng
make lake-ingest            # tất cả bảng khai trong sources.yml
make lake-ingest-list       # xem danh sách bảng đã khai
```

Bên dưới là `spark-submit ingest.py --table orders`.

## Hợp đồng của bronze layer

1. **Giữ nguyên số dòng nguồn.** Model không được lọc bỏ dòng. Dòng hỏng được *gắn cờ*
   `_is_valid = false` kèm `_invalid_reason`; việc loại bỏ là chuyện của silver.
2. **Schema tường minh, áp theo thứ tự cột.** Cả hai engine bỏ qua tên trong header, nên
   đổi tên cột ngay ở khối `{% set columns %}` được (`Date` → `sale_date`). Cái giá: nếu
   nguồn đổi *thứ tự* cột, dữ liệu vào nhầm cột mà không có lỗi nào báo.
3. **Mọi bảng có cột audit** `_source_file` và `_ingested_at`, do chính model SQL sinh ra.

## Khi nguồn không còn là CSV (local → production)

`SOURCE_READERS` trong `common/io.py` là seam. Chuyển `orders` sang Postgres:

```python
# 1. ingestion/common/io.py — viết reader
def read_jdbc(spark, source, schema):
    return spark.read.format("jdbc").option("url", source).option("dbtable", ...).load()

# 2. đăng ký nó
SOURCE_READERS = {"csv": read_csv, "postgres": read_jdbc}
```

```yaml
# 3. ingestion/config/sources.yml
  - table: orders
    type: postgres          # ← đổi ở đây
```

Model SQL, silver, gold và mọi test dbt **không sửa dòng nào**.

`type:` không phải chú thích trang trí: `tests/test_bronze_models.py` đối chiếu nó với
`SOURCE_READERS`, nên khai một loại chưa có reader là fail ngay.

> **Còn thiếu ở phía DuckDB:** `bronze_source()` hiện chỉ sinh `read_csv(...)`. Muốn môi
> trường local cũng đọc được Postgres thì macro cần dispatch theo `type` (DuckDB có sẵn
> `postgres_scan`). Chưa làm — nên hiện tại `type: postgres` mới chỉ đúng cho lakehouse.

## Chỗ CHƯA sẵn sàng cho production: chế độ ghi

Đọc nguồn thì đã có seam. **Ghi thì chưa.** `common/iceberg.py` ghi bằng `createOrReplace()`
— ghi đè toàn bộ bảng mỗi lần chạy.

Điều này đúng ở đây vì nguồn là file CSV tĩnh và bảng lớn nhất chỉ 714k dòng — nạp lại toàn
bộ mất vài giây và cho tính **idempotent** miễn phí. Nó **không dùng được** với bảng 500
triệu dòng nạp hàng ngày. Lúc đó cần ba thứ hiện chưa có:

| Cần gì | Vì sao | Đụng vào đâu |
| --- | --- | --- |
| **Watermark / incremental** | Chỉ đọc dòng đổi từ lần chạy trước | `io.py` + lưu trạng thái lần chạy |
| **MERGE INTO thay vì replace** | Ghi đè cả bảng 500M dòng mỗi ngày là bất khả thi | `iceberg.py` |
| **Backfill theo khoảng ngày** | Chạy lại một tháng cũ mà không đụng dữ liệu khác | tham số cho `job.run()` |

Đây là **giới hạn có thật, không phải thiếu sót do quên**. Xây incremental cho một file CSV
tĩnh là viết code không ai chạy được. Nhưng khi nguồn thật xuất hiện, đây là việc lớn nhất —
lớn hơn nhiều so với đổi reader.

## Thêm một bảng mới

1. Thêm một mục vào `config/sources.yml`
2. Tạo `../ingestion/bronze_specs/bronze_<bảng>.sql`
3. `pytest tests -q` — bắt ngay nếu hai bước trên lệch nhau
4. `make lake-ingest-<bảng>`

**Không phải viết Python**, và không phải sửa Makefile (danh sách target sinh từ
`sources.yml`). Chi tiết tới tận gold: [`docs/them-bang-moi.md`](../docs/them-bang-moi.md).

## Vì sao chọn partition như vậy

`sources.yml` khai chiến lược partition của từng bảng cùng số dòng. Đây là **nguồn thật**,
không còn là chú thích — chuỗi `months(order_date)` được phân tích thành hàm biến đổi
Iceberg lúc ghi. Nguyên tắc: **partition quá mịn còn hại hơn không partition** — 650k dòng
trải 2012-2023 mà partition theo ngày sẽ ra ~3800 file vài chục KB và làm writer OOM.
