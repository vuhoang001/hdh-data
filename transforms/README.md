# transforms/

Project dbt — biến bronze thành silver rồi gold. **Một project duy nhất chạy trên hai engine**,
chọn bằng `--target duckdb|trino`.

```text
transforms/
├── dbt_project.yml     # materialization theo layer, var data_dir
├── profiles.yml        # 2 target: duckdb (mặc định) + trino — giá trị lấy từ .env
├── packages.yml        # dbt_utils
├── macros/
└── models/{bronze,silver,gold}/
```

## Vì sao một project chạy được cả hai engine

Hai môi trường tạo bronze theo hai cách khác nhau:

| | bronze đến từ đâu | silver/gold |
| --- | --- | --- |
| `--target duckdb` | dbt model đọc thẳng CSV (`models/bronze/`) | dbt trên DuckDB |
| `--target trino` | Spark ghi Iceberg (`ingestion/`) | dbt trên Trino |

Ba macro làm cho khác biệt đó biến mất khỏi code silver/gold:

| Macro | Việc nó làm |
| --- | --- |
| `bronze_ref.sql` | `{{ bronze('orders') }}` → `ref()` ở DuckDB, `source()` ở Trino. Mọi model silver viết một kiểu, chạy được cả hai engine. |
| `generate_schema_name.sql` | Dùng thẳng tên schema đã đặt (`bronze`, `analytics`), không ghép tiền tố target như mặc định của dbt |
| `generate_alias_name.sql` | Bỏ tiền tố `bronze_` khỏi tên bảng: model `bronze_orders` → bảng `bronze.orders`, khớp namespace Spark ghi vào |

Kết quả: `select * from bronze.orders` chạy y hệt ở cả hai môi trường.

`bronze_helpers.sql` chứa `read_source_csv` / `invalid_reason` / `bronze_audit` — bản DuckDB
của đúng những gì `ingestion/common/` làm ở Spark, **giữ nguyên tên nhãn lỗi** để truy vết được.

## `models/bronze/` dùng để làm gì nếu Spark đã ghi bronze rồi?

13 file trong `models/bronze/` **chỉ chạy ở target `duckdb`** (`+enabled` trong
`dbt_project.yml`). Ở target `trino` chúng bị tắt vì Spark đã ghi Iceberg — nhưng chúng
không phải code chết: đó là thứ làm cho môi trường nhẹ chạy được **mà không cần Spark**.

Đổi lại, cùng một rule chất lượng tồn tại ở hai nơi:

```text
ingestion/connectors/ingest_orders.py    ← PySpark, cho lakehouse
transforms/models/bronze/bronze_orders.sql ← SQL, cho DuckDB
```

**Sửa một bên mà quên bên kia = hai môi trường cho hai kết quả khác nhau, im lặng.**
`tests/test_bronze_parity.py` so tập nhãn `_invalid_reason` của từng cặp và fail nếu lệch.
Chạy `pytest tests -q` sau mỗi lần đổi rule bronze.

## Materialization theo layer

Khai báo ở `dbt_project.yml` theo **thư mục**, nên file mới thêm vào tự động thừa hưởng:

| Layer | Kiểu | Lý do |
| --- | --- | --- |
| `bronze` | table, **chỉ bật khi target=duckdb** | Ở Trino, Spark đã ghi Iceberg rồi — bật lên là dbt build đè lại |
| `silver` | view | Chỉ lọc/đổi tên, rẻ; luôn phản ánh bronze mới nhất |
| `gold` | table | Group by/join đắt, tính một lần |

## Cấu hình

`profiles.yml` không hardcode giá trị nào — host, port, catalog, schema đều qua
`{{ env_var(...) }}` với nguồn là `.env` ở gốc repo. Nhờ đó đổi tên catalog Iceberg là
Spark, Trino và dbt cùng đổi theo.

## Chạy

```bash
make duckdb-deps && make duckdb-run     # DuckDB: bronze -> silver -> gold + test
make lake-dbt-deps && make lake-dbt     # Trino:  silver -> gold + test
```

Cả hai đều là `dbt build` — tạo model **và** chạy test ngay sau mỗi model, nên silver fail
test thì gold không build từ dữ liệu hỏng.
