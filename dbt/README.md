# dbt/

Project dbt — biến bronze thành silver rồi gold. **Một project duy nhất chạy trên hai engine**,
chọn bằng `--target duckdb|trino`.

```text
dbt/
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
| `--target duckdb` | dbt model đọc thẳng CSV (`ingestion/bronze_specs/`) | dbt trên DuckDB |
| `--target trino` | Spark ghi Iceberg (`ingestion/`) | dbt trên Trino |

Ba macro làm cho khác biệt đó biến mất khỏi code silver/gold:

| Macro | Việc nó làm |
| --- | --- |
| `bronze_ref.sql` | `{{ bronze('orders') }}` → `ref()` ở DuckDB, `source()` ở Trino. Mọi model silver viết một kiểu, chạy được cả hai engine. |
| `generate_schema_name.sql` | Dùng thẳng tên schema đã đặt (`bronze`, `analytics`), không ghép tiền tố target như mặc định của dbt |
| `generate_alias_name.sql` | Bỏ tiền tố `bronze_` khỏi tên bảng: model `bronze_orders` → bảng `bronze.orders`, khớp namespace Spark ghi vào |

Kết quả: `select * from bronze.orders` chạy y hệt ở cả hai môi trường.

`bronze_helpers.sql` chứa `bronze_source()` — **seam duy nhất** giữa hai engine. Ở dbt nó nở
thành `read_csv(...)`; ở Spark, `ingestion/common/sql_model.py` thay nó bằng tên temp view.
Mọi thứ còn lại trong model bronze là SQL thuần, portable giữa DuckDB và Spark SQL.

## `ingestion/bronze_specs/` dùng để làm gì nếu Spark đã ghi bronze rồi?

13 file trong `ingestion/bronze_specs/` **chỉ chạy ở target `duckdb`** (`+enabled` trong
`dbt_project.yml`). Ở target `trino` chúng bị tắt vì Spark đã ghi Iceberg — nhưng chúng
không phải code chết: đó là thứ làm cho môi trường nhẹ chạy được **mà không cần Spark**.

Và chúng cũng **không bị nhân đôi**: ở target `trino`, `ingestion/ingest.py` đọc chính các
file này rồi chạy bằng `spark.sql()`. Một bản logic, hai engine thực thi.

```text
ingestion/bronze_specs/bronze_orders.sql
    ├─ dbt build  --target duckdb   → bảng trong hdh.duckdb
    └─ spark-submit ingest.py       → bảng Iceberg trên MinIO
```

**Ràng buộc kèm theo:** mọi biểu thức ở đây phải là SQL portable giữa DuckDB và Spark SQL.
`tests/test_bronze_models.py` kiểm khuôn khai báo; chạy `pytest tests -q` sau mỗi lần sửa.

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
