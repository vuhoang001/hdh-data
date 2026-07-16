# hdh-data — Pipeline ETL học tập

Một pipeline ETL chạy hoàn toàn bằng Docker để học Data Engineering:

```
        CSV thô
           │
           ▼
   ┌───────────────┐   ghi bảng    ┌──────────────────┐
   │   Spark (ETL) │──────────────▶│  Iceberg (raw)   │
   └───────────────┘   Iceberg     └────────┬─────────┘
                                            │  metadata: Iceberg REST catalog
                                            │  file:     MinIO (S3)
                                            ▼
   ┌───────────────┐   transform   ┌──────────────────┐   query   ┌────────────┐
   │  dbt (test)   │──────────────▶│ Iceberg analytics│──────────▶│   Trino    │
   └───────────────┘   qua Trino   └──────────────────┘   SQL     └────────────┘
```

## Thành phần

| Service        | Vai trò                                   | Cổng (localhost)         |
|----------------|-------------------------------------------|--------------------------|
| **MinIO**      | Object storage (S3) lưu file dữ liệu      | 9000 API / 9001 console  |
| **iceberg-rest** | REST catalog quản lý metadata Iceberg   | 8181                     |
| **Trino**      | Engine truy vấn SQL trên Iceberg          | 8080                     |
| **Spark**      | Chạy pipeline ETL (PySpark)               | —                        |
| **dbt**        | Transform + test dữ liệu (qua Trino)      | —                        |

## Yêu cầu
- Docker + Docker Compose (đã có sẵn trên máy).
- Lần đầu chạy sẽ build image Spark/dbt và tải jar Iceberg → cần mạng.

## Chạy pipeline (4 bước)

```bash
cd ~/learn/hdh-data

# 1) Khởi động toàn bộ stack (lần đầu sẽ build image)
make up
make ps          # đợi tới khi trino/spark/dbt ở trạng thái "Up"

# 2) Ingest: Spark đọc data/orders.csv -> bảng Iceberg iceberg.raw.orders
make ingest

# 3) Transform + Test: dbt build staging + marts, chạy các test dữ liệu
make dbt-deps    # cài dbt_utils (chạy 1 lần)
make dbt         # = dbt build (chạy model + test)

# 4) Truy vấn kết quả bằng Trino
make query
```

> Không dùng `make`? Xem các lệnh `docker compose ...` tương ứng trong `Makefile`.

## Kiểm tra thủ công

**MinIO console:** http://localhost:9001 — user `admin` / pass `password123`
(sẽ thấy bucket `warehouse` chứa file Iceberg sau khi ingest).

**Trino CLI:**
```bash
make trino
```
```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.raw.orders ORDER BY order_id;
SELECT * FROM iceberg.analytics.stg_orders;
SELECT * FROM iceberg.analytics.orders_daily ORDER BY order_date;
```

**Spark SQL:**
```bash
make spark-sql
```
```sql
SELECT status, count(*) FROM iceberg.raw.orders GROUP BY status;
```

## Cấu trúc thư mục

```
hdh-data/
├── docker-compose.yml          # định nghĩa 5 service
├── .env                        # credential & version dùng chung
├── Makefile                    # lệnh tắt (up/ingest/dbt/query)
├── data/orders.csv             # dữ liệu nguồn mẫu
├── trino/etc/catalog/
│   └── iceberg.properties       # catalog Trino -> REST + MinIO
├── spark/
│   ├── Dockerfile              # Spark 3.5 + jar Iceberg/AWS
│   ├── conf/spark-defaults.conf # cấu hình catalog Iceberg cho Spark
│   └── jobs/ingest_orders.py    # job ETL: CSV -> Iceberg
└── dbt/
    ├── Dockerfile              # dbt-core + dbt-trino
    ├── profiles.yml            # kết nối dbt -> Trino
    └── hdh_dbt/                # dbt project (models + tests)
        ├── models/staging/     # stg_orders (view) + source + tests
        └── models/marts/       # orders_daily (table) + tests
```

## Layer dữ liệu
- **raw** (`iceberg.raw.orders`): Spark ghi thẳng từ CSV, phân vùng theo `order_date`.
- **staging** (`iceberg.analytics.stg_orders`): dbt làm sạch, chuẩn hoá — view.
- **marts** (`iceberg.analytics.orders_daily`): dbt tổng hợp doanh thu/ngày — table.

## Test dữ liệu (dbt)
- `not_null`, `unique` cho `order_id`
- `accepted_values` cho `status` (completed/pending/cancelled)
- `accepted_range` cho `revenue > 0` (dùng dbt_utils)

Chỉ chạy test: `make dbt-test`

## Dọn dẹp
```bash
make down        # dừng, giữ dữ liệu
make clean       # dừng + xoá volume MinIO (mất sạch dữ liệu)
```
