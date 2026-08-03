# hdh-data — Pipeline ETL học tập (2 môi trường)

Một pipeline ETL học Data Engineering, chạy được trên **hai môi trường độc lập** với **cùng
một bộ model dbt**:

| Môi trường | Engine | Khi nào dùng | Cần gì | Thời gian |
|---|---|---|---|---|
| **DuckDB** (nhẹ) | dbt-duckdb | Học/ dev nhanh, không cần hạ tầng | Chỉ Docker | ~vài giây |
| **Spark + Trino** (lakehouse) | Spark + Trino + Iceberg + MinIO | Giống production, dữ liệu thật trên object storage | Docker + mạng (tải jar) | ~vài phút |

Cùng logic bronze → silver → gold, chỉ khác engine thực thi. Chọn môi trường bằng tiền tố
lệnh `make`: `duckdb-*` hoặc `st-*` (spark-trino).

```
                 data/*.csv  (13 file nguồn, dùng chung)
                      │
        ┌─────────────┴──────────────┐
        ▼                            ▼
  ┌───────────────┐            ┌──────────────────────────────────────┐
  │  MÔI TRƯỜNG 1 │            │           MÔI TRƯỜNG 2               │
  │   DuckDB      │            │        Spark + Trino (lakehouse)     │
  │               │            │                                      │
  │ dbt-duckdb:   │            │ Spark ─ingest→ Iceberg(bronze)       │
  │  bronze(CSV)  │            │           trên MinIO (S3)            │
  │   → silver    │            │      ↑ metadata: Iceberg REST catalog│
  │   → gold      │            │ dbt ─(qua Trino)→ silver → gold      │
  └───────┬───────┘            └──────────────────┬───────────────────┘
          ▼                                       ▼
   file hdh.duckdb                      Trino SQL trên Iceberg
```

## Bí quyết dùng chung 1 project dbt cho 2 engine

Điểm mấu chốt để **không nhân đôi** logic silver/gold: một lớp macro mỏng che đi phần khác
biệt giữa hai engine.

- **`{{ bronze('orders') }}`** ([macros/bronze_ref.sql](dbt/hdh_dbt/macros/bronze_ref.sql)) —
  silver trỏ về đúng nguồn bronze theo môi trường:
  - target `duckdb` → `ref('bronze_orders')` (bronze là dbt model đọc CSV)
  - target `trino`  → `source('bronze', 'orders')` (bronze do Spark ghi Iceberg)
- **macro ngày tháng** ([macros/portable_dates.sql](dbt/hdh_dbt/macros/portable_dates.sql)) —
  gom vài hàm lệch nhau giữa Trino và DuckDB (`date_format`↔`strftime`,
  `day_of_week`↔`isodow`, `week_of_year`↔`week`).
- **bronze layer** chỉ build khi `target=duckdb` (khai báo `enabled` ở
  [dbt_project.yml](dbt/hdh_dbt/dbt_project.yml)); ở target `trino` bronze do Spark lo, nên
  source Iceberg được tắt khi chạy DuckDB.

Nhờ vậy mọi model silver/gold viết **một lần**, chạy đúng ở cả hai nơi.

## Yêu cầu
- Docker + Docker Compose.
- Riêng môi trường Spark+Trino: lần đầu chạy sẽ build image Spark/dbt và tải jar Iceberg → cần mạng.

## Xem toàn bộ lệnh
```bash
make            # in danh sách lệnh kèm mô tả
```

---

## Môi trường 1 — DuckDB (nhẹ, khuyến nghị để bắt đầu)

Toàn bộ pipeline (bronze đọc CSV → silver → gold + test) chạy trong **một container** bằng
dbt-duckdb. Không MinIO, không Iceberg, không Trino, không Spark.

```bash
make duckdb-up       # 1) build image + bật container (lần đầu ~30s build)
make duckdb-deps     # 2) cài dbt_utils (chạy 1 lần)
make duckdb-run      # 3) dbt build: bronze → silver → gold + 113 test (~7s)
make duckdb-query    # 4) xem thử bảng gold
```

Kiểm tra thủ công:
```bash
make duckdb-shell    # liệt kê các bảng trong file hdh.duckdb
```

Dọn dẹp:
```bash
make duckdb-down     # dừng (giữ file .duckdb)
make duckdb-clean    # dừng + xoá volume (mất file .duckdb)
```

---

## Môi trường 2 — Spark + Trino (lakehouse)

| Service | Vai trò | Cổng (localhost) |
|---|---|---|
| **MinIO** | Object storage (S3) lưu file Iceberg | 9000 API / 9001 console |
| **iceberg-rest** | REST catalog quản lý metadata Iceberg | 8181 |
| **Trino** | Engine truy vấn SQL trên Iceberg | 8080 |
| **Spark** | Ingest CSV → bronze (PySpark) | — |
| **dbt** | Transform + test silver/gold (qua Trino) | — |

```bash
make st-up           # 1) bật toàn bộ stack (lần đầu build image + tải jar)
make st-ps           # đợi tới khi trino/spark/dbt ở trạng thái "Up"

make st-ingest       # 2) Spark: CSV → 13 bảng iceberg.bronze (~2 phút)
                     #    hoặc từng bảng: make st-ingest-orders, st-ingest-products, ...

make st-dbt-deps     # 3) cài dbt_utils (chạy 1 lần)
make st-dbt          #    dbt build silver + gold + test (--target trino)

make st-query        # 4) truy vấn kết quả bằng Trino
```

Kiểm tra thủ công:
- **MinIO console:** http://localhost:9001 — user/pass trong `.env` (bucket `warehouse` chứa file Iceberg).
- **Trino CLI:** `make st-trino`
  ```sql
  SHOW SCHEMAS FROM iceberg;
  SELECT * FROM iceberg.bronze.orders LIMIT 20;
  SELECT * FROM iceberg.analytics.fact_order_items LIMIT 20;
  SELECT * FROM iceberg.analytics.gold_revenue_daily ORDER BY order_date DESC LIMIT 20;
  ```
- **Spark SQL:** `make st-spark-sql`

Dọn dẹp:
```bash
make st-down         # dừng, giữ dữ liệu
make st-clean        # dừng + xoá volume MinIO (mất sạch dữ liệu)
```

> `.env` chứa credential cho stack này (MinIO + Postgres metastore). Copy từ `.env.example`
> nếu chưa có. Môi trường DuckDB không cần `.env`.

## Tài liệu

- [Star schema](docs/star-schema.md) — thiết kế dim/fact ở gold layer, lý do thiết kế, query mẫu.
- [Star schema — lý thuyết](docs/star-schema-ly-thuyet.md) — quy trình 4 bước Kimball, bus matrix, SCD, bridge table.
- [Mô hình dữ liệu](docs/mo-hinh-du-lieu.md) — sơ đồ quan hệ 13 bảng, công thức join đúng. **Đọc trước khi viết query join.**
- [Thêm một bảng mới](docs/them-bang-moi.md) — hướng dẫn từng bước từ CSV tới gold cho cả hai môi trường.

## Cấu trúc thư mục

```
hdh-data/
├── Makefile                       # lệnh tắt: duckdb-* và st-*
├── .env / .env.example            # credential cho stack spark-trino
├── data/                          # 13 CSV nguồn (dùng chung 2 môi trường)
├── environments/
│   ├── duckdb/docker-compose.yml       # môi trường 1: 1 container dbt-duckdb
│   └── spark-trino/docker-compose.yml  # môi trường 2: minio+iceberg+spark+trino+dbt
├── dbt/
│   ├── Dockerfile                 # 1 image, 2 adapter: dbt-duckdb + dbt-trino
│   ├── profiles.yml               # 2 target: duckdb (mặc định) + trino
│   └── hdh_dbt/
│       ├── dbt_project.yml        # bronze enabled khi target=duckdb; silver=view, gold=table
│       ├── macros/
│       │   ├── bronze_ref.sql         # bronze() — chọn nguồn theo môi trường
│       │   ├── portable_dates.sql     # hàm ngày tháng portable Trino↔DuckDB
│       │   ├── bronze_helpers.sql     # read_source_csv, invalid_reason, bronze_audit
│       │   ├── generate_schema_name.sql  # dùng thẳng tên schema (bronze/analytics)
│       │   └── generate_alias_name.sql   # bỏ tiền tố bronze_ khỏi tên bảng
│       └── models/
│           ├── bronze/            # 13 model đọc CSV (CHỈ chạy ở target duckdb)
│           ├── silver/            # 6 view + _sources.yml (source Iceberg cho target trino)
│           └── gold/              # dim_*, fact_*, gold_* (star schema)
├── spark/                         # Spark jobs ingest bronze (môi trường 2)
│   ├── Dockerfile · conf/ · jobs/common/ · jobs/bronze/ (13 job)
├── trino/etc/catalog/             # catalog Trino → Iceberg REST + MinIO
└── iceberg-rest/Dockerfile        # REST catalog + driver Postgres
```

## Layer dữ liệu

Mỗi layer chịu trách nhiệm một việc: bronze **mô tả** nguồn, silver **quyết định** dữ liệu nào
dùng được, gold **trả lời** câu hỏi business.

- **bronze** — chuẩn hoá text, gắn cờ `_is_valid` + `_invalid_reason` + audit; giữ nguyên số
  dòng nguồn. Ở DuckDB do dbt model làm; ở Trino do Spark làm. **Cùng rule chất lượng.**
  13 bảng: `orders` (646.945), `order_items` (714.669), `payments`, `shipments`, `reviews`,
  `returns`, `inventory`, `customers` (121.930), `geography` (39.948), `products` (2.412),
  `promotions` (50), `sales_daily`, `web_traffic`.

  > **`sales_daily` không khớp doanh thu tính từ `order_items`** — đây là hai nguồn số độc lập;
  > chênh lệch là thứ cần điều tra, đừng "sửa" cho khớp.
- **silver** — lọc theo `_is_valid`, bỏ cột kỹ thuật, thêm cột dẫn xuất. Là **view** (rẻ, luôn
  phản ánh bronze mới nhất): `silver_orders`, `silver_order_items`, `silver_customers`,
  `silver_products`, `silver_geography`, `silver_promotions`.
- **gold** — star schema, vật liệu hoá thành **table**: `dim_customer`, `dim_product`,
  `dim_promotion`, `dim_date`, `fact_order_items`, và 2 bảng tổng hợp `gold_orders_daily`,
  `gold_revenue_daily`.

## Test dữ liệu

`make duckdb-run` / `make st-dbt` = `dbt build` — tạo model **và** test ngay sau mỗi model.
Silver fail test thì gold không build từ dữ liệu hỏng. Gồm `not_null`/`unique` cho khoá,
`accepted_values`, `relationships` (bắt dòng mồ côi), `accepted_range` (chặn số âm), và
[test hạt fact](dbt/hdh_dbt/tests/assert_fact_order_items_grain.sql) (số dòng fact = silver).

Chỉ chạy test: `make duckdb-test` hoặc `make st-dbt-test`.
