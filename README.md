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

# 2) Ingest: Spark đọc CSV -> bảng Iceberg trong iceberg.bronze
make ingest                # toàn bộ bảng; hoặc make ingest-orders / make ingest-order-items

# 3) Transform + Test: dbt build silver + gold, chạy các test dữ liệu
make dbt-deps    # cài dbt_utils (chạy 1 lần)
make dbt         # = dbt build (chạy model + test)

# 4) Truy vấn kết quả bằng Trino
make query
```

> Không dùng `make`? Xem các lệnh `docker compose ...` tương ứng trong `Makefile`.

## Tài liệu

- [Thêm một bảng mới vào pipeline](docs/them-bang-moi.md) — hướng dẫn từng bước từ CSV tới
  gold, kèm quy ước cho từng layer và các lỗi hay gặp.

## Kiểm tra thủ công

**MinIO console:** http://localhost:9001 — dùng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` trong `.env`
(sẽ thấy bucket `warehouse` chứa file Iceberg sau khi ingest).

**Trino CLI:**
```bash
make trino
```
```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.bronze.orders LIMIT 20;
SELECT * FROM iceberg.analytics.silver_orders LIMIT 20;
SELECT * FROM iceberg.analytics.gold_orders_daily ORDER BY order_date LIMIT 20;
SELECT * FROM iceberg.analytics.gold_revenue_daily ORDER BY order_date DESC LIMIT 20;
```

**Spark SQL:**
```bash
make spark-sql
```
```sql
SELECT order_status, count(*) FROM iceberg.bronze.orders GROUP BY order_status;
```

## Cấu trúc thư mục

```
hdh-data/
├── docker-compose.yml          # định nghĩa các service
├── .env                        # credential & version dùng chung
├── Makefile                    # lệnh tắt (up/ingest/dbt/query)
├── data/                       # CSV nguồn
├── iceberg-rest/
│   └── Dockerfile              # REST catalog + driver Postgres
├── trino/etc/catalog/
│   └── iceberg.properties      # catalog Trino -> REST + MinIO
├── spark/
│   ├── Dockerfile              # Spark 3.5 + jar Iceberg/AWS
│   ├── conf/spark-defaults.conf # cấu hình catalog Iceberg cho Spark
│   └── jobs/
│       ├── common/             # CHỈ hạ tầng: session, đọc file, ghi Iceberg
│       │   ├── session.py
│       │   ├── io.py
│       │   └── iceberg.py
│       └── bronze/             # mỗi bảng 1 job, tự giữ logic của mình
│           ├── ingest_orders.py
│           └── ingest_order_items.py
├── docs/
│   └── them-bang-moi.md        # hướng dẫn thêm bảng mới
└── dbt/
    ├── Dockerfile              # dbt-core + dbt-trino
    ├── profiles.yml            # kết nối dbt -> Trino
    └── hdh_dbt/                # dbt project (models + tests)
        ├── models/silver/      # silver_orders, silver_order_items (view) + source + tests
        └── models/gold/        # gold_orders_daily, gold_revenue_daily (table) + tests
```

`common/` chỉ chứa phần kỹ thuật dùng lại được (tạo SparkSession, đọc CSV, ghi Iceberg lên
MinIO). Schema và rule làm sạch của từng bảng nằm trong chính file job ở `bronze/`, nên thêm
bảng mới = thêm 1 file, không phải sửa `common/`.

## Layer dữ liệu

Mỗi layer chịu trách nhiệm một việc: bronze **mô tả** nguồn, silver **quyết định** dữ liệu nào
dùng được, gold **trả lời** câu hỏi business.

- **bronze** — Spark ingest CSV, chuẩn hoá text, gắn cờ `_is_valid` + metadata audit. Giữ
  nguyên số dòng nguồn, không lọc bỏ gì (để luôn truy ngược được về dữ liệu gốc).
  - `iceberg.bronze.orders` — đơn hàng
  - `iceberg.bronze.order_items` — dòng hàng
- **silver** — dbt lọc theo `_is_valid`, bỏ cột kỹ thuật, thêm cột dẫn xuất. Là **view** vì
  chỉ lọc/đổi tên nên rẻ, và luôn phản ánh bronze mới nhất.
  - `iceberg.analytics.silver_orders`
  - `iceberg.analytics.silver_order_items` — thêm `line_amount`
- **gold** — dbt tổng hợp. Là **table** vì `group by`/`join` trên hàng trăm nghìn dòng thì
  đắt, tính một lần rồi dùng lại.
  - `iceberg.analytics.gold_orders_daily` — số đơn theo ngày
  - `iceberg.analytics.gold_revenue_daily` — doanh thu theo ngày (join items với orders)

> Cả silver lẫn gold đều nằm trong schema `analytics` (khai báo ở `dbt/profiles.yml`);
> `silver_`/`gold_` là tiền tố quy ước, không phải schema riêng.

## Test dữ liệu (dbt)

`make dbt` = `dbt build` — tạo model **và** test ngay sau mỗi model, theo đúng thứ tự phụ
thuộc. Silver fail test thì gold không được build từ dữ liệu hỏng đó.

- `not_null`, `unique` cho khoá chính (`orders.order_id`, `gold_*.order_date`)
- `accepted_values` cho `order_status` (created/paid/shipped/delivered/returned/cancelled)
- `relationships` cho `silver_order_items.order_id` → `silver_orders` — bắt dòng hàng mồ côi,
  thứ mà bronze không thể thấy vì nó xử lý từng bảng độc lập
- `accepted_range` chặn số âm cho `line_amount`, `revenue`, và chặn `num_orders <= 0`

`order_items` **không** test `unique` trên `order_id`: đó là bảng fact, một đơn có nhiều dòng hàng.

Chỉ chạy test: `make dbt-test`

## Dọn dẹp
```bash
make down        # dừng, giữ dữ liệu
make clean       # dừng + xoá volume MinIO (mất sạch dữ liệu)
```
