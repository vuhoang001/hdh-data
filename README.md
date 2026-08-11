# hdh-data — Lakehouse ELT pipeline

Pipeline ELT học Data Engineering. **MỘT bộ code, MỘT storage, HAI execution engine.**

> Storage, catalog, table format và business logic là MỘT. Chỉ execution engine là hai.

| Môi trường | Engine | Nguồn | Namespace | Khi nào dùng |
| --- | --- | --- | --- | --- |
| **dev** (`ENV=dev`, mặc định) | DuckDB | sample (6 tháng) | `dev_bronze/dev_silver/dev_gold` | Dev nhanh, chạy CI |
| **prod** (`ENV=prod`) | Trino (+ Spark ingest) | full | `bronze/silver/gold` | Giống production |

Cả hai môi trường ghi/đọc **cùng** một storage: bảng **Iceberg** trên **MinIO**, metadata qua
**Iceberg REST catalog** (metastore Postgres). Đổi môi trường bằng một biến — `make ENV=prod` —
và nó đổi engine + đổi nguồn + đổi namespace, **không** đổi một dòng business logic nào.

```text
              data/*.csv  (13 file nguồn, dùng chung)
                    │  load_landing.py  (→ Parquet trên MinIO)
                    ▼
        s3://warehouse/<landing>/…      landing zone (immutable)
                    │  ingest.py  ── engine đổi theo ENV ──┐
                    │     dev: DuckDB   ·   prod: Spark     │  chạy CHÍNH ingestion/bronze_specs/bronze_<t>.sql
                    ▼                                       │
   ┌───────────────────────────────────────────────────────┴───┐
   │            ICEBERG trên MinIO  (MỘT storage)               │
   │     namespace bronze → silver → gold                       │
   │              ▲                    ▲                         │
   │              │        dbt build (1 bộ model, 2 target)      │
   │      Iceberg REST catalog (Postgres metastore)             │
   └──────────────┬────────────────────────────────────────────┘
        dev: DuckDB │ prod: Trino     ← chỉ execution engine là khác
```

Bằng chứng: từ catalog trống, `make ENV=dev pipeline` ingest 13 bảng bronze bằng DuckDB rồi
`dbt build` ra silver/gold — **PASS=209, ERROR=0**. Cùng bộ model đó chạy trên Trino ở prod.

---

## Bắt đầu

**Yêu cầu:** Docker + Docker Compose. Lần đầu chạy sẽ build image dbt/iceberg-rest (và
image Spark ở prod) + tải jar → cần mạng.

```bash
make setup            # tạo config/.env.local, build image, cài dbt_utils
make pipeline         # landing → bronze → silver → gold + test   (ENV=dev mặc định)
make test             # test cấu trúc repo + test dữ liệu
```

Mọi target đều nhận `ENV=`. Chạy production (Trino + Spark) trên cùng máy:

```bash
make ENV=prod up
make ENV=prod pipeline
```

```bash
make                  # danh sách lệnh kèm mô tả
make env-check        # in cấu hình đang có hiệu lực (engine, namespace, bucket…)
make ENV=prod env-check
```

Vòng đời hạ tầng: `make up` · `make down` (giữ dữ liệu) · `make clean` (xoá volume) ·
`make ps` · `make logs`.

### Cổng mở ra host

| Service | Vai trò | Cổng (localhost) |
| --- | --- | --- |
| **MinIO** | Object storage (S3) lưu file Iceberg | 9000 API · 9001 console |
| **iceberg-rest** | REST catalog quản lý metadata Iceberg | 8181 |
| **Trino** *(chỉ prod)* | Engine truy vấn SQL trên Iceberg | 8080 |

MinIO console: <http://localhost:9001> (user/pass trong `config/.env.<env>`), bucket `warehouse`.

---

## Cấu trúc thư mục

Tách theo **tốc độ thay đổi** và **tính di động**, không theo công nghệ:

| Thư mục | Đổi bao lâu một lần | Khi lên production |
| --- | --- | --- |
| `ingestion/` · `dbt/` | hàng ngày — thêm bảng, sửa rule | **giữ nguyên 100%** |
| `infra/images/` | vài tháng — nâng version Spark/dbt | thay bằng EMR, dbt Cloud |
| `infra/` · `config/` | vài tháng — thêm service, đổi endpoint | thay bằng Terraform / k8s / secret manager |

`ingestion` + `dbt` chính là **E-L** và **T** của ELT — tên thư mục nói ra kiến trúc.
Mỗi thư mục có README riêng giải thích quyết định thiết kế bên trong nó.

```text
hdh-data/
├── Makefile                   # điều phối theo ENV: make ENV=dev|prod <target>
├── config/                    # ★ NGUỒN SỰ THẬT cho mọi cấu hình
│   ├── .env.shared            #   version, tên catalog/bucket — giống mọi env
│   ├── .env.dev               #   DuckDB · sample · namespace dev_*
│   ├── .env.prod              #   Trino/Spark · full · namespace bronze/silver/gold
│   └── .env.local.example     #   mẫu cho secret thật + override cá nhân (→ .env.local, gitignore)
├── data/                      # 13 CSV nguồn (dùng chung 2 môi trường)
│
├── infra/                     # tất cả hạ tầng Docker: compose + build image + init
│   ├── compose.base.yml       #   ★ minio · postgres · iceberg-rest — DÙNG CHUNG dev+prod
│   ├── compose.dev.yml        #   + container dbt (DuckDB)
│   ├── compose.prod.yml       #   + trino + spark + dbt
│   ├── images/                #   ★ Dockerfile build image từng engine: dbt · spark · trino
│   ├── minio/init-buckets.sh  #   tạo bucket + prefix landing
│   └── iceberg-rest/          #   Dockerfile catalog
│
├── ingestion/                 # nguồn → landing → bronze Iceberg (E-L)
│   ├── ingest.py              #   ★ điểm vào DUY NHẤT, mọi bảng, mọi engine
│   ├── load_landing.py        #   CSV → Parquet trên landing zone (+ sampling ở dev)
│   ├── config/sources.yml     #   ★ đăng ký 13 bảng — Makefile sinh target từ đây
│   ├── bronze_specs/          #   ★ đặc tả bronze SQL (schema + luật) — engine ingest chạy
│   ├── engines/               #   base · duckdb_engine (dev) · spark_engine (prod)
│   └── common/                #   config · spec · sql_model · io · job · errors · logging
│
├── dbt/                       # dbt project (T): staging → marts
│   ├── profiles.yml           #   2 target: dev (duckdb) + prod (trino), giá trị từ env
│   ├── dbt_project.yml        #   on-run-start tạo namespace silver/gold; severity theo tầng
│   ├── models/staging/        #   tầng SILVER: làm sạch, lọc _is_valid
│   ├── models/marts/          #   tầng GOLD: star schema (dim/fact) + tổng hợp
│   └── macros/                #   ensure_layer_schemas · duckdb_iceberg_materializations · portable_dates
│
├── tests/unit/                # test cấu trúc repo, chạy không cần Docker
├── docs/                      # thiết kế star schema, mô hình dữ liệu, kiến trúc, hướng dẫn
└── .github/workflows/ci.yml   # CI: chạy trọn pipeline dev + test trên mỗi PR
```

**Vì sao bronze SQL nằm ở `ingestion/bronze_specs/` chứ không phải `dbt/models/`:** chúng
không còn là dbt model. Cùng file `.sql` đó được **hai** engine ingestion chạy (DuckDB ở dev,
Spark ở prod) để ghi bronze vào Iceberg; dbt chỉ **đọc** bronze qua `source()`. Để chúng trong
`models/` sẽ khiến dbt cố build lại thứ mà tầng ingestion vừa ghi.

### Cấu hình quản lý ở đâu

Nguyên tắc: **`config/.env.*` giữ GIÁ TRỊ, mỗi thành phần giữ FILE cấu hình của riêng nó.**
Không endpoint, bucket, hay version nào bị hardcode hai lần. Thứ tự nạp:
`.env.shared` → `.env.<env>` → `.env.local` (gitignore, thắng sau cùng).

| File | Nội suy bằng | Ai đọc |
| --- | --- | --- |
| `infra/compose.*.yml` | `${VAR}` | docker compose |
| `infra/images/trino-runner/catalog/iceberg.properties` | `${ENV:VAR}` | Trino |
| `infra/images/spark-runner/spark-defaults.conf.tmpl` | `${VAR}` | entrypoint render lúc khởi động |
| `dbt/profiles.yml` · `dbt_project.yml` | `{{ env_var('VAR') }}` | dbt |
| `ingestion/common/config.py` | `os.environ` | code DuckDB/Spark |
| `Makefile` | `-include config/.env.*` | make |

Đổi `WAREHOUSE_BUCKET` một chỗ là Spark, Trino, Iceberg REST và MinIO cùng đổi. Secret ở
production **không** sửa `.env.prod` — inject qua secret manager (mọi biến đọc qua `os.environ`
/ `${VAR}` nên inject kiểu gì cũng chạy).

---

## Một dbt project, hai engine — không nhân đôi logic

Business logic viết **một lần**, chạy đúng ở cả hai engine. Ba lớp adapter mỏng che phần khác biệt:

- **bronze là `source()` ở mọi target** ([models/_sources.yml](dbt/models/_sources.yml)) —
  bronze do tầng ingestion ghi vào Iceberg ở **cả hai** môi trường, nên dbt luôn đọc qua
  `source('bronze', …)`. Đồ thị model giống hệt nhau ở dev và prod (không còn `enabled` theo target).
- **[macros/portable_dates.sql](dbt/macros/portable_dates.sql)** — gom vài hàm lệch nhau
  giữa Trino và DuckDB (`date_format`↔`strftime`, `day_of_week`↔`isodow`, `week_of_year`↔`week`).
- **[macros/duckdb_iceberg_materializations.sql](dbt/macros/duckdb_iceberg_materializations.sql)** —
  override materialization `table` cho DuckDB thành DROP+CREATE (DuckDB-Iceberg chưa hỗ trợ
  `CREATE OR REPLACE` và rename-trong-transaction). Chỉ áp ở dev; prod dùng bản mặc định của Trino.
- **[macros/ensure_layer_schemas.sql](dbt/macros/ensure_layer_schemas.sql)** — `on-run-start`
  tạo sẵn namespace silver/gold (dbt tự tạo schema không ổn định trên catalog Iceberg REST).

Chi tiết: [dbt/README.md](dbt/README.md).

## Layer dữ liệu

Mỗi layer một việc: bronze **mô tả** nguồn, silver **quyết định** dữ liệu nào dùng được,
gold **trả lời** câu hỏi business.

- **bronze** — chuẩn hoá text, gắn cờ `_is_valid` + `_invalid_reason` + cột audit
  (`_source_file`, `_ingested_at`); giữ nguyên số dòng nguồn. **Một bản logic duy nhất** ở
  `ingestion/bronze_specs/bronze_<t>.sql` — DuckDB chạy nó ở dev, Spark chạy chính nó ở prod.
  13 bảng: `orders`, `order_items`, `payments`, `shipments`, `reviews`, `returns`, `inventory`,
  `customers`, `geography`, `products`, `promotions`, `sales_daily`, `web_traffic`.

  > **`sales_daily` không khớp doanh thu tính từ `order_items`** — hai nguồn số độc lập;
  > chênh lệch là thứ cần điều tra, đừng "sửa" cho khớp.
- **silver** — lọc theo `_is_valid`, bỏ cột kỹ thuật, thêm cột dẫn xuất. Vật liệu hoá thành
  **table** ở cả hai env (DuckDB-Iceberg chưa hỗ trợ `CREATE VIEW`): `silver_orders`,
  `silver_order_items`, `silver_customers`, `silver_products`, `silver_geography`, `silver_promotions`.
- **gold** — star schema, **table**: `dim_customer`, `dim_product`, `dim_promotion`, `dim_date`,
  `fact_order_items`, và 2 bảng tổng hợp `gold_orders_daily`, `gold_revenue_daily`.

## Pipeline chạy từng bước

```bash
make landing          # B1. data/*.csv → Parquet trên landing zone (MinIO)
make ingest           # B2. landing → 13 bảng bronze Iceberg (engine theo ENV)
                      #     hoặc từng bảng: make ingest-orders
make ingest-list      #     liệt kê các bảng đã khai
make dbt-build        # B3. dbt build silver + gold + test (--target theo ENV)
make dbt-unit         #     chỉ unit test (nhanh, không cần dữ liệu)
make query            # xem thử một bảng gold
```

`make pipeline` = `landing` → `ingest` → `dbt-build`. Đổi engine chỉ bằng `ENV=prod`.

## Test

Hai lớp, hai chỗ, kiểm hai thứ khác nhau:

```bash
make test-repo        # cấu trúc repo — pytest tests/unit, vài giây, không cần Docker
make dbt-test         # dữ liệu — chạy qua engine của ENV đang chọn
make test             # cả hai
make ci-local         # đúng chuỗi mà CI chạy (dev)
```

- **`tests/unit/`** kiểm *repo có nhất quán không*: khai bảng trong `sources.yml` mà quên tạo
  model SQL (và ngược lại), model thiếu cột audit / luật chất lượng, `file:` lệch `source_file`,
  `partition_by` sai cú pháp, khai `type:` chưa có reader trong `io.py`.
- **Test dữ liệu nằm trong dbt**, cạnh model, ở các file `_*.yml`. `dbt build` chạy test ngay sau
  mỗi model nên silver fail thì gold không build từ dữ liệu hỏng: `not_null`/`unique`,
  `accepted_values`, `relationships`, `accepted_range`, và
  [test hạt fact](dbt/tests/assert_fact_order_items_grain.sql).

> **Hai thư mục tên "tests", hai nghĩa — đừng nhầm:** `tests/` ở gốc là test **cấu trúc repo**
> (pytest); `dbt/tests/` là **singular data test** của dbt (SQL, chạy trong `dbt build`).

## Local đọc CSV, production đọc Postgres — đổi ở đâu?

Đúng một chỗ: `SOURCE_READERS` trong `ingestion/common/io.py`, cộng với `type:` trong
`sources.yml`. Model SQL, silver, gold và test dbt không đụng tới.
Chi tiết: [ingestion/README.md](ingestion/README.md).

## Thêm một bảng mới

1. Khai báo trong [`ingestion/config/sources.yml`](ingestion/config/sources.yml)
2. Tạo `ingestion/bronze_specs/bronze_<bảng>.sql` — **một file, hai engine cùng chạy**
3. `make test-repo` — bắt ngay nếu hai bước trên lệch nhau
4. `make ingest-<bảng>` rồi `make dbt-build`

Không phải sửa Makefile: danh sách target sinh ra từ `sources.yml`.
Hướng dẫn đầy đủ tới tận gold: [docs/them-bang-moi.md](docs/them-bang-moi.md).

## Tài liệu

| Tài liệu | Nội dung |
| --- | --- |
| [docs/CURRENT_ARCHITECTURE.md](docs/CURRENT_ARCHITECTURE.md) · [TARGET](docs/TARGET_ARCHITECTURE.md) · [MIGRATION](docs/MIGRATION_PLAN.md) | Audit kiến trúc cũ → thiết kế mục tiêu → kế hoạch di trú (bản ghi thiết kế) |
| [docs/them-bang-moi.md](docs/them-bang-moi.md) | Hướng dẫn từng bước từ CSV tới gold |
| [docs/mo-hinh-du-lieu.md](docs/mo-hinh-du-lieu.md) | Sơ đồ quan hệ 13 bảng, công thức join. **Đọc trước khi viết query join.** |
| [docs/star-schema.md](docs/star-schema.md) · [star-schema-ly-thuyet.md](docs/star-schema-ly-thuyet.md) | Thiết kế dim/fact ở gold, lý thuyết Kimball |
| [ingestion/README.md](ingestion/README.md) | Hợp đồng bronze layer, cách viết connector, hai engine ingestion |
| [dbt/README.md](dbt/README.md) | Macro tương thích 2 engine, materialization theo layer |
| [infra/images/README.md](infra/images/README.md) | Vì sao Spark config phải render ở entrypoint |
| [infra/README.md](infra/README.md) | Stack dùng chung + overlay theo env, và điều phải sửa khi lên production |
