# CURRENT_ARCHITECTURE — Kiến trúc hiện tại (Phase 0: Audit)

> Tài liệu này **mô tả**, không đề xuất. Phần đề xuất nằm ở
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) và [MIGRATION_PLAN.md](MIGRATION_PLAN.md).
>
> Mọi kết luận dưới đây được đối chiếu với code thật. Các phát hiện đánh dấu
> **[ĐÃ CHẠY THẬT]** là kết quả spike thực nghiệm, không phải suy luận từ tài liệu.

---

## 1. Sơ đồ kiến trúc hiện tại

Điểm mấu chốt: đây **không phải hai engine trên cùng một storage**. Đây là **hai
pipeline có storage khác nhau**, chỉ dùng chung phần logic SQL.

```text
                        data/*.csv  (13 file, bind mount tu HOST)
                                    │
                ┌───────────────────┴───────────────────┐
                │                                       │
                ▼                                       ▼
   ┌────────────────────────┐            ┌──────────────────────────────────┐
   │  MOI TRUONG 1: DuckDB  │            │  MOI TRUONG 2: Lakehouse         │
   │  compose.duckdb.yml    │            │  compose.lakehouse.yml           │
   ├────────────────────────┤            ├──────────────────────────────────┤
   │ dbt-duckdb             │            │ Spark ─ingest.py─→ Iceberg bronze│
   │  bronze = dbt MODEL    │            │   (bronze = SOURCE, khong phai   │
   │    doc read_csv()      │            │    dbt model)                    │
   │  → silver (view)       │            │ dbt ─qua Trino─→ silver → gold   │
   │  → gold (table)        │            │                                  │
   └───────────┬────────────┘            └──────────────┬───────────────────┘
               ▼                                        ▼
    /warehouse/hdh.duckdb                    s3://warehouse/  (MinIO)
    (named volume, FILE LOCAL)               Iceberg REST catalog + Postgres
               │                                        │
        ✗ KHONG dung MinIO                    ✓ Object storage
        ✗ KHONG dung Iceberg                  ✓ Iceberg table format
```

**Hệ quả trực tiếp:** dữ liệu dev và dữ liệu prod không bao giờ là cùng một thứ, không
thể so sánh, không thể chuyển qua lại. Đây là vấn đề gốc mà refactor này phải giải.

---

## 2. Trả lời 17 câu hỏi audit

### 2.1 Source hiện tại có những module nào?

| Module | Vai trò | Đánh giá |
| --- | --- | --- |
| `ingestion/` | CSV → Iceberg bronze (chỉ lakehouse) | **Tốt, giữ lại** — thiết kế đã gần đúng |
| `dbt/` | dbt project: bronze/silver/gold | **Tốt, giữ lại** — refactor nhẹ |
| `engine-runners/` | Dockerfile + config từng engine | Giữ, bổ sung |
| `infra/local/` | 2 docker compose stack | Hợp nhất lại |
| `tests/` | pytest kiểm cấu trúc repo | **Tốt, giữ lại** |
| `docs/` | 5 tài liệu thiết kế | Giữ, bổ sung |
| `data/` | 13 CSV nguồn + `sample_submission.csv` | Chuyển thành landing zone |
| `Makefile` | Điều phối, ~30 target | Tái cấu trúc theo môi trường |

### 2.2 Ingestion đang làm gì?

`ingestion/ingest.py` là điểm vào duy nhất: `spark-submit ingest.py --table orders`.

Luồng trong `common/job.py::run()`:
```text
spec.load(table)          đọc sources.yml   → type, partition_by, extra_metrics
sql_model.load(table)     đọc bronze_*.sql  → source_file, columns, template SQL
build_spark_session()
read_source(...)          → createOrReplaceTempView("bronze_source_input")
spark.sql(model.render()) ← CHAY CHINH DOAN SQL MA dbt BUILD
write_iceberg_table(...)  → createOrReplace (GHI DE TOAN BO)
count + log
```

**Đây là phần thiết kế tốt nhất của repo.** Nó đã tách "render SQL" khỏi "thực thi SQL".
Chỉ có bước thực thi là gắn chặt Spark.

### 2.3 Transformation đang làm gì?

dbt project `dbt/` (profile `hdh_dbt`), 3 tầng:

| Tầng | Số model | Materialization | Ghi chú |
| --- | --- | --- | --- |
| bronze | 13 | `table`, `+enabled: target.type=='duckdb'` | **Chỉ tồn tại ở DuckDB** |
| silver | 6 | `view` | `orders, order_items, customers, products, geography, promotions` |
| gold | 7 | `table` | 4 dim + 1 fact + 2 aggregate |

7/13 bảng bronze (`payments, shipments, reviews, returns, inventory, sales_daily,
web_traffic`) **dừng lại ở bronze**, không có silver/gold.

### 2.4 Storage đang làm gì?

| Môi trường | Nơi lưu | Format |
| --- | --- | --- |
| DuckDB | named volume `duckdb-warehouse` → `/warehouse/hdh.duckdb` | file DuckDB |
| Lakehouse | MinIO bucket `warehouse` | Iceberg (format-version 2) |
| Nguồn | bind mount `./data` từ host | CSV |

**Không có landing zone trên object storage.** Nguồn là filesystem host ở cả hai môi trường.

### 2.5 Metadata / catalog đang làm gì?

- Iceberg REST catalog (`hdh-iceberg-rest`, image tự build), metastore là **Postgres**
  (`CATALOG_URI: jdbc:postgresql://...`). Compose ghi rõ lý do không dùng SQLite mặc định:
  SQLite in-memory mất bảng khi pool đóng connection.
- `CATALOG_WAREHOUSE: s3://${WAREHOUSE_BUCKET}/`, `CATALOG_IO__IMPL: S3FileIO`.
- **Môi trường DuckDB không có catalog nào** — không namespace, không snapshot, không metadata.

### 2.6–2.10 Iceberg / MinIO / DuckDB / Trino / dbt dùng ở đâu

| Công nghệ | Dùng ở đâu | Chỉ ở môi trường nào |
| --- | --- | --- |
| **Iceberg** | `common/iceberg.py`, `spec.partition_columns()`, `_sources.yml`, `trino-runner/catalog/iceberg.properties` | **Chỉ lakehouse** |
| **MinIO** | `compose.lakehouse.yml`, `minio/init-buckets.sh`, `spark-defaults.conf.tmpl` | **Chỉ lakehouse** |
| **DuckDB** | `profiles.yml` target `duckdb`, `macros/bronze_helpers.sql` (read_csv), Makefile `duckdb-*` | **Chỉ dev** |
| **Trino** | `profiles.yml` target `trino`, `trino-runner/catalog/`, Makefile `lake-*` | **Chỉ lakehouse** |
| **dbt** | `dbt/` — **DÙNG CHUNG CẢ HAI** (2 target, 1 bộ model) | cả hai ✓ |

### 2.11 Business logic nằm trong Python không?

**Gần như không — và đây là điểm mạnh lớn nhất của repo.**

`ingestion/common/` khai báo rõ trong docstring `__init__.py`: *"Package này CHỈ chứa phần
kỹ thuật... KHÔNG chứa business logic của bất kỳ bảng nào."* Kiểm chứng lại: đúng.

Trước đây bronze có **hai bản** (13 file PySpark + 13 file SQL). Commit `92f31ca` đã gộp
về một bản: Spark đọc chính file `.sql` mà dbt build. Đây là refactor đúng hướng và **phải
được bảo toàn**.

Ngoại lệ duy nhất: `sources.yml` chứa `extra_metrics` (`"ngày bán lỗ": "_margin_negative"`)
— là điều kiện SQL nằm trong file cấu hình. Nhỏ, chấp nhận được.

### 2.12 Business logic nằm trong SQL không?

**Có — đúng chỗ.** Toàn bộ: schema, chuẩn hoá, luật chất lượng, cột dẫn xuất, star schema.

Điểm cần biết: `ingestion/bronze_specs/bronze_*.sql` **bị ràng buộc phải là SQL portable**
(chạy giống nhau trên DuckDB + Spark SQL). `macros/bronze_helpers.sql` ghi rõ ràng buộc này
và liệt kê các hàm đã dùng đạt yêu cầu.

### 2.13 Business logic nằm trong orchestration không?

**Có, một ít trong Makefile:**
- `BRONZE_TABLES := $(shell sed -n 's/^...table:...//p' sources.yml)` — parse YAML bằng `sed`.
  Fragile (phụ thuộc thụt lề) nhưng đúng nguyên tắc không lặp danh sách bảng.
- Thứ tự pipeline (`ingest` → `dbt`) chỉ tồn tại dưới dạng thứ tự target trong Makefile và
  hướng dẫn README. Không có định nghĩa pipeline nào máy đọc được.

### 2.14 Có hard-code environment không?

**Cấu hình thì không — đây là điểm mạnh.** `.env` là nguồn sự thật duy nhất, nội suy qua
6 cơ chế khác nhau (compose `${VAR}`, Trino `${ENV:VAR}`, Spark template render lúc
entrypoint, dbt `env_var()`, Python `os.environ`, make `-include`).

**Nhưng logic thì CÓ** — và đây là vi phạm nguyên tắc §4 của spec:

| Vị trí | Nội dung | Vấn đề |
| --- | --- | --- |
| `macros/bronze_ref.sql` | `{% if target.type == 'duckdb' %} ref() {% else %} source() {% endif %}` | Rẽ nhánh môi trường **trong business logic** |
| `dbt_project.yml` | `bronze: +enabled: "{{ target.type == 'duckdb' }}"` | Cả một tầng chỉ tồn tại ở 1 env |
| `models/staging/_sources.yml` | `enabled: "{{ target.type != 'duckdb' }}"` | Nghịch đảo của trên |
| `profiles.yml` | fallback `'hdh_local.duckdb'`, `'../data'` | Hardcode đường dẫn host (có comment giải thích) |
| `compose.lakehouse.yml` | `TRINO_PORT: 8080` | Hardcode (có comment: cổng trong network) |

### 2.15 Có secret/credential hard-code không?

**Không có secret thật bị commit.** Kiểm chứng:
- `.gitignore` có `.env` và `.env.*` với ngoại lệ `!.env.example` — đúng.
- `.env.example` dùng placeholder `change-me` cho mọi mật khẩu.
- Không file nào khác chứa credential.

**Rủi ro còn lại:** `.env` thật trên máy dev đang dùng nguyên giá trị `change-me`
(`AWS_SECRET_ACCESS_KEY=change-me`, `CATALOG_DB_PASSWORD=change-me`). Chấp nhận được
cho local, nhưng khi lên production cần secret manager thật chứ không phải file `.env`.

### 2.16 Có code chỉ chạy được ở DEV hoặc PROD không?

**Có, khá nhiều** — đây là danh sách phải xoá/hợp nhất:

| Thành phần | Chỉ chạy ở | Ghi chú |
| --- | --- | --- |
| 13 model `ingestion/bronze_specs/*.sql` (khi dbt build) | DEV | `+enabled` tắt ở trino |
| `macros/bronze_helpers.sql` → `read_csv()` | DEV | Cú pháp riêng DuckDB |
| `source('bronze', ...)` trong `_sources.yml` | PROD | Tắt ở duckdb |
| `ingestion/` toàn bộ | PROD | Cần Spark — **DEV KHÔNG CÓ ĐƯỜNG NÀO GHI BRONZE** |
| `make lake-freshness` | PROD | Source freshness cần source, dev không có source |
| `compose.duckdb.yml` | DEV | |
| ~9 target `duckdb-*`, ~14 target `lake-*` | tương ứng | |

**Khoảng trống nghiêm trọng nhất:** DEV không có ingestion. Nếu bỏ đường `bronze = dbt
model đọc CSV`, DEV mất hoàn toàn khả năng tạo bronze.

### 2.17 Có dependency chỉ phục vụ một environment không?

| Dependency | Phục vụ | Ghi chú |
| --- | --- | --- |
| `dbt-duckdb==1.10.1` | DEV | Cùng image với prod (image có sẵn cả 2 adapter) |
| `dbt-trino==1.10.3` | PROD | |
| `pyspark` (image `apache/spark:3.5.6-python3`) | PROD | ~1.79GB |
| jar Iceberg-Spark + Postgres JDBC | PROD | Tải lúc build image |
| `trinodb/trino:latest` | PROD | **Không pin version** — xem mục 4 |
| `pyyaml` | Cả hai | |

`common/spec.py` và `common/sql_model.py` **cố ý không import pyspark ở cấp module** để
`tests/` chạy được mà không cần cài Spark 300MB. Đây là thiết kế tốt, phải giữ.

---

## 3. Kết quả spike thực nghiệm **[ĐÃ CHẠY THẬT]**

Tôi đã dựng MinIO + Postgres + Iceberg REST (không Trino, không Spark) và chạy DuckDB
1.5.5 trong network của stack. Đây là các ràng buộc **cứng** mà thiết kế phải tuân theo —
không phải phỏng đoán từ tài liệu.

### 3.1 Những thứ CHẠY ĐƯỢC ✓

| Kiểm tra | Kết quả |
| --- | --- |
| `ATTACH` Iceberg REST catalog từ DuckDB | ✓ (xem cú pháp chính xác bên dưới) |
| `CREATE SCHEMA` / `CREATE TABLE AS SELECT` / `INSERT` | ✓ |
| DuckDB **đọc bảng do Spark ghi** | ✓ `bronze.orders` = **646.945 dòng** (khớp README) |
| DuckDB **đọc bảng do Trino ghi** | ✓ `analytics.gold_revenue_daily` |
| dbt-duckdb ATTACH + đọc `ice.bronze.orders` | ✓ |
| `duckdb` trong image `hdh-dbt:1.12.0` sẵn có | ✓ **1.5.5** — không cần rebuild vì version |

Cú pháp ATTACH đã kiểm chứng (`AUTHORIZATION_TYPE 'none'` là **bắt buộc** vì
`iceberg-rest` chạy không auth; thiếu nó thì DuckDB mặc định `oauth2` và báo lỗi):

```sql
CREATE SECRET minio_s3 (TYPE S3, KEY_ID '...', SECRET '...',
    ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');

ATTACH 'warehouse' AS ice (
    TYPE ICEBERG,
    ENDPOINT 'http://iceberg-rest:8181',
    AUTHORIZATION_TYPE 'none'
);
```

> **Lưu ý vận hành:** REST catalog trả về đường dẫn trỏ `minio:9000` (tên nội bộ), nên
> tiến trình DuckDB phải nằm **trong** network của stack, hoặc `minio` phải phân giải
> được từ nơi chạy. Đây là lý do "DuckDB chạy native trên laptop" (spec §21) cần thêm
> một bước: hoặc map hostname, hoặc expose MinIO ra host và đổi endpoint theo môi trường.

### 3.2 Những thứ KHÔNG chạy được ✗ — ràng buộc bắt buộc phải thiết kế quanh

| Hạn chế | Thông báo lỗi thật | Ảnh hưởng |
| --- | --- | --- |
| **`CREATE VIEW` không hỗ trợ** | `Not implemented Error: Create View` | ⚠️ **Toàn bộ tầng silver đang là `view`** |
| **`CREATE OR REPLACE TABLE` không hỗ trợ** | `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements` | Mọi thao tác ghi đè phải DROP + CREATE |
| **RENAME trong transaction** | `Catalog Error: This table (x) was modified already, can't be renamed!` | ⚠️ **Materialization `table` mặc định của dbt fail** |

### 3.3 Chẩn đoán chi tiết lỗi materialization

dbt build model `table` theo trình tự: tạo `model__dbt_tmp` → `ALTER ... RENAME TO model`.
Kết quả `dbt run` thật:

```text
1 of 1 ERROR creating sql table model dbt_spike.spike_status_count ... [ERROR in 0.14s]
  Catalog Error: This table (spike_status_count__dbt_tmp) was modified already, can't be renamed!
```

Tôi đã cô lập nguyên nhân bằng ba phép thử:

| Phép thử | Kết quả |
| --- | --- |
| `BEGIN; CTAS; RENAME; COMMIT` (đúng cách dbt làm) | ✗ **fail** |
| `CTAS; RENAME` ở chế độ autocommit | ✓ ok |
| `DROP; CREATE; DROP; CREATE` lặp lại | ✓ ok |

→ **Nguyên nhân là transaction**, không phải bản thân RENAME. Fix nằm ở tầng project:
adapter dùng các macro chuẩn `relations/{create_intermediate, rename_intermediate,
replace, drop}` — đều override được bằng macro trong `dbt/macros/`. Không phải
fork adapter, không phải đổi adapter.

---

## 4. Danh sách vấn đề kiến trúc (Output B)

Xếp theo mức độ nghiêm trọng.

### P0 — Chặn mục tiêu dev/prod đồng bộ

| # | Vấn đề | Bằng chứng |
| --- | --- | --- |
| **P0-1** | **DEV không dùng MinIO và không dùng Iceberg.** Đây là vi phạm trực tiếp spec §10, §11 | `compose.duckdb.yml` chỉ có 1 service, volume `duckdb-warehouse` |
| **P0-2** | **Nguồn nằm trên local filesystem**, không có landing zone object storage | `./data:${SPARK_DATA_DIR}:ro` |
| **P0-3** | **DEV không có ingestion.** Bỏ đường "bronze = dbt model đọc CSV" là DEV mất khả năng tạo bronze | `ingestion/` yêu cầu `spark-submit` |
| **P0-4** | **Rẽ nhánh môi trường nằm trong business logic** | `bronze_ref.sql`, `+enabled` ở `dbt_project.yml` và `_sources.yml` |

### P1 — Ảnh hưởng tới correctness / vận hành

| # | Vấn đề | Bằng chứng |
| --- | --- | --- |
| **P1-1** | **Không có incremental / backfill / watermark.** Mọi lần ingest ghi đè toàn bộ | `write_iceberg_table` → `.createOrReplace()` |
| **P1-2** | **CI chỉ kiểm 1 engine.** Không có gì đảm bảo DuckDB và Trino cho cùng kết quả | `ci.yml` chỉ chạy `make ci-local` (DuckDB) |
| **P1-3** | **Không có data contract.** PK/owner/freshness rải rác trong `_*.yml`, `partition_by` ở `sources.yml`, không có nơi thống nhất | — |
| **P1-4** | **Observability tối thiểu.** Chỉ `logger.info` đếm dòng, không structured, không lưu lại | `common/job.py` |
| **P1-5** | **silver và gold chung một namespace** `analytics`, trái với yêu cầu tách bronze/silver/gold | `ANALYTICS_SCHEMA=analytics` |
| **P1-6** | **`trinodb/trino:latest` không pin version** — trái chính nguyên tắc pin `==` mà `dbt-runner/Dockerfile` tự đặt ra | `compose.lakehouse.yml` |

### P2 — Nợ kỹ thuật, không chặn

| # | Vấn đề |
| --- | --- |
| **P2-1** | Bảng map kiểu tồn tại 2 nơi, phải sửa thủ công cả hai (`bronze_helpers.sql` ↔ `sql_model.py`) — chính comment trong code cảnh báo điều này |
| **P2-2** | Makefile parse YAML bằng `sed`, vỡ nếu đổi thụt lề |
| **P2-3** | Không có orchestration thật (không lịch, không retry từng bảng, không backfill) |
| **P2-4** | 7/13 bảng bronze không có silver/gold |
| **P2-5** | `dim_customer` là SCD Type 1, không có `snapshots/` |
| **P2-6** | Không có cơ chế offline install (spec §22) |
| **P2-7** | Chưa có CLI (spec §20); mọi thứ qua Makefile |

---

## 5. Những phần TỐT — phải giữ, không được rewrite

Đây là danh sách bảo vệ. Refactor không được làm hỏng các điểm sau:

1. **Một bản logic bronze duy nhất** (`bronze_*.sql` chạy bởi cả dbt lẫn Spark).
   Đây là thành quả của commit `92f31ca`, đã xoá được cả một lớp test canh lệch.
2. **`sql_model.py` tách "render" khỏi "execute"** — chính vì vậy việc thêm engine thứ ba
   (DuckDB) là *thêm một executor*, không phải viết lại renderer.
3. **`spec.py` / `sql_model.py` không import pyspark ở cấp module** → test chạy nhẹ.
4. **`.env` là nguồn sự thật duy nhất**, không hardcode bucket/endpoint ở bất kỳ đâu.
5. **`sources.yml` là registry** — Makefile sinh target từ đó, không lặp danh sách bảng.
6. **`tests/test_bronze_models.py`** — kiểm khuôn khai báo bằng chính code mà Spark chạy.
7. **Severity test theo tầng** (`bronze: warn`, `silver/gold: error`) đặt tập trung ở
   `dbt_project.yml` kèm lý do.
8. **dbt unit tests** (`_unit_tests.yml`) — kiểm công thức bằng dữ liệu bịa, 2 giây.
9. **Chất lượng tài liệu.** Mỗi thư mục có README giải thích *quyết định thiết kế*, không
   chỉ mô tả. Phải duy trì chuẩn này.
10. **Container chạy bằng UID host** (`DOCKER_USER`), tránh file root trên bind mount.

---

## 6. Kiểm kê file hiện tại

```text
ingestion/
  ingest.py                    CLI entrypoint (argparse: --table, --list)
  config/sources.yml           registry 13 bảng: table/file/type/partition_by/extra_metrics
  common/
    __init__.py                docstring: KHÔNG re-export (tránh kéo pyspark)
    config.py                  đọc env → CATALOG, BRONZE_SCHEMA, DATA_DIR, MODELS_DIR
    spec.py                    parse sources.yml → SourceSpec + parse partition_by
    sql_model.py               parse bronze_*.sql → BronzeModel (schema + render)
    io.py                      SOURCE_READERS = {"csv": read_csv}  ← SEAM nguồn
    job.py                     khung chạy chung  ← GẮN CHẶT SPARK
    session.py                 SparkSession + logger
    iceberg.py                 create_namespace / write_iceberg_table / count

dbt/
  dbt_project.yml              vars.data_dir, materialization + severity theo tầng
  profiles.yml                 2 target: duckdb | trino
  macros/
    bronze_helpers.sql         bronze_source() → read_csv()      [DuckDB]
    bronze_ref.sql             bronze() → ref() hoặc source()    [rẽ nhánh env]
    portable_dates.sql         date_format↔strftime, day_of_week↔isodow, ...
    generate_schema_name.sql   /  generate_alias_name.sql
  ingestion/bronze_specs/               13 .sql + _bronze.yml
  models/staging/               6 .sql + 6 .yml + _sources.yml (freshness)
  models/marts/                 7 .sql + 6 .yml + _unit_tests.yml
  tests/                       5 singular test .sql
  tests/generic/               not_future_date.sql, sum_equals.sql

engine-runners/
  spark-runner/                Dockerfile, entrypoint.sh, spark-defaults.conf.tmpl
  dbt-runner/Dockerfile        1 image, 2 adapter, pin ==
  trino-runner/catalog/        iceberg.properties (${ENV:VAR})

infra/local/
  compose.duckdb.yml           1 service
  compose.lakehouse.yml        6 service
  iceberg-rest/Dockerfile  ·  minio/init-buckets.sh

tests/test_bronze_models.py    kiểm khuôn bronze, không cần Docker
.github/workflows/ci.yml       job duckdb-pipeline + job lint
Makefile                       ~30 target
data/                          13 CSV nguồn + sample_submission.csv
docs/                          them-bang-moi · star-schema · star-schema-ly-thuyet
                               mo-hinh-du-lieu · README
```

---

## 7. Kết luận audit

Repo này **không phải một mớ hỗn độn cần viết lại**. Nó là một thiết kế tốt đã đi được
80% quãng đường tới mục tiêu, và dừng lại ở đúng một chỗ:

> DuckDB được chọn làm *môi trường nhẹ* thay vì làm *engine nhẹ trên cùng storage*.

Mọi vấn đề P0 đều bắt nguồn từ quyết định đó. Spike đã chứng minh quyết định đó **có thể
đảo ngược được** — DuckDB đọc/ghi Iceberg trên MinIO thật, và đọc được đúng dữ liệu Spark
đã ghi.

Do đó migration là **refactor có trọng điểm**, không phải rewrite. Chi tiết:
[TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) → [MIGRATION_PLAN.md](MIGRATION_PLAN.md).
