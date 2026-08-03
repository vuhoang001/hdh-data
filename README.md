# hdh-data — Lakehouse ELT pipeline

Pipeline ELT học Data Engineering, chạy được trên **hai môi trường độc lập** với **cùng một
bộ model dbt**:

| Môi trường | Engine | Khi nào dùng | Cần gì | Thời gian |
| --- | --- | --- | --- | --- |
| **DuckDB** (nhẹ) | dbt-duckdb | Dev nhanh, chạy CI | Chỉ Docker | ~vài giây |
| **Lakehouse** | Spark + Trino + Iceberg + MinIO | Giống production, dữ liệu trên object storage | Docker + mạng (tải jar) | ~vài phút |

Cùng logic bronze → silver → gold, chỉ khác engine thực thi. Chọn môi trường bằng tiền tố
lệnh `make`: `duckdb-*` hoặc `lake-*`.

```text
                 data/*.csv  (13 file nguồn, dùng chung)
                      │
        ┌─────────────┴──────────────┐
        ▼                            ▼
  ┌───────────────┐            ┌──────────────────────────────────────┐
  │  MÔI TRƯỜNG 1 │            │           MÔI TRƯỜNG 2               │
  │   DuckDB      │            │              Lakehouse               │
  │               │            │                                      │
  │ dbt-duckdb:   │            │ Spark ─ingest→ Iceberg(bronze)       │
  │  bronze(CSV)  │            │           trên MinIO (S3)            │
  │   → silver    │            │      ↑ metadata: Iceberg REST catalog│
  │   → gold      │            │ dbt ─(qua Trino)→ silver → gold      │
  └───────┬───────┘            └──────────────────┬───────────────────┘
          ▼                                       ▼
   file hdh.duckdb                      Trino SQL trên Iceberg
```

---

## Cấu trúc thư mục

Tách theo **tốc độ thay đổi** và **tính di động**, không theo công nghệ:

| Thư mục | Đổi bao lâu một lần | Khi lên production |
| --- | --- | --- |
| `ingestion/` · `transforms/` | hàng ngày — thêm bảng, sửa rule | **giữ nguyên 100%** |
| `engine-runners/` | vài tháng — nâng version Spark/dbt | thay bằng EMR, dbt Cloud |
| `infra/` | vài tháng — thêm service | thay bằng Terraform / k8s |

`ingestion` + `transforms` chính là **E-L** và **T** của ELT — tên thư mục nói ra kiến trúc.

Đây cũng là lý do **dbt nằm ở hai chỗ có chủ đích**: `transforms/` là *code* (sửa mỗi ngày),
`engine-runners/dbt-runner/` là *runtime image* (gần như không đụng). Gộp lại nghĩa là mỗi
lần thêm một model SQL lại phải nhìn thấy Dockerfile.

Mỗi thư mục có README riêng giải thích quyết định thiết kế bên trong nó.

```text
hdh-data/
├── .env / .env.example        # ★ NGUỒN SỰ THẬT DUY NHẤT cho mọi cấu hình
├── Makefile                   # điều phối: duckdb-* và lake-*
├── data/                      # 13 CSV nguồn (dùng chung 2 môi trường)
│
├── infra/local/               # docker compose 2 stack + init MinIO + Iceberg REST
│
├── ingestion/                 # CSV → bronze Iceberg (chỉ môi trường lakehouse)
│   ├── config/sources.yml     #   ★ đăng ký 13 bảng — Makefile sinh target từ đây
│   ├── common/                #   hạ tầng dùng chung: config, khung job, IO, Iceberg
│   └── connectors/            #   13 file, mỗi file = business logic của 1 bảng
│
├── transforms/                # dbt project: bronze → silver → gold
│   ├── profiles.yml           #   2 target: duckdb + trino, giá trị từ .env
│   └── models/{bronze,silver,gold}/
│
├── engine-runners/            # image + cấu hình của từng engine
│   ├── spark-runner/          #   Dockerfile · entrypoint · spark-defaults.conf.tmpl
│   ├── dbt-runner/            #   1 image, 2 adapter (duckdb + trino)
│   └── trino-runner/catalog/  #   catalog Iceberg cho Trino
│
├── tests/                     # test cấu trúc repo, chạy không cần Docker
├── docs/                      # thiết kế star schema, mô hình dữ liệu, hướng dẫn
└── .github/workflows/ci.yml   # CI: chạy pipeline DuckDB + test trên mỗi PR
```

Điều phối hiện do `Makefile` đảm nhiệm. Khi cần chạy theo lịch, retry riêng một bảng, hay
backfill thì mới cần tới Airflow/Dagster — lúc đó DAG nên **gọi lại đúng các target Makefile
đang có** và lấy danh sách bảng từ `ingestion/config/sources.yml`, để chạy tay và chạy theo
lịch không bao giờ lệch nhau.

### Cấu hình quản lý ở đâu

Nguyên tắc: **`.env` giữ GIÁ TRỊ, mỗi thành phần giữ FILE cấu hình của riêng nó.**
Không endpoint, bucket, hay version nào bị hardcode hai lần.

| File | Nội suy bằng | Ai đọc |
| --- | --- | --- |
| `infra/local/compose.*.yml` | `${VAR}` | docker compose |
| `engine-runners/trino-runner/catalog/iceberg.properties` | `${ENV:VAR}` | Trino (sẵn có) |
| `engine-runners/spark-runner/spark-defaults.conf.tmpl` | `${VAR}` | entrypoint render lúc khởi động |
| `transforms/profiles.yml` · `dbt_project.yml` | `{{ env_var('VAR') }}` | dbt |
| `ingestion/common/config.py` | `os.environ` | code Spark |
| `Makefile` | `-include .env` | make |

Đổi `WAREHOUSE_BUCKET` một chỗ trong `.env` là Spark, Trino, Iceberg REST và MinIO cùng đổi.

```bash
make env-check      # in giá trị đang có hiệu lực
```

---

## Bắt đầu

```bash
cp .env.example .env     # đổi mật khẩu trước khi dùng thật
make                     # in danh sách lệnh kèm mô tả
```

**Yêu cầu:** Docker + Docker Compose. Riêng môi trường lakehouse, lần đầu chạy sẽ build
image Spark/dbt và tải jar Iceberg → cần mạng.

### Môi trường 1 — DuckDB (khuyến nghị để bắt đầu)

Toàn bộ pipeline chạy trong **một container**. Không MinIO, không Iceberg, không Trino,
không Spark.

```bash
make duckdb-up       # 1) build image + bật container (lần đầu ~30s)
make duckdb-deps     # 2) cài dbt_utils (chạy 1 lần)
make duckdb-run      # 3) dbt build: bronze → silver → gold + test (~7s)
make duckdb-query    # 4) xem thử bảng gold

make duckdb-shell    # liệt kê các bảng trong file hdh.duckdb
make duckdb-down     # dừng (giữ file .duckdb)
make duckdb-clean    # dừng + xoá volume (mất file .duckdb)
```

### Môi trường 2 — Lakehouse

| Service | Vai trò | Cổng (localhost) |
| --- | --- | --- |
| **MinIO** | Object storage (S3) lưu file Iceberg | 9000 API / 9001 console |
| **iceberg-postgres** | Metastore của catalog | — |
| **iceberg-rest** | REST catalog quản lý metadata Iceberg | 8181 |
| **Trino** | Engine truy vấn SQL trên Iceberg | 8080 |
| **Spark** | Ingest CSV → bronze | — |
| **dbt** | Transform + test silver/gold qua Trino | — |

```bash
make lake-up         # 1) bật toàn bộ stack (lần đầu build image + tải jar)
make lake-ps         #    đợi tới khi trino/spark/dbt ở trạng thái "Up"

make lake-ingest     # 2) Spark: CSV → 13 bảng bronze (~2 phút)
                     #    hoặc từng bảng: make lake-ingest-orders

make lake-dbt-deps   # 3) cài dbt_utils (chạy 1 lần)
make lake-dbt        #    dbt build silver + gold + test (--target trino)

make lake-query      # 4) truy vấn kết quả bằng Trino
```

Kiểm tra thủ công:

- **MinIO console:** <http://localhost:9001> — user/pass trong `.env`, bucket `warehouse`.
- **Trino CLI:** `make lake-trino`

  ```sql
  SHOW SCHEMAS FROM iceberg;
  SELECT * FROM iceberg.bronze.orders LIMIT 20;
  SELECT * FROM iceberg.analytics.fact_order_items LIMIT 20;
  SELECT * FROM iceberg.analytics.gold_revenue_daily ORDER BY order_date DESC LIMIT 20;
  ```

- **Spark SQL:** `make lake-spark-sql`

```bash
make lake-down       # dừng, giữ dữ liệu
make lake-clean      # dừng + xoá volume MinIO (mất sạch dữ liệu)
```

---

## Bí quyết dùng chung 1 project dbt cho 2 engine

Điểm mấu chốt để **không nhân đôi** logic silver/gold: một lớp macro mỏng che đi phần khác
biệt giữa hai engine.

- **`{{ bronze('orders') }}`** ([macros/bronze_ref.sql](transforms/macros/bronze_ref.sql)) —
  silver trỏ về đúng nguồn bronze theo môi trường:
  - target `duckdb` → `ref('bronze_orders')` (bronze là dbt model đọc CSV)
  - target `trino`  → `source('bronze', 'orders')` (bronze do Spark ghi Iceberg)
- **macro ngày tháng** ([macros/portable_dates.sql](transforms/macros/portable_dates.sql)) —
  gom vài hàm lệch nhau giữa Trino và DuckDB (`date_format`↔`strftime`,
  `day_of_week`↔`isodow`, `week_of_year`↔`week`).
- **bronze layer** chỉ build khi `target=duckdb` (khai báo `enabled` ở
  [dbt_project.yml](transforms/dbt_project.yml)); ở target `trino` bronze do Spark lo, nên
  source Iceberg được tắt khi chạy DuckDB.

Nhờ vậy mọi model silver/gold viết **một lần**, chạy đúng ở cả hai nơi.
Chi tiết: [transforms/README.md](transforms/README.md).

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

## Test

Hai lớp, ở hai chỗ khác nhau vì kiểm tra hai thứ khác nhau.

```bash
pytest tests -q     # cấu trúc repo — vài giây, không cần Docker
make duckdb-run     # dữ liệu — dbt build = tạo model VÀ test ngay sau mỗi model
make ci-local       # đúng chuỗi mà CI chạy
```

**`tests/`** kiểm tra *repo có nhất quán không* — hai thứ mà không công cụ nào khác bắt được:

| File | Bắt lỗi gì |
| --- | --- |
| `test_sources_registry.py` | Khai bảng trong `sources.yml` mà quên tạo connector (và ngược lại) · trỏ tới CSV không tồn tại · khai `type:` chưa có reader trong `io.py` |
| `test_bronze_parity.py` | **Hai bản cài đặt bronze lệch nhau** — sửa rule ở Spark mà quên bản dbt |

Cái thứ hai quan trọng vì bronze được viết **hai lần** (13 file PySpark cho lakehouse + 13
file SQL cho DuckDB). Đó là đánh đổi có ý thức để dev loop và CI chạy trong vài giây thay vì
phải dựng MinIO+Spark+Trino — nhưng nếu hai bản lệch, hai môi trường cho hai kết quả khác
nhau mà không lỗi nào báo. Test so tập nhãn `_invalid_reason` của từng cặp file.

## Local đọc CSV, production đọc Postgres — đổi ở đâu?

Đúng một chỗ: `ingestion/common/io.py`. `BronzeJob` tách sẵn *nguồn* khỏi *rule làm sạch* và
*đích*, nên đổi nguồn không đụng tới `transform()`, silver, gold hay test dbt. Chi tiết kèm
diff cụ thể: [ingestion/README.md](ingestion/README.md).

> **Hai thư mục tên "tests", hai nghĩa khác nhau — đừng nhầm:**
> `tests/` ở gốc repo là test **cấu trúc repo**, chạy bằng pytest, không cần Docker.
> `transforms/tests/` là **singular data test** của dbt, viết bằng SQL, chạy trong `dbt build`.
> Mỗi cái theo đúng quy ước của hệ sinh thái nó — đổi tên bên nào cũng làm người mới ngạc nhiên.

**Test dữ liệu nằm trong dbt**, cạnh model, ở các file `_*.yml` — đó là chỗ đúng của chúng.
`dbt build` chạy test ngay sau mỗi model, nên silver fail test thì gold không build từ dữ liệu
hỏng. Gồm `not_null`/`unique` cho khoá, `accepted_values`, `relationships` (bắt dòng mồ côi),
`accepted_range` (chặn số âm), và
[test hạt fact](transforms/tests/assert_fact_order_items_grain.sql) (số dòng fact = silver).

Chỉ chạy test dữ liệu: `make duckdb-test` hoặc `make lake-dbt-test`.

## Thêm một bảng mới

1. Khai báo trong [`ingestion/config/sources.yml`](ingestion/config/sources.yml)
2. Tạo `ingestion/connectors/ingest_<bảng>.py` (Spark) và
   `transforms/models/bronze/bronze_<bảng>.sql` (DuckDB)
3. `pytest tests` — bắt ngay nếu hai bước trên lệch nhau
4. `make lake-ingest-<bảng>` rồi `make lake-dbt`

Không phải sửa Makefile: danh sách target sinh ra từ `sources.yml`.
Hướng dẫn đầy đủ tới tận gold: [docs/them-bang-moi.md](docs/them-bang-moi.md).

## Tài liệu

| Tài liệu | Nội dung |
| --- | --- |
| [docs/them-bang-moi.md](docs/them-bang-moi.md) | Hướng dẫn từng bước từ CSV tới gold cho cả hai môi trường |
| [docs/mo-hinh-du-lieu.md](docs/mo-hinh-du-lieu.md) | Sơ đồ quan hệ 13 bảng, công thức join đúng. **Đọc trước khi viết query join.** |
| [docs/star-schema.md](docs/star-schema.md) | Thiết kế dim/fact ở gold layer, lý do thiết kế, query mẫu |
| [docs/star-schema-ly-thuyet.md](docs/star-schema-ly-thuyet.md) | Quy trình 4 bước Kimball, bus matrix, SCD, bridge table |
| [ingestion/README.md](ingestion/README.md) | Hợp đồng của bronze layer, cách viết connector |
| [transforms/README.md](transforms/README.md) | Macro tương thích 2 engine, materialization theo layer |
| [engine-runners/README.md](engine-runners/README.md) | Vì sao Spark config phải render ở entrypoint |
| [infra/README.md](infra/README.md) | Hai stack local, và 3 thứ phải sửa trước khi lên production |
