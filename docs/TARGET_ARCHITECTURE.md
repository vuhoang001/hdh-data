# TARGET_ARCHITECTURE — Kiến trúc mục tiêu

> Đọc [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) trước. Tài liệu này chỉ nói
> **sẽ thành cái gì và vì sao**; *làm theo thứ tự nào* nằm ở
> [MIGRATION_PLAN.md](MIGRATION_PLAN.md).

---

## 1. Nguyên tắc nền

Toàn bộ thiết kế xoay quanh một câu:

> **Storage, catalog, table format, và business logic là MỘT. Chỉ execution engine là hai.**

Kiến trúc hiện tại có hai storage. Kiến trúc mục tiêu có một.

```text
                    ┌──────────────────────────────────┐
                    │        SOURCES                   │
                    │  CSV · DB · API · CDC            │
                    └────────────────┬─────────────────┘
                                     │  connector (registry: sources.yml)
                                     ▼
                    ┌──────────────────────────────────┐
                    │   LANDING   s3://<bucket>/landing│   Parquet, immutable
                    └────────────────┬─────────────────┘
                                     │  IngestionEngine  (DuckDB | Spark)
                                     │  chay CHINH bronze_<table>.sql
                                     ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                    ICEBERG  on  MinIO                               │
   │   namespace bronze  →  namespace silver  →  namespace gold          │
   │            ▲                    ▲                  ▲                │
   │            │                    └────── dbt ───────┘                │
   │            │                    (MOT bo model, HAI target)          │
   └────────────┼────────────────────────────────────────────────────────┘
                │
        Iceberg REST catalog  (metastore: Postgres)
                │
     ┌──────────┴──────────┐
     ▼                     ▼
  DEV: DuckDB          PROD: Trino
  (+ ingest)           (+ Spark khi can scale)
                │
                ▼
        CONSUMPTION: Superset / BI / API
```

**Điểm khác biệt cốt lõi so với hiện tại:** mũi tên từ DuckDB và từ Trino cùng chỉ vào
**một** khối Iceberg/MinIO. Hôm nay chúng chỉ vào hai khối khác nhau.

Bằng chứng khả thi: spike đã cho DuckDB đọc đúng `bronze.orders` **646.945 dòng** do Spark
ghi, và đọc `gold_revenue_daily` do Trino ghi. Chi tiết ở CURRENT_ARCHITECTURE §3.

---

## 2. Ranh giới: cái gì được phụ thuộc môi trường, cái gì không

Spec §4 cấm rẽ nhánh môi trường. Nhưng cấm tuyệt đối ở mọi tầng là bất khả thi — hai
engine có dialect khác nhau thật. Nên ranh giới phải nói rõ:

| Tầng | Được rẽ nhánh theo env? | Lý do |
| --- | --- | --- |
| **Model SQL** (`models/**/*.sql`) | ❌ **TUYỆT ĐỐI KHÔNG** | Đây là business logic |
| **Data contract / test** | ❌ Không | Phải giống nhau mới so sánh được |
| **Macro tương thích** (`portable_dates`, materialization override) | ✅ Có | Đây là **adapter layer**, đúng chỗ của nó |
| **profiles / config / compose** | ✅ Có | Đây là infrastructure |
| **IngestionEngine** | ✅ Có (qua interface) | Đúng nghĩa polymorphism |

Nói cách khác: **rẽ nhánh được phép tồn tại, nhưng chỉ ở tầng adapter, và phải được đặt
tên là adapter.** Cái sai của `bronze_ref.sql` hiện nay không phải là nó rẽ nhánh — mà là
nó rẽ nhánh **giữa hai khái niệm dữ liệu khác nhau** (`ref` = model do dbt tạo, `source` =
bảng do hệ khác tạo) ngay trong đường dẫn business logic.

---

## 3. Quyết định thiết kế

Mỗi quyết định ghi rõ: lý do, đánh đổi, và mức chắc chắn.

### D1 — bronze trở thành `source` ở CẢ HAI môi trường → **xoá `bronze_ref.sql`**

Hôm nay bronze là *dbt model* ở dev và *source* ở prod. Sau refactor, bronze do
**IngestionEngine** ghi vào Iceberg ở cả hai môi trường, nên dbt luôn đọc qua `source()`.

```diff
- {{ bronze('orders') }}          → ref() hoặc source() tuỳ target
+ {{ source('bronze', 'orders') }}   → giống hệt nhau ở mọi target
```

**Được:** xoá hẳn `macros/bronze_ref.sql`, xoá `+enabled` ở `dbt_project.yml`, xoá
`enabled:` ở `_sources.yml`. Rẽ nhánh môi trường biến mất khỏi business logic.
**Mất:** không còn chạy được dbt "trần" không cần MinIO. Chấp nhận — đó chính là mục tiêu.
**Chắc chắn:** cao.

### D2 — silver đổi từ `view` sang `table` **[BẮT BUỘC BỞI RÀNG BUỘC KỸ THUẬT]**

Spike xác nhận: `CREATE VIEW` báo `Not implemented Error: Create View` trong DuckDB-Iceberg.
Tầng silver hiện là `+materialized: view`, nên **không thể giữ nguyên**.

Ba lựa chọn và lý do chọn:

| Phương án | Đánh giá |
| --- | --- |
| silver = `table` ở cả hai env | ✅ **Chọn** — đồng nhất, đơn giản nhất, giữ được "same semantic result" |
| silver = `view` ở Trino, `table` ở DuckDB | ❌ Hai env khác materialization → khác hành vi freshness, khó so sánh |
| silver = ephemeral (CTE inline) | ❌ Mất khả năng test silver độc lập; gold SQL phình to |

**Mất:** silver không còn "luôn phản ánh bronze mới nhất" (README hiện tại nêu đây là lý do
chọn view), và tốn thêm dung lượng. **Được:** materialization giống nhau ở hai môi trường,
silver query nhanh hơn, và mở đường cho incremental sau này.
**Chắc chắn:** cao — đây là ràng buộc cứng, không phải sở thích.

### D3 — Override materialization `table` cho target DuckDB **[BẮT BUỘC]**

Spike xác nhận nguyên nhân chính xác: dbt tạo `x__dbt_tmp` rồi RENAME **trong một
transaction**; DuckDB-Iceberg từ chối rename bảng đã bị sửa trong cùng transaction.
Autocommit thì chạy được, và DROP+CREATE lặp lại được.

Fix: macro trong `dbt/macros/` override `relations/*` cho target duckdb, dùng
DROP + CREATE thay vì tmp + rename. Adapter dùng macro chuẩn nên đây là extension point
được hỗ trợ — **không fork adapter, không đổi adapter**.

**Mất:** materialization không còn atomic ở dev (có một khoảnh khắc bảng không tồn tại).
Chấp nhận được ở dev; prod dùng Trino nên không ảnh hưởng.
**Chắc chắn:** cao về nguyên nhân, trung bình về hình dạng cuối của macro — sẽ chốt ở Phase 4.

### D4 — DEV có ingestion riêng: `DuckDBIngestionEngine`

Đây là mảnh **còn thiếu hoàn toàn** hôm nay (P0-3). DuckDB đọc landing Parquet rồi chạy
**chính** `bronze_<table>.sql` và ghi vào Iceberg — đúng cách Spark đang làm.

`sql_model.py` đã tách render khỏi execute, nên đây là **thêm một executor**, không phải
viết lại renderer. Logic bronze vẫn đúng một bản.

### D5 — Landing zone Parquet trên MinIO

Nguồn không còn là bind mount. Loader đẩy `data/*.csv` → `s3://<bucket>/landing/<table>/`
dưới dạng **Parquet**.

**Vì sao Parquet chứ không phải CSV:** giữ được kiểu dữ liệu thật (CSV qua Hive/Trino trả
mọi cột `VARCHAR`, buộc phải viết lại `bronze_source` thành sinh `cast`), nén tốt hơn, và
production hiếm khi để raw CSV làm landing. Khối `{% set columns %}` giữ nguyên vai trò.
**Mất:** thêm một bước loader.

### D6 — Tách namespace: `bronze` / `silver` / `gold`

Hôm nay silver và gold chung namespace `analytics`. Tách ra 3 namespace đúng theo spec §10.
Iceberg catalog tự map namespace → prefix trên MinIO, nên layout `s3://<bucket>/{bronze,
silver,gold}/` có gần như miễn phí.

**Mất:** `ANALYTICS_SCHEMA` biến mất → mọi query mẫu, Makefile, docs phải cập nhật.

### D7 — GIỮ tên `bronze/silver/gold` và thư mục `dbt/`

Spec §5/§9 gợi ý `staging/intermediate/marts` và thư mục `dbt/`. **Đề xuất không đổi**, vì:

- `bronze/silver/gold` và `staging/intermediate/marts` là **cùng một khái niệm**, khác quy ước.
- Đổi tên đụng 40+ file model, 20+ file YAML, toàn bộ 5 tài liệu trong `docs/`, macro
  `generate_schema_name`, Makefile, và mọi query mẫu — **đổi lấy giá trị chức năng bằng 0**.
- README hiện tại giải thích có chủ đích vì sao là `ingestion/` + `dbt/`: đó là
  **E-L** và **T** của ELT. Tên thư mục đang nói ra kiến trúc.

Đây là quyết định của bạn — nếu muốn theo đúng quy ước dbt chuẩn, tôi đổi ở Phase 4, chi
phí khoảng nửa ngày và rủi ro thấp (chủ yếu là sed + chạy lại test).

### D8 — Không tạo `QueryEngine` tổng quát cho tầng transform

Spec §12 đề nghị `QueryEngine` với `execute/query/create_table/write`, có `DuckDBEngine`
và `TrinoEngine`.

**Đề xuất: chỉ làm cho ingestion, không làm cho transform.** Lý do: ở tầng transform,
**dbt đã chính là abstraction đó rồi** — `dbt build --target dev|prod` chuyển engine mà
model không đổi một ký tự. Viết thêm một `QueryEngine` để rồi dbt không dùng tới là đúng
định nghĩa "abstraction quá mức cần thiết" mà spec §27 cấm.

Nơi thật sự cần abstraction là ingestion, vì ở đó **không có dbt**:

```text
IngestionEngine            (interface, ~4 method)
      ├── DuckDBIngestionEngine     dev · laptop · nhẹ
      └── SparkIngestionEngine      prod · scale (code hiện có)
```

Nếu sau này cần chạy SQL ngoài dbt (maintenance, compaction, kiểm tra), lúc đó mới thêm —
và thêm đúng cái cần.

---

## 4. Thiết kế cấu hình

`.env` đã là nguồn sự thật duy nhất và **không chỗ nào hardcode bucket/endpoint** — nền
này giữ nguyên. Chỉ tách theo môi trường và bổ sung biến còn thiếu.

```text
config/
  .env.shared      # version pin, tên namespace, tên bucket — giống nhau mọi env
  .env.dev         # DuckDB, MinIO local, sample data
  .env.prod        # Trino, MinIO/S3 thật, full data
```

`make ENV=dev` / `make ENV=prod` nạp `.env.shared` + `.env.$(ENV)`.

| Biến | DEV | PROD |
| --- | --- | --- |
| `ENVIRONMENT` | `dev` | `prod` |
| `DBT_TARGET` | `dev` | `prod` |
| `INGESTION_ENGINE` | `duckdb` | `spark` |
| `MINIO_ENDPOINT` | `minio:9000` (trong container) / `localhost:9000` (native) | endpoint thật |
| `ICEBERG_REST_URI` | `http://iceberg-rest:8181` | URI thật |
| `WAREHOUSE_BUCKET` | `lakehouse-dev` | `lakehouse` |
| `LANDING_PREFIX` | `landing` | `landing` |
| `BRONZE_NAMESPACE` / `SILVER_NAMESPACE` / `GOLD_NAMESPACE` | `bronze`/`silver`/`gold` | giống hệt |
| `TRINO_HOST` / `TRINO_PORT` | — | `trino` / `8080` |
| `DUCKDB_PATH` | `/tmp/dev.duckdb` (chỉ là scratch, **không** phải warehouse) | — |
| `SAMPLE_ENABLED` | `true` | `false` |

**Lưu ý quan trọng về `DUCKDB_PATH`:** sau refactor, file `.duckdb` **không còn là kho dữ
liệu**. Nó chỉ là database tạm để DuckDB có chỗ ATTACH từ đó. Toàn bộ dữ liệu nằm trên
Iceberg/MinIO. Đây là thay đổi ý nghĩa cần nói rõ trong README, nếu không người dùng cũ sẽ
tưởng mất dữ liệu.

**Secret ở production:** `.env` chỉ dùng cho local. Prod phải lấy từ secret manager
(Docker/K8s secret, Vault, AWS SM). Thiết kế: mọi biến đọc qua `os.environ` nên inject
kiểu gì cũng được — không cần đổi code.

### profiles.yml mục tiêu

```yaml
hdh_dbt:
  target: "{{ env_var('DBT_TARGET', 'dev') }}"
  outputs:
    dev:                      # DuckDB → Iceberg → MinIO
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      database: "{{ env_var('ICEBERG_CATALOG_NAME') }}"
      extensions: [httpfs, iceberg]
      secrets:
        - type: s3
          key_id: "{{ env_var('MINIO_ACCESS_KEY') }}"
          secret: "{{ env_var('MINIO_SECRET_KEY') }}"
          endpoint: "{{ env_var('MINIO_ENDPOINT') }}"
          url_style: path
          use_ssl: false
      attach:
        - path: "{{ env_var('WAREHOUSE_BUCKET') }}"
          alias: "{{ env_var('ICEBERG_CATALOG_NAME') }}"
          type: ICEBERG
          options:
            endpoint: "{{ env_var('ICEBERG_REST_URI') }}"
            authorization_type: none      # ← BẮT BUỘC, đã kiểm chứng
    prod:                     # Trino → Iceberg → MinIO  (gần như giữ nguyên hiện tại)
      type: trino
      ...
```

Cú pháp `attach` và `secrets` ở trên **đã được kiểm chứng bằng cách đọc source
`dbt/adapters/duckdb/credentials.py`**: `Attachment` có trường `options: Dict[str, Any]`
render thành `KEY 'value'`, và `Secret` nhận `secret_kwargs` tuỳ ý.

---

## 5. Repository structure mục tiêu

Nguyên tắc: **tiến hoá cấu trúc hiện tại**, không áp khuôn mẫu. Spec §5 đã nói rõ không
được máy móc, và §27 cấm over-abstraction. Cụ thể **không** tạo `apps/api/`, `apps/worker/`,
`core/domain/` — chưa có nhu cầu nào trong repo này cần tới chúng.

```text
hdh-data/
├── platform                       ★ MỚI  CLI (spec §20) — wrapper mỏng, gọi lại Makefile
│
├── config/                        ★ MỚI  thay cho .env đơn lẻ
│   ├── .env.shared · .env.dev · .env.prod
│   └── contracts/                 ★ MỚI  data contract từng dataset (spec §15)
│
├── ingestion/                     GIỮ — refactor phần engine
│   ├── ingest.py                  giữ; thêm --engine, --mode
│   ├── load_landing.py            ★ MỚI  CSV/DB → landing Parquet trên MinIO
│   ├── config/sources.yml         giữ; thêm watermark, contract ref
│   ├── engines/                   ★ MỚI  tách từ job.py
│   │   ├── base.py                IngestionEngine (interface)
│   │   ├── duckdb_engine.py       ★ MỚI  DEV
│   │   └── spark_engine.py        ← chuyển từ job.py + session.py
│   └── common/                    giữ: config · spec · sql_model · io · iceberg
│
├── dbt/                    GIỮ TÊN (D7) — dbt project
│   ├── macros/
│   │   ├── bronze_helpers.sql     sửa: read_csv → iceberg source
│   │   ├── bronze_ref.sql         ✗ XOÁ (D1)
│   │   ├── portable_dates.sql     giữ
│   │   └── duckdb_iceberg_materializations.sql   ★ MỚI (D3)
│   └── models/{bronze,silver,gold}/   giữ; bronze chuyển sang source-only
│
├── engine-runners/                giữ: spark-runner · dbt-runner · trino-runner
├── infra/
│   ├── compose.base.yml           ★ MỚI  minio · postgres · iceberg-rest  (DÙNG CHUNG)
│   ├── compose.dev.yml            ★ MỚI  + dbt-duckdb
│   ├── compose.prod.yml           ★ MỚI  + trino (+ spark tuỳ chọn)
│   └── minio/ · iceberg-rest/     giữ
│
├── tests/
│   ├── unit/                      ← test_bronze_models.py chuyển vào
│   ├── integration/               ★ MỚI  duckdb+iceberg, trino+iceberg
│   └── parity/                    ★ MỚI  DEV vs PROD cùng kết quả (spec §25)
│
├── scripts/ · docs/ · data/
├── Makefile · README.md · .env.example
```

`compose.base.yml` là thay đổi cấu trúc quan trọng nhất ở tầng infra: MinIO + catalog
**dùng chung**, dev và prod chỉ chồng thêm engine của mình. Hôm nay hai compose file không
chia sẻ gì cả.

---

## 6. Data contract (spec §15)

Hôm nay metadata rải ba nơi: `sources.yml` (partition), `_bronze.yml`/`_sources.yml`
(test), model SQL (`columns`). Gom về `config/contracts/<dataset>.yml`:

```yaml
name: orders
layer: bronze
owner: data-platform
source: {system: ecommerce, type: csv, file: orders.csv}
primary_key: [order_id]
partition_by: months(order_date)
freshness: {warn_after: 24h, error_after: 72h}
watermark: {column: order_date, strategy: incremental}
columns:
  - {name: order_id,   type: integer, nullable: false}
  - {name: order_date, type: date}
quality:
  - {rule: not_null, columns: [order_id]}
  - {rule: unique,   columns: [order_id]}
  - {rule: accepted_values, column: order_status,
     values: [created, paid, shipped, delivered, returned, cancelled]}
```

**Cách tránh trùng lặp:** contract là **nguồn**, `_bronze.yml` của dbt được **sinh ra** từ
nó bằng script, và `tests/unit/` kiểm hai bên khớp. Không chép tay hai bản — đó đúng là
loại lệch mà repo này vừa mất công xoá bỏ ở tầng bronze.

---

## 7. Incremental (spec §17)

Hôm nay: `createOrReplace` — ghi đè toàn bộ, mọi lần.

Thiết kế mục tiêu, thêm cột kỹ thuật vào bronze:

| Cột | Ý nghĩa |
| --- | --- |
| `_ingested_at` | đã có |
| `_source_file` | đã có |
| `_batch_id` | ★ mới — định danh lần chạy, cho phép rollback/reprocess theo batch |
| `_watermark` | ★ mới — giá trị mốc của lần chạy |

Bốn chế độ, chọn bằng `--mode`:

```text
full        ghi đè toàn bộ            (hiện tại; giữ làm mặc định an toàn)
incremental append/merge theo watermark > lần chạy trước
backfill    chạy lại một khoảng thời gian chỉ định
reprocess   xoá theo _batch_id rồi chạy lại
```

Idempotency dựa trên Iceberg MERGE theo primary key trong contract. **Đây là phần rủi ro
cao nhất của cả migration** nên nó nằm ở phase cuối, sau khi dev/prod đã đồng bộ và có
test chặn hồi quy.

---

## 8. Observability (spec §18) và Error handling (§19)

Hiện tại: `logger.info` đếm dòng, không structured, không lưu lại.

Mục tiêu — mỗi lần chạy ghi một bản ghi JSON có cấu trúc:

```json
{"event":"ingestion.completed","table":"orders","engine":"duckdb","env":"dev",
 "batch_id":"...","rows_read":646945,"rows_written":646945,"rows_rejected":0,
 "duration_ms":12043,"watermark":"2024-12-31"}
```

Ghi ra stdout (JSON lines) + một bảng Iceberg `ops.pipeline_runs` để truy vấn được bằng
chính SQL. Không thêm hạ tầng metrics mới ở giai đoạn này — đó là over-engineering cho
quy mô hiện tại.

Error handling: phân loại lỗi thành `SourceError` / `IngestionError` / `StorageError` /
`ContractError` / `QualityError`, mỗi loại có exit code riêng để orchestrator xử lý khác
nhau. Tuyệt đối không `except Exception: pass` — hiện tại repo **không** có chỗ nào như
vậy, cần giữ nguyên tình trạng đó.

---

## 9. Offline capability (spec §22)

| Thành phần | Cách bundle |
| --- | --- |
| Python packages | `pip download` → `offline-bundle/wheels/` |
| dbt packages | `dbt_packages/` đã vendored sẵn (dbt_utils) — commit hoặc bundle |
| Docker images | `docker save` → `offline-bundle/images/*.tar` |
| Iceberg / JDBC jar | tải sẵn vào `offline-bundle/jars/`, Dockerfile COPY thay vì curl |

CLI: `platform bundle create` / `platform bundle install`.

Điều kiện tiên quyết: **pin chặt mọi version**. Repo đã pin `==` cho dbt (Dockerfile ghi rõ
lý do) nhưng `trinodb/trino:latest` và `minio/minio:latest` thì chưa — phải sửa ở Phase 1,
nếu không bundle không tái lập được.

---

## 10. Rủi ro đã biết và cách xử lý

| Rủi ro | Mức | Xử lý |
| --- | --- | --- |
| DuckDB-Iceberg không có `CREATE VIEW` | ✅ đã biết | D2 — silver thành table |
| dbt `table` materialization fail | ✅ đã biết, đã tìm ra nguyên nhân | D3 — macro override |
| `CREATE OR REPLACE` không hỗ trợ | ✅ đã biết | DROP+CREATE trong macro |
| **DuckDB chạy native trên laptop (spec §21)** | ⚠️ **CHƯA KIỂM** | Spike chạy *trong* network. Native cần MinIO + iceberg-rest expose ra host (đã map 9000/8181) và endpoint dùng `localhost`. **Phải verify ở Phase 9** |
| Ghi đồng thời từ 2 engine vào cùng bảng | ⚠️ chưa gặp | Chốt **ownership**: một engine ghi một tầng, không chồng lấn |
| DuckDB-Iceberg còn mới, API có thể đổi | ⚠️ | Pin `duckdb==1.5.5`; integration test bắt hồi quy |
| Mất khả năng chạy dbt không cần Docker | ⚠️ chấp nhận | Đánh đổi có chủ đích của D1 |

---

## 11. Definition of Done ánh xạ sang kiểm chứng

| Yêu cầu (spec §29) | Kiểm bằng |
| --- | --- |
| `make dev-up && make pipeline && make test` chạy được | Phase 9 |
| DEV dùng DuckDB làm engine, dữ liệu trên Iceberg/MinIO | `tests/integration/` |
| `dbt build --target prod` chạy với Trino | Phase 10 |
| Hai env dùng **cùng** model/logic/contract/test | `tests/parity/` — chạy cả hai rồi so kết quả |
| Không hardcode credential/endpoint | grep trong CI |
| bronze/silver/gold là Iceberg table thật | truy vấn metadata catalog, không chỉ đếm file |
