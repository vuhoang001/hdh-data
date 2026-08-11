# MIGRATION_PLAN — Kế hoạch di trú theo phase

> Đọc [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) và
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) trước.
>
> **Trạng thái: chờ `APPROVED`.** Chưa có dòng code nào của repo bị sửa.

---

## Nguyên tắc thứ tự

Spec §24 yêu cầu pipeline cũ phải chạy được càng lâu càng tốt. Điều đó quyết định thứ tự,
và có **một ràng buộc cứng**:

> Phase 4 (dbt chuyển bronze sang `source` ở mọi target) **phá vỡ môi trường DuckDB** nếu
> Phase 3 (DuckDB ingestion) chưa xong — vì lúc đó DEV không còn đường nào tạo ra bronze.

Nên thứ tự bắt buộc là: **cấu hình → storage → ingestion DEV → mới được chuyển dbt.**

Cột "Pipeline sau phase" dưới đây theo dõi đúng cam kết backward-compat:

| Phase | Nội dung | DEV cũ | PROD cũ | Rủi ro |
| --- | --- | --- | --- | --- |
| 0 | Audit | ✅ | ✅ | — |
| 1 | Cấu hình + pin version | ✅ | ✅ | Thấp |
| 2 | Storage hợp nhất + landing zone | ✅ | ✅ | Thấp |
| 3 | IngestionEngine + DuckDB ingest | ✅ | ✅ | **Trung bình** |
| 4 | dbt: bronze→source, silver→table, macro | ⚠️ thay bằng DEV mới | ✅ | **CAO** |
| 5 | Ingestion registry + loader hoàn chỉnh | ✅ mới | ✅ | Trung bình |
| 6 | CLI + orchestration | ✅ | ✅ | Thấp |
| 7 | Data contract + quality | ✅ | ✅ | Trung bình |
| 8 | Observability | ✅ | ✅ | Thấp |
| 9 | Chốt DEV | ✅ | ✅ | Trung bình |
| 10 | Chốt PROD | ✅ | ✅ | Trung bình |
| 11 | Integration + parity test | ✅ | ✅ | Trung bình |
| 12 | Docs + dọn code chết | ✅ | ✅ | Thấp |

Phase 4 là điểm không quay lui dễ. Đề xuất: **dừng lại xin xác nhận lần hai trước Phase 4**.

---

## Phase 1 — Tách cấu hình theo môi trường

**Goal.** Một bộ cấu hình có `dev`/`prod` tách bạch, mọi version được pin, chưa đụng logic.

**Files.** `.env` → `config/.env.{shared,dev,prod}` · `.env.example` · `Makefile`
(`-include config/.env.$(ENV)`) · `infra/local/compose.*.yml` (pin image) ·
`.github/workflows/ci.yml` · `.gitignore`

**Changes.**
- Tách biến dùng chung / riêng môi trường theo bảng ở TARGET §4.
- Bổ sung: `ENVIRONMENT`, `DBT_TARGET`, `INGESTION_ENGINE`, `MINIO_ENDPOINT`,
  `SILVER_NAMESPACE`, `GOLD_NAMESPACE`, `LANDING_PREFIX`, `SAMPLE_ENABLED`.
- **Pin `trinodb/trino:latest` và `minio/minio:latest` về version cụ thể** (P1-6) — điều
  kiện tiên quyết cho offline bundle ở Phase sau.
- `make ENV=dev|prod`, mặc định `dev`.

**Risk.** Thấp. Rủi ro thật là quên một biến khiến container không khởi động.

**Tests.** `make env-check ENV=dev` và `ENV=prod` · `docker compose config -q` cả hai file ·
`pytest tests -q` · `make ci-local` phải xanh y như trước.

**Rollback.** `git revert`. Không có thay đổi dữ liệu.

---

## Phase 2 — Hợp nhất storage và tạo landing zone

**Goal.** MinIO + Iceberg catalog trở thành hạ tầng **dùng chung**; có landing zone trên
object storage. Chưa đổi engine nào.

**Files.** `infra/compose.base.yml` (mới) · `infra/compose.dev.yml` (mới) ·
`infra/compose.prod.yml` (mới) · `infra/minio/init-buckets.sh` · `Makefile`

**Changes.**
- Tách compose: `base` (minio, iceberg-postgres, iceberg-rest) + overlay theo env.
- `init-buckets.sh` tạo thêm prefix `landing/` và bucket `lakehouse-dev`.
- Tạo namespace `silver`, `gold` trong catalog (hôm nay chỉ có `bronze` + `analytics`).
- **Giữ nguyên** `compose.duckdb.yml` và `compose.lakehouse.yml` ở giai đoạn này để
  pipeline cũ vẫn chạy song song.

**Risk.** Thấp–trung bình. Đổi tên bucket có thể làm mất tham chiếu tới dữ liệu Iceberg cũ.
Giảm thiểu: **không đổi tên bucket `warehouse` hiện có**, chỉ thêm bucket/prefix mới.

**Tests.** `make lake-up` rồi `make lake-dbt` vẫn chạy · MinIO console thấy prefix mới ·
`SHOW SCHEMAS FROM iceberg` thấy đủ namespace.

**Rollback.** `git revert` + `docker compose down` (không `-v`, giữ dữ liệu).

---

## Phase 3 — Trừu tượng hoá ingestion engine + DuckDB ingestion

**Goal.** DEV có khả năng ghi bronze vào Iceberg **mà không cần Spark**. Đây là mảnh còn
thiếu hoàn toàn (P0-3) và là điều kiện để Phase 4 tồn tại.

**Files.** `ingestion/engines/base.py` (mới) · `ingestion/engines/duckdb_engine.py` (mới) ·
`ingestion/engines/spark_engine.py` (chuyển từ `common/job.py` + `common/session.py`) ·
`ingestion/common/job.py` (thu gọn còn điều phối) · `ingestion/ingest.py` (thêm `--engine`) ·
`ingestion/common/io.py` (thêm reader parquet)

**Changes.**
```text
IngestionEngine (interface)
    read_source(spec, model)  -> đăng ký quan hệ nguồn
    run_sql(sql)              -> chạy chính bronze_<table>.sql
    write_table(df, target, partition_by)
    count(table) / count_where(table, condition)

SparkIngestionEngine   ← code hiện có, chuyển vào, KHÔNG đổi hành vi
DuckDBIngestionEngine  ← mới: ATTACH iceberg REST, đọc landing, ghi Iceberg
```
Cú pháp ATTACH đã kiểm chứng thực nghiệm (CURRENT §3.1), gồm `AUTHORIZATION_TYPE 'none'`.

**Risk.** **Trung bình.** Hai điểm cần canh:
1. Kiểu dữ liệu giữa DuckDB và Spark phải map ra Iceberg giống nhau (`NEUTRAL_TYPES` hiện
   có 4 kiểu + hậu tố `!` cho required) — nếu lệch, hai engine tạo schema Iceberg khác nhau.
2. `partition_by` hiện dựng bằng `pyspark.sql.functions`; DuckDB cần đường khác
   (DDL `CREATE TABLE ... PARTITIONED BY`). `spec.parse_partition()` đã tách sẵn phần
   phân tích khỏi phần dựng biểu thức — tận dụng đúng chỗ này.

**Tests.**
- `pytest tests/unit -q` (không cần Docker).
- Mới: ingest 1 bảng nhỏ (`promotions`, 50 dòng) bằng **cả hai** engine vào 2 namespace
  khác nhau, so schema + số dòng + checksum → phải khớp tuyệt đối.
- `make lake-ingest` (Spark) vẫn chạy đúng như trước.

**Rollback.** `git revert`. `job.py` giữ nguyên chữ ký hàm `run(table)` nên đường Spark cũ
không bị ảnh hưởng dù revert ở bất kỳ điểm nào.

---

## Phase 4 — dbt: một logic, hai engine ⚠️ **PHASE RỦI RO CAO NHẤT**

**Goal.** Xoá rẽ nhánh môi trường khỏi business logic. Sau phase này DEV chạy DuckDB trên
Iceberg thật.

**Files.** `transforms/macros/bronze_ref.sql` (**xoá**) ·
`transforms/macros/duckdb_iceberg_materializations.sql` (mới) ·
`transforms/macros/bronze_helpers.sql` (sửa) · `transforms/dbt_project.yml` ·
`transforms/profiles.yml` · `transforms/models/silver/_sources.yml` ·
6 file `models/silver/*.sql` (`{{ bronze(x) }}` → `{{ source('bronze', x) }}`) ·
13 file `models/bronze/*.sql` (chuyển vai trò: không còn là dbt model được build)

**Changes.**
1. **D1** — bronze là `source` ở mọi target; xoá `bronze()`, xoá `+enabled`, xoá `enabled:`.
2. **D2** — `silver: +materialized: table` (bắt buộc: DuckDB-Iceberg không có `CREATE VIEW`).
3. **D3** — macro override materialization `table` cho target duckdb: DROP + CREATE thay
   cho tmp + rename (nguyên nhân đã cô lập: transaction — xem CURRENT §3.3).
4. `profiles.yml` target `dev` dùng `attach`/`secrets` như TARGET §4.
5. Tách schema silver/gold theo `SILVER_NAMESPACE`/`GOLD_NAMESPACE`.

**Risk.** **CAO.** Ba nguồn rủi ro:
- Macro override materialization là phần chưa có tiền lệ trong repo; hình dạng cuối cùng
  chỉ chốt được khi chạy thật (đã biết nguyên nhân + biết DROP/CREATE hoạt động).
- Đổi silver từ view sang table đổi cả ngữ nghĩa freshness.
- 13 model bronze đổi vai trò — dễ để sót file mồ côi.

**Tests.**
- `dbt parse` và `dbt compile` cho **cả hai** target trước khi chạy.
- `dbt build --target dev` đầy đủ bronze→silver→gold + toàn bộ test dữ liệu.
- `dbt build --target prod` phải xanh y như trước.
- `dbt test --select test_type:unit` (không cần dữ liệu) chạy trước để bắt lỗi công thức sớm.
- **So sánh số dòng từng bảng gold trước/sau** — đây là lưới an toàn quan trọng nhất của phase.

**Rollback.** `git revert` toàn phase (không revert từng file — các thay đổi phụ thuộc lẫn
nhau). Dữ liệu Iceberg mới nằm ở namespace/bucket riêng nên không đè lên dữ liệu cũ.

---

## Phase 5 — Landing loader và registry

**Goal.** Nguồn không còn là bind mount; `sources.yml` mô tả đủ để chạy cả hai engine.

**Files.** `ingestion/load_landing.py` (mới) · `ingestion/config/sources.yml` ·
`ingestion/common/io.py` · `Makefile`

**Changes.** Loader `data/*.csv` → `s3://<bucket>/<landing>/<table>/*.parquet` (D5) ·
reader landing cho cả hai engine · bỏ mount `./data` khỏi compose prod ·
thêm sampling có cascade cho DEV (`SAMPLE_ENABLED`).

> **Sampling phải cascade theo khoá ngoại**, nếu không test `relationships` ở silver sẽ đỏ
> ở DEV trong khi PROD xanh — tức là tạo ra đúng loại lệch dev/prod mà cả refactor này
> đang tìm cách xoá. Cửa sổ ngày trên `orders` → cascade `order_items` → `customers` →
> `geography` → `products`; `promotions` (50 dòng) giữ nguyên cả bảng.

**Risk.** Trung bình — sampling sai làm vỡ ràng buộc tham chiếu.

**Tests.** `pytest tests/unit` · ingest full ở PROD + sample ở DEV, chạy `dbt test` cả hai,
cả hai phải xanh.

**Rollback.** `git revert`; landing là dữ liệu dẫn xuất, xoá prefix và tạo lại được.

---

## Phase 6 — CLI và orchestration

**Goal.** `platform` CLI (spec §20) và định nghĩa pipeline máy đọc được.

**Files.** `platform` (mới) · `orchestration/` (mới) · `Makefile`

**Changes.** `platform dev up|down` · `ingest [table]` · `dbt run|test|build` ·
`pipeline run|status` · **`doctor`** (kiểm MinIO, catalog, DuckDB, Trino, dbt, config,
credential, network). CLI là **wrapper mỏng gọi lại Makefile target** — không nhân đôi logic.

**Risk.** Thấp. Rủi ro duy nhất là CLI và Makefile trôi lệch nhau; chặn bằng nguyên tắc
"CLI không được chứa logic riêng".

**Tests.** `platform doctor` ở cả hai env · smoke test từng lệnh.

**Rollback.** Xoá CLI, Makefile vẫn đủ dùng.

---

## Phase 7 — Data contract và data quality

**Goal.** Metadata gom một chỗ; test sinh ra từ contract thay vì chép tay.

**Files.** `config/contracts/*.yml` (13 file mới) · `scripts/gen_dbt_schema.py` (mới) ·
`transforms/models/*/_*.yml` (sinh ra) · `tests/unit/test_contracts.py` (mới)

**Changes.** Contract theo mẫu TARGET §6 · script sinh YAML dbt từ contract ·
test kiểm contract ↔ model SQL ↔ YAML khớp nhau.

**Risk.** Trung bình — sinh YAML có thể làm mất test viết tay đang có. Giảm thiểu: so
danh sách test trước/sau, không được thiếu cái nào.

**Tests.** `dbt test` số lượng test không giảm · `pytest tests/unit`.

**Rollback.** `git revert`; YAML sinh ra được commit nên khôi phục được.

---

## Phase 8 — Observability và error handling

**Goal.** Biết được pipeline chạy gì, bao lâu, bao nhiêu dòng, hỏng ở đâu.

**Files.** `ingestion/common/logging.py` (mới) · `ingestion/common/errors.py` (mới) ·
`ingestion/engines/*.py` · `ingestion/common/job.py`

**Changes.** Structured JSON log · bảng `ops.pipeline_runs` trên Iceberg · phân loại
exception theo TARGET §8 · exit code riêng cho từng loại lỗi.

**Risk.** Thấp.

**Tests.** Ingest một bảng, kiểm log parse được thành JSON hợp lệ và có đủ trường ·
ép lỗi (bảng không tồn tại, MinIO tắt) kiểm đúng loại exception + exit code.

**Rollback.** `git revert`.

---

## Phase 9 — Chốt môi trường DEV

**Goal.** Đạt Definition of Done phía DEV.

**Changes.** `make dev-up` · `make pipeline` · `make test` chạy trọn vẹn từ máy sạch.

> **Phải verify ở phase này:** DuckDB chạy **native trên laptop** (spec §21, không qua
> Docker). Spike đã chạy *bên trong* network của stack; bản native cần MinIO và
> iceberg-rest expose ra host (cổng 9000/8181 đã map sẵn) và endpoint trỏ `localhost`.
> Đây là điểm **chưa được kiểm chứng** — nếu không đạt, phải điều chỉnh cấu hình endpoint
> theo môi trường, không phải đổi kiến trúc.

**Risk.** Trung bình.

**Tests.** Clone sạch → `make setup && make dev-up && make pipeline && make test` ·
kiểm dữ liệu nằm trên MinIO thật, không phải file local.

**Rollback.** —

---

## Phase 10 — Chốt môi trường PROD

**Goal.** `dbt build --target prod` với Trino, cùng model, cùng contract.

**Risk.** Trung bình — chủ yếu là khác biệt dialect lộ ra ở dữ liệu đầy đủ.

**Tests.** Full pipeline với dữ liệu đầy đủ · so số dòng mọi bảng gold với baseline
trước migration · `dbt source freshness`.

**Rollback.** Giữ nhánh cũ chạy song song tới khi PROD mới xanh.

---

## Phase 11 — Integration test và parity test

**Goal.** Chứng minh yêu cầu quan trọng nhất của spec §25: *cùng model, hai engine, cùng
kết quả ngữ nghĩa*.

**Files.** `tests/integration/` · `tests/parity/` · `.github/workflows/ci.yml`

**Changes.**
| Test | Kiểm gì |
| --- | --- |
| `duckdb + iceberg` | ATTACH, CREATE, INSERT, đọc lại |
| `trino + iceberg` | tương tự |
| `iceberg + minio` | metadata/snapshot thật, không chỉ đếm file |
| **`parity`** | chạy cùng bộ model trên cả hai engine, so số dòng + checksum từng bảng gold |

CI: job DEV (nhẹ, luôn chạy) + job PROD/parity (nặng, chạy trên PR vào `main`).

**Risk.** Trung bình — CI chậm đi đáng kể. Giảm thiểu: parity chạy trên dữ liệu sample.

**Tests.** Chính nó.

**Rollback.** Tắt job nặng, giữ job nhẹ.

---

## Phase 12 — Tài liệu và dọn code chết

**Goal.** Xoá phần đã thay thế — **chỉ sau khi** cái mới đã được chứng minh.

**Files.** `infra/local/compose.duckdb.yml` (xoá) · `compose.lakehouse.yml` (xoá, đã tách
thành base+overlay) · target `duckdb-*`/`lake-*` cũ trong Makefile · `README.md` ·
`docs/*.md` cập nhật theo kiến trúc mới.

**Changes.** README theo cấu trúc spec §26 (Architecture · Prerequisites · Quick Start ·
DEV · PROD · Configuration · Running pipeline · Tests · Debugging · Troubleshooting ·
Deployment · Offline installation).

**Risk.** Thấp — nhưng **chỉ chạy phase này khi Phase 9, 10, 11 đều xanh**. Spec §27 cấm
xoá code cũ trước khi hiểu và thay thế được nó.

**Tests.** Toàn bộ CI xanh · người mới clone làm theo README chạy được.

**Rollback.** `git revert`.

---

## Thay đổi dependency (Output F)

| Hành động | Gói | Lý do |
| --- | --- | --- |
| **ADD** | `duckdb==1.5.5` (tường minh, phía ingestion) | DEV ingestion engine. Đã xác nhận có sẵn trong image `hdh-dbt:1.12.0` |
| **ADD** | `pyarrow` | Ghi landing Parquet |
| **ADD** | `boto3` hoặc `minio` | Loader đẩy file lên object storage |
| **ADD** | `typer` hoặc `click` (tuỳ chọn) | CLI `platform` — có thể dùng `argparse` sẵn có để khỏi thêm dependency |
| **KEEP** | `dbt-core==1.12.0`, `dbt-duckdb==1.10.1`, `dbt-trino==1.10.3` | Đã pin `==`, không cần nâng |
| **KEEP** | `pyspark` (image) | PROD scale — vẫn là engine ingest cho dữ liệu lớn |
| **KEEP** | `pyyaml`, `pytest` | |
| **PIN** | `trinodb/trino:latest` → version cụ thể | P1-6; điều kiện cho offline bundle |
| **PIN** | `minio/minio:latest`, `minio/mc:latest` → version cụ thể | như trên |
| **REMOVE** | — | Không gói nào bị loại. Phần xoá là *code* và *compose*, không phải dependency |

> `dbt-duckdb==1.10.1` khai `duckdb>=1.0.0` (không chặn trần) nên resolve về 1.5.5.
> Cần **pin tường minh `duckdb==1.5.5`** để một lần `pip install` trong tương lai không
> âm thầm kéo về version khác — đúng nguyên tắc mà `dbt-runner/Dockerfile` đã tự đặt ra.

---

## Ánh xạ file cũ → mới (Output D)

```text
GIỮ NGUYÊN VỊ TRÍ, SỬA NỘI DUNG
  ingestion/ingest.py                        → + --engine, --mode
  ingestion/config/sources.yml               → + watermark, contract ref
  ingestion/common/{config,spec,sql_model,io,iceberg}.py   → gần như giữ nguyên
  transforms/models/{bronze,silver,gold}/    → giữ tên tầng (D7)
  transforms/macros/portable_dates.sql       → giữ
  tests/test_bronze_models.py                → tests/unit/test_bronze_models.py

CHUYỂN CHỖ
  ingestion/common/job.py      ─┬→ ingestion/engines/spark_engine.py
  ingestion/common/session.py  ─┘   (+ common/job.py còn lại phần điều phối)
  infra/local/compose.lakehouse.yml → infra/compose.base.yml + compose.prod.yml
  infra/local/compose.duckdb.yml    → infra/compose.base.yml + compose.dev.yml
  .env                              → config/.env.{shared,dev,prod}

VIẾT MỚI
  ingestion/engines/base.py                          IngestionEngine interface
  ingestion/engines/duckdb_engine.py                 DEV ingestion
  ingestion/load_landing.py                          CSV/DB → landing Parquet
  transforms/macros/duckdb_iceberg_materializations.sql   D3
  config/contracts/*.yml                             13 data contract
  scripts/gen_dbt_schema.py                          contract → dbt YAML
  platform                                           CLI
  tests/integration/ · tests/parity/                 spec §25

XOÁ (chỉ ở Phase 12, sau khi cái mới đã xanh)
  transforms/macros/bronze_ref.sql                   D1
  infra/local/compose.duckdb.yml                     đã tách
  Makefile: target duckdb-* / lake-* cũ              thay bằng ENV=dev|prod
```

---

## Điều cần bạn quyết trước khi bắt đầu

| # | Câu hỏi | Đề xuất của tôi |
| --- | --- | --- |
| 1 | Giữ `bronze/silver/gold` hay đổi sang `staging/intermediate/marts`? | **Giữ** (D7) — đổi tốn 40+ file, giá trị chức năng bằng 0 |
| 2 | Giữ thư mục `transforms/` hay đổi thành `dbt/`? | **Giữ** — README giải thích có chủ đích: `ingestion` = E-L, `transforms` = T |
| 3 | Có làm `QueryEngine` tổng quát cho tầng transform không? | **Không** (D8) — dbt đã là abstraction đó; thêm nữa là over-abstraction |
| 4 | silver chuyển sang `table` — chấp nhận chứ? | **Bắt buộc** (D2), không có lựa chọn khác |
| 5 | Có dừng xin xác nhận lần hai trước Phase 4 không? | **Nên** — đây là phase không quay lui dễ |
| 6 | Incremental làm ở Phase nào? | Sau khi dev/prod đã đồng bộ và có parity test |

---

## Trạng thái

**Phase 0 hoàn tất.** Ba tài liệu đã có. Chưa sửa dòng code nào của repo.

Đang chờ `APPROVED` để bắt đầu Phase 1.
