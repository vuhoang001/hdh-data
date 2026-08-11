# infra/

Hạ tầng chạy pipeline bằng docker compose. **MỘT base dùng chung + overlay theo môi trường** —
đây là thay đổi kiến trúc quan trọng nhất ở tầng infra: dev và prod dùng CHUNG storage
(MinIO + Iceberg catalog), chỉ chồng thêm execution engine của mình.

| File | Vai trò | Dịch vụ |
| --- | --- | --- |
| `compose.base.yml` | Hạ tầng DÙNG CHUNG dev + prod | minio · minio-init · iceberg-postgres · iceberg-rest |
| `compose.dev.yml` | Overlay dev (engine = DuckDB) | + dbt (một container làm cả ingest lẫn transform) |
| `compose.prod.yml` | Overlay prod (engine = Trino) | + trino · spark · dbt |

Makefile chọn overlay theo `ENV`:

```bash
make ENV=dev  up     #  -f compose.base.yml -f compose.dev.yml
make ENV=prod up     #  -f compose.base.yml -f compose.prod.yml
```

- `images/` — Dockerfile + cấu hình **build image** của từng engine (`dbt-runner`,
  `spark-runner`, `trino-runner`). Compose `build:` từ đây. Tách khỏi phần compose vì đây là
  build-time (đổi vài tháng một lần khi nâng version), khác nhịp với run-time. Xem `images/README.md`.
- `minio/init-buckets.sh` — tạo bucket `warehouse` + prefix landing rồi thoát, chạy 1 lần lúc `up`.
- `iceberg-rest/Dockerfile` — image `iceberg-rest-fixture` gốc chỉ đóng gói driver SQLite;
  file này thêm driver Postgres để catalog có metastore thật. SQLite in-memory mất sạch bảng
  mỗi khi connection pool đóng connection.

Compose file **không hardcode giá trị nào** — tất cả đến từ `config/.env.*`. Kiểm tra nhanh:

```bash
make env-check
docker compose --project-directory . \
  --env-file config/.env.shared --env-file config/.env.dev \
  -f infra/compose.base.yml -f infra/compose.dev.yml config
```

## Vì sao dev và prod chung một catalog nhưng không đè nhau

Một Iceberg REST catalog chỉ quản lý đúng một warehouse, nên tách môi trường bằng **NAMESPACE**
chứ không bằng bucket: dev ghi vào `dev_bronze/dev_silver/dev_gold` và landing riêng
(`landing-dev`), prod ghi vào `bronze/silver/gold` và `landing`. Tên **biến** giống hệt nhau ở
hai env (`BRONZE_NAMESPACE`…), chỉ **giá trị** khác — nên code và model không biết mình ở env nào.

## Vì sao giữ nguyên tên project `hdh-spark-trino`

`compose.base.yml` khai `name: hdh-spark-trino` dù thư mục đã đổi. Đổi tên project sẽ làm docker
coi các named volume hiện có (`minio-data`, `iceberg-catalog-db`) là của project khác — tức
mất sạch dữ liệu đã ingest.

## Trước khi dùng cho production

Cấu hình ở đây cố tình dễ dãi để học cho nhanh. Ba thứ phải sửa trước khi chạy thật:

1. `mc anonymous set public` trong `minio/init-buckets.sh` — bucket đang mở công khai.
2. Trino chạy `method: none`, không auth — xem `dbt/profiles.yml`.
3. Credential nằm trong `config/.env.prod` (placeholder) — inject qua secret manager, đừng commit giá trị thật.

Code trong `ingestion/` và `dbt/` thì **không phải sửa** khi lên production, vì chúng chỉ
đọc endpoint từ biến môi trường: đổi MinIO sang S3 hay Trino container sang Trino trên k8s chỉ
là đổi giá trị trong `config/.env.*`.
