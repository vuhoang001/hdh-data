# infra/

Hạ tầng chạy pipeline bằng docker compose. Hai stack độc lập, chọn bằng tiền tố target
trong Makefile:

| File | Stack | Dịch vụ |
| --- | --- | --- |
| `local/compose.duckdb.yml` | `make duckdb-*` | 1 container dbt-duckdb |
| `local/compose.lakehouse.yml` | `make lake-*` | minio · minio-init · iceberg-postgres · iceberg-rest · trino · spark · dbt |

- `local/minio/init-buckets.sh` — tạo bucket warehouse rồi thoát, chạy 1 lần lúc `make lake-up`.
- `local/iceberg-rest/Dockerfile` — image `iceberg-rest-fixture` gốc chỉ đóng gói driver
  SQLite; file này thêm driver Postgres để catalog có metastore thật. SQLite in-memory mất
  sạch bảng mỗi khi connection pool đóng connection.

Cả hai compose file **không hardcode giá trị nào** — tất cả đến từ `.env` ở gốc repo.
Kiểm tra nhanh giá trị sẽ được nạp:

```bash
make env-check
docker compose --project-directory . -f infra/local/compose.lakehouse.yml config
```

## Vì sao giữ nguyên tên project `hdh-spark-trino`

`compose.lakehouse.yml` khai báo `name: hdh-spark-trino` dù thư mục và target đã đổi tên.
Đổi tên project sẽ làm docker coi các named volume hiện có (`minio-data`,
`iceberg-catalog-db`) là của project khác — tức mất sạch dữ liệu đã ingest.

## Trước khi dùng cho production

Cấu hình ở đây cố tình dễ dãi để học cho nhanh. Ba thứ phải sửa trước khi chạy thật:

1. `mc anonymous set public` trong `local/minio/init-buckets.sh` — bucket đang mở công khai.
2. Trino chạy `method: none`, không auth — xem `transforms/profiles.yml`.
3. Credential nằm trong `.env` — cần chuyển sang secret manager.

Code trong `ingestion/` và `transforms/` thì **không phải sửa** khi lên production, vì chúng
chỉ đọc endpoint từ biến môi trường: đổi MinIO sang S3 hay Trino container sang Trino trên
k8s chỉ là đổi giá trị trong `.env`.
