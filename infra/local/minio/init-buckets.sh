#!/bin/sh
# Khởi tạo bucket warehouse trên MinIO rồi thoát. Chạy 1 lần lúc `make lake-up`.
# Mọi giá trị đến từ .env qua biến môi trường — không hardcode gì ở đây.
set -eu

mc alias set local "$S3_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "local/$WAREHOUSE_BUCKET"

# Môi trường học chạy không auth cho tiện; đừng dùng cấu hình này ở production.
mc anonymous set public "local/$WAREHOUSE_BUCKET"

echo ">> Bucket $WAREHOUSE_BUCKET san sang"
