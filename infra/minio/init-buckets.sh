#!/bin/sh
# Khởi tạo bucket warehouse + landing zone trên MinIO rồi thoát.
# Chạy 1 lần lúc `make up` (cả dev lẫn prod đều dùng chung script này).
# Mọi giá trị đến từ config/.env.* qua biến môi trường — không hardcode gì ở đây.
set -eu

mc alias set local "$S3_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "local/$WAREHOUSE_BUCKET"

# Môi trường học chạy không auth cho tiện; đừng dùng cấu hình này ở production.
mc anonymous set public "local/$WAREHOUSE_BUCKET"

# LANDING ZONE — nơi nguồn được đẩy lên dưới dạng Parquet TRƯỚC khi vào bronze.
# S3 không có thư mục thật, prefix chỉ tồn tại khi có object bên trong. Tạo một
# object giữ chỗ để `mc ls` và MinIO console nhìn thấy được prefix ngay từ đầu,
# giúp phân biệt "chưa load lần nào" với "cấu hình sai bucket".
echo "landing zone" | mc pipe "local/$WAREHOUSE_BUCKET/$LANDING_PREFIX/.keep"

echo ">> Bucket $WAREHOUSE_BUCKET san sang (landing: $LANDING_PREFIX/)"
