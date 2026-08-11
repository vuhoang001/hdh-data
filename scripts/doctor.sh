#!/usr/bin/env bash
# Kiểm tra sức khoẻ môi trường: cấu hình -> hạ tầng -> kết nối -> dữ liệu.
#
# Thứ tự KHÔNG ngẫu nhiên: đi từ thứ rẻ nhất và hay sai nhất (thiếu biến env) tới thứ
# đắt nhất (query thật). Hỏng sớm thì dừng sớm, và thông báo chỉ đúng nguyên nhân gốc
# thay vì để người dùng nhìn một lỗi kết nối khó hiểu ở tận cuối.
#
# Chạy: make doctor  /  make ENV=prod doctor
set -uo pipefail

FAIL=0
ok()   { printf '  \033[32m✓\033[0m %s\n' "$1"; }
bad()  { printf '  \033[31m✗\033[0m %s\n' "$1"; FAIL=1; }
warn() { printf '  \033[33m!\033[0m %s\n' "$1"; }

echo "── Cấu hình (ENV=${ENV:-dev}) ─────────────────────────────────"
for v in ENVIRONMENT DBT_TARGET INGESTION_ENGINE ICEBERG_CATALOG_NAME \
         ICEBERG_REST_URI WAREHOUSE_BUCKET MINIO_ENDPOINT \
         BRONZE_NAMESPACE SILVER_NAMESPACE GOLD_NAMESPACE; do
    if [ -z "${!v:-}" ]; then bad "thiếu biến $v"; else ok "$v=${!v}"; fi
done

# Secret còn để nguyên placeholder thì local vẫn chạy, nhưng production thì không.
if [ "${ENVIRONMENT:-dev}" = "prod" ] && [ "${AWS_SECRET_ACCESS_KEY:-}" = "change-me" ]; then
    warn "AWS_SECRET_ACCESS_KEY vẫn là placeholder 'change-me' — đừng dùng ở production thật"
fi

echo
echo "── Công cụ ────────────────────────────────────────────────────"
command -v docker >/dev/null && ok "docker $(docker --version | cut -d' ' -f3 | tr -d ,)" \
    || bad "không thấy docker"
docker compose version >/dev/null 2>&1 && ok "docker compose" || bad "không thấy docker compose"

echo
echo "── Hạ tầng ────────────────────────────────────────────────────"
running() { docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$1"; }
for c in hdh-minio hdh-iceberg-postgres hdh-iceberg-rest; do
    running "$c" && ok "$c đang chạy" || bad "$c KHÔNG chạy — thử: make ENV=${ENV:-dev} up"
done

if [ "${INGESTION_ENGINE:-}" = "spark" ]; then
    running hdh-spark && ok "hdh-spark đang chạy" || warn "hdh-spark không chạy (cần cho ingest ở prod)"
    running hdh-trino && ok "hdh-trino đang chạy" || bad "hdh-trino KHÔNG chạy (cần cho dbt ở prod)"
else
    running hdh-dev-runner && ok "hdh-dev-runner đang chạy" || bad "hdh-dev-runner KHÔNG chạy"
fi

echo
echo "── Kết nối ────────────────────────────────────────────────────"
# Gọi từ TRONG network: MINIO_ENDPOINT/ICEBERG_REST_URI trỏ vào tên service nội bộ,
# nên curl từ host sẽ sai trừ khi bạn đã override sang localhost trong .env.local.
probe() {  # probe <mô tả> <url>
    if docker run --rm --network hdh-net curlimages/curl:latest -sf -m 5 "$2" >/dev/null 2>&1; then
        ok "$1"
    else
        bad "$1 — không tới được $2"
    fi
}
if docker network inspect hdh-net >/dev/null 2>&1; then
    probe "MinIO sống" "http://${MINIO_ENDPOINT:-minio:9000}/minio/health/live"
    probe "Iceberg REST catalog trả lời" "${ICEBERG_REST_URI:-http://iceberg-rest:8181}/v1/config?warehouse=${WAREHOUSE_BUCKET:-warehouse}"
else
    bad "network hdh-net chưa tồn tại — chạy: make ENV=${ENV:-dev} up"
fi

echo
if [ "$FAIL" -eq 0 ]; then
    printf '\033[32mMôi trường sẵn sàng.\033[0m Chạy: make ENV=%s pipeline\n' "${ENV:-dev}"
else
    printf '\033[31mCó vấn đề ở trên.\033[0m Sửa theo thứ tự từ trên xuống — lỗi sau thường là hệ quả của lỗi trước.\n'
fi
exit "$FAIL"
