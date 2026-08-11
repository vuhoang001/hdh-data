#!/usr/bin/env bash
# Nội suy template config Spark bằng biến môi trường hiện tại, rồi chạy lệnh được truyền vào.
#
# Nhờ bước này, infra/images/spark-runner/spark-defaults.conf.tmpl là NƠI DUY NHẤT khai báo cấu hình Spark,
# còn giá trị thì đến từ .env — không có endpoint/bucket nào bị hardcode trong image.
set -euo pipefail

TEMPLATE="${SPARK_CONF_TEMPLATE:-/opt/spark/conf-template/spark-defaults.conf.tmpl}"
RENDERED="${SPARK_CONF_DIR:-/opt/spark/conf}/spark-defaults.conf"

if [[ -f "$TEMPLATE" ]]; then
    mkdir -p "$(dirname "$RENDERED")"
    # expandvars thay ${VAR} và $VAR; biến không tồn tại được giữ nguyên để lỗi lộ ra sớm.
    python3 -c 'import os,sys; sys.stdout.write(os.path.expandvars(sys.stdin.read()))' \
        < "$TEMPLATE" > "$RENDERED"
    # Còn ${...} ở dòng CẤU HÌNH (bỏ qua comment) nghĩa là .env thiếu biến -> dừng ngay,
    # vì Spark sẽ nuốt giá trị sai này và chỉ báo lỗi mãi sau, ở chỗ khó lần.
    if grep -vE '^[[:space:]]*(#|$)' "$RENDERED" | grep -q '\${'; then
        echo "LỖI: còn biến chưa được thay trong $RENDERED — thiếu khai báo trong .env:" >&2
        grep -vE '^[[:space:]]*(#|$)' "$RENDERED" | grep '\${' >&2
        exit 1
    fi
else
    echo "CẢNH BÁO: không thấy template $TEMPLATE, Spark sẽ chạy với config mặc định." >&2
fi

exec "$@"
