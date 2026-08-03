# Một image dbt dùng cho CẢ HAI môi trường:
#   - dbt-duckdb : môi trường nhẹ, bronze+silver+gold chạy trong DuckDB
#   - dbt-trino  : môi trường lakehouse, silver/gold chạy qua Trino trên Iceberg
# Cùng project dbt, chọn engine bằng `dbt build --target duckdb|trino`.
FROM python:3.11-slim

ARG DBT_CORE_VERSION
ARG DBT_DUCKDB_VERSION
ARG DBT_TRINO_VERSION
ARG DBT_PROJECT_DIR=/usr/app/dbt

# Pin CHÍNH XÁC (==), không dùng ~=. dbt-core và adapter đánh version độc lập nhau, và
# `~=1.9` từng âm thầm kéo về dbt-core 1.12 — image tag nói 1.9 mà bên trong là 1.12.
# Với data pipeline thì "tháng trước chạy được" phải tái lập được.
RUN test -n "${DBT_CORE_VERSION}" && pip install --no-cache-dir \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-duckdb==${DBT_DUCKDB_VERSION}" \
    "dbt-trino==${DBT_TRINO_VERSION}"

# Container chạy bằng UID của user host (compose truyền `user:`), KHÔNG chạy root.
# Nếu chạy root thì mọi file dbt sinh ra trên bind mount — target/, logs/, dbt_packages/,
# package-lock.yml — sẽ thuộc root:root, khiến IDE và `rm` của bạn không đụng được.
#
# Hệ quả của việc bỏ root: /root/.dbt không còn đọc được, nên profiles.yml lấy từ chính
# thư mục project (DBT_PROFILES_DIR) — nó vốn đã nằm ở transforms/profiles.yml.
ENV DBT_PROFILES_DIR=${DBT_PROJECT_DIR}

# dbt ghi ~/.dbt/.user.yml (thống kê ẩn danh). Trỏ HOME vào /tmp để nó không cần ghi vào
# thư mục của root, cũng không rác ra thư mục project trên máy bạn.
ENV HOME=/tmp

# /warehouse là nơi file hdh.duckdb sống (named volume ở môi trường DuckDB).
# Docker khởi tạo named volume mới THEO quyền của thư mục này trong image, nên phải tạo sẵn
# với quyền mở — nếu không, volume sẽ là root:root và container chạy uid 1000 không ghi được.
RUN mkdir -p "${DBT_PROJECT_DIR}" /warehouse && chmod 777 /warehouse /tmp
WORKDIR ${DBT_PROJECT_DIR}
