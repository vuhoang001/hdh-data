# Một image dùng cho CẢ HAI môi trường và CẢ HAI việc:
#
#   transform : dbt-duckdb (dev)  ·  dbt-trino (prod)   -> `dbt build --target dev|prod`
#   ingestion : duckdb + pyarrow + boto3 (dev)          -> `python ingest.py`
#
# Gộp ingestion vào đây thay vì tạo image riêng, vì cả hai chỉ cần đúng một thứ: thư
# viện duckdb. Ở prod, ingestion do image Spark đảm nhiệm (xem spark-runner/), còn
# container này vẫn dùng để chạy loader landing.
FROM python:3.11-slim

ARG DBT_CORE_VERSION
ARG DBT_DUCKDB_VERSION
ARG DBT_TRINO_VERSION
ARG DUCKDB_VERSION
ARG PYARROW_VERSION
ARG BOTO3_VERSION
ARG PYYAML_VERSION
ARG DBT_PROJECT_DIR=/usr/app/dbt

# Pin CHÍNH XÁC (==), không dùng ~=. dbt-core và adapter đánh version độc lập nhau, và
# `~=1.9` từng âm thầm kéo về dbt-core 1.12 — image tag nói 1.9 mà bên trong là 1.12.
# Với data pipeline thì "tháng trước chạy được" phải tái lập được.
#
# duckdb pin TƯỜNG MINH và cài CÙNG LƯỢT với dbt-duckdb: adapter chỉ khai
# `duckdb>=1.0.0` (không chặn trần), nên nếu để pip tự quyết thì mỗi lần build lại có
# thể ra một version khác. Cần >= 1.4.0 mới GHI được Iceberg qua REST catalog. Cài chung
# một lệnh để pip resolve một lần, không ghi đè lẫn nhau.
RUN test -n "${DBT_CORE_VERSION}" && test -n "${DUCKDB_VERSION}" && \
    pip install --no-cache-dir \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-duckdb==${DBT_DUCKDB_VERSION}" \
    "dbt-trino==${DBT_TRINO_VERSION}" \
    "duckdb==${DUCKDB_VERSION}" \
    "pyarrow==${PYARROW_VERSION}" \
    "boto3==${BOTO3_VERSION}" \
    "PyYAML==${PYYAML_VERSION}"

# Container chạy bằng UID của user host (compose truyền `user:`), KHÔNG chạy root.
# Nếu chạy root thì mọi file dbt sinh ra trên bind mount — target/, logs/, dbt_packages/,
# package-lock.yml — sẽ thuộc root:root, khiến IDE và `rm` của bạn không đụng được.
#
# Hệ quả của việc bỏ root: /root/.dbt không còn đọc được, nên profiles.yml lấy từ chính
# thư mục project (DBT_PROFILES_DIR) — nó vốn đã nằm ở dbt/profiles.yml.
ENV DBT_PROFILES_DIR=${DBT_PROJECT_DIR}

# dbt ghi ~/.dbt/.user.yml (thống kê ẩn danh). Trỏ HOME vào /tmp để nó không cần ghi vào
# thư mục của root, cũng không rác ra thư mục project trên máy bạn.
ENV HOME=/tmp

# DuckDB tải extension (httpfs, iceberg) về $HOME/.duckdb lúc chạy lần đầu. Tạo sẵn với
# quyền mở để container chạy bằng UID bất kỳ vẫn ghi được.
RUN mkdir -p "${DBT_PROJECT_DIR}" /tmp/.duckdb && chmod -R 777 /tmp
WORKDIR ${DBT_PROJECT_DIR}
