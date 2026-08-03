FROM python:3.11-slim

# Một image dbt dùng cho CẢ HAI môi trường:
#   - dbt-duckdb : môi trường nhẹ, bronze+silver+gold chạy trong DuckDB
#   - dbt-trino  : môi trường lakehouse, silver/gold chạy qua Trino trên Iceberg
# Cùng project dbt, chọn engine bằng `dbt build --target duckdb|trino`.
RUN pip install --no-cache-dir \
    "dbt-core~=1.9" \
    "dbt-duckdb~=1.9" \
    "dbt-trino~=1.9"

RUN mkdir -p /root/.dbt /usr/app
WORKDIR /usr/app/hdh_dbt
