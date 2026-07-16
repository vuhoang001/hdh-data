FROM python:3.11-slim

# dbt + adapter Trino (dbt chạy transform/test qua Trino trên Iceberg)
RUN pip install --no-cache-dir "dbt-core~=1.9" "dbt-trino~=1.9"

RUN mkdir -p /root/.dbt /usr/app
WORKDIR /usr/app/hdh_dbt
