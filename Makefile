# Lệnh tắt cho hdh-data — HAI môi trường độc lập, chọn theo tiền tố target:
#
#   duckdb-*  : môi trường NHẸ    (CSV -> DuckDB, chỉ cần Docker, chạy vài giây)
#   st-*      : môi trường LAKEHOUSE (MinIO+Iceberg+Spark+Trino, giống production)
#
# Luồng nhanh:
#   DuckDB : make duckdb-up && make duckdb-deps && make duckdb-run && make duckdb-query
#   Trino  : make st-up && make st-ingest && make st-dbt-deps && make st-dbt && make st-query

.DEFAULT_GOAL := help

# --project-directory . để mọi đường dẫn trong compose tính từ gốc repo và .env được nạp.
COMPOSE_DUCKDB = docker compose --project-directory . -f environments/duckdb/docker-compose.yml
COMPOSE_ST     = docker compose --project-directory . -f environments/spark-trino/docker-compose.yml

.PHONY: help \
        duckdb-up duckdb-deps duckdb-run duckdb-test duckdb-query duckdb-shell duckdb-down duckdb-clean duckdb-ps \
        st-up st-down st-clean st-ps st-logs st-dbt-deps st-dbt st-dbt-test st-trino st-spark-sql st-query \
        st-ingest st-ingest-orders st-ingest-order-items st-ingest-customers st-ingest-geography \
        st-ingest-products st-ingest-payments st-ingest-shipments st-ingest-returns st-ingest-reviews \
        st-ingest-promotions st-ingest-inventory st-ingest-sales-daily st-ingest-web-traffic

help:          ## Danh sách lệnh
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# =========================================================================
# Môi trường 1: DuckDB (nhẹ) — bronze+silver+gold chạy trọn trong dbt-duckdb
# =========================================================================
duckdb-up:     ## [duckdb] Bật container dbt-duckdb (build lần đầu)
	$(COMPOSE_DUCKDB) up -d --build

duckdb-deps:   ## [duckdb] Cài dbt_utils (chạy 1 lần)
	$(COMPOSE_DUCKDB) exec dbt dbt deps

duckdb-run:    ## [duckdb] Build cả pipeline: bronze -> silver -> gold + test
	$(COMPOSE_DUCKDB) exec dbt dbt build --target duckdb

duckdb-test:   ## [duckdb] Chỉ chạy test dữ liệu
	$(COMPOSE_DUCKDB) exec dbt dbt test --target duckdb

duckdb-query:  ## [duckdb] Xem thử 1 bảng gold
	$(COMPOSE_DUCKDB) exec dbt dbt show --target duckdb --limit 20 \
	  --inline "select * from analytics.gold_orders_daily order by order_date"

duckdb-shell:  ## [duckdb] Mở DuckDB CLI trên file warehouse
	$(COMPOSE_DUCKDB) exec dbt python -c "import duckdb; duckdb.connect('/warehouse/hdh.duckdb').sql('show all tables').show()"

duckdb-ps:     ## [duckdb] Trạng thái container
	$(COMPOSE_DUCKDB) ps

duckdb-down:   ## [duckdb] Dừng (giữ file .duckdb)
	$(COMPOSE_DUCKDB) down

duckdb-clean:  ## [duckdb] Dừng + xoá volume (mất file .duckdb)
	$(COMPOSE_DUCKDB) down -v

# =========================================================================
# Môi trường 2: Spark + Trino (lakehouse trên Iceberg/MinIO)
# =========================================================================
st-up:         ## [spark-trino] Bật toàn bộ stack (build image lần đầu)
	$(COMPOSE_ST) up -d --build

st-ps:         ## [spark-trino] Trạng thái container
	$(COMPOSE_ST) ps

st-logs:       ## [spark-trino] Xem log
	$(COMPOSE_ST) logs -f

st-down:       ## [spark-trino] Dừng stack (giữ dữ liệu)
	$(COMPOSE_ST) down

st-clean:      ## [spark-trino] Dừng + xoá volume MinIO (mất sạch dữ liệu)
	$(COMPOSE_ST) down -v

# ----- Bước 1: Ingest bằng Spark (CSV -> Iceberg bronze) -----
SPARK_SUBMIT = $(COMPOSE_ST) exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze

st-ingest: st-ingest-orders st-ingest-order-items st-ingest-customers st-ingest-geography st-ingest-products \
           st-ingest-payments st-ingest-shipments st-ingest-returns st-ingest-reviews st-ingest-promotions \
           st-ingest-inventory st-ingest-sales-daily st-ingest-web-traffic   ## [spark-trino] Ingest toàn bộ 13 bảng bronze

st-ingest-orders:        ## [spark-trino] Ingest orders
	$(SPARK_SUBMIT)/ingest_orders.py
st-ingest-order-items:   ## [spark-trino] Ingest order_items
	$(SPARK_SUBMIT)/ingest_order_items.py
st-ingest-customers:     ## [spark-trino] Ingest customers
	$(SPARK_SUBMIT)/ingest_customers.py
st-ingest-geography:     ## [spark-trino] Ingest geography
	$(SPARK_SUBMIT)/ingest_geography.py
st-ingest-products:      ## [spark-trino] Ingest products
	$(SPARK_SUBMIT)/ingest_products.py
st-ingest-payments:      ## [spark-trino] Ingest payments
	$(SPARK_SUBMIT)/ingest_payments.py
st-ingest-shipments:     ## [spark-trino] Ingest shipments
	$(SPARK_SUBMIT)/ingest_shipments.py
st-ingest-returns:       ## [spark-trino] Ingest returns
	$(SPARK_SUBMIT)/ingest_returns.py
st-ingest-reviews:       ## [spark-trino] Ingest reviews
	$(SPARK_SUBMIT)/ingest_reviews.py
st-ingest-promotions:    ## [spark-trino] Ingest promotions
	$(SPARK_SUBMIT)/ingest_promotions.py
st-ingest-inventory:     ## [spark-trino] Ingest inventory
	$(SPARK_SUBMIT)/ingest_inventory.py
st-ingest-sales-daily:   ## [spark-trino] Ingest sales_daily
	$(SPARK_SUBMIT)/ingest_sales_daily.py
st-ingest-web-traffic:   ## [spark-trino] Ingest web_traffic
	$(SPARK_SUBMIT)/ingest_web_traffic.py

st-spark-sql:  ## [spark-trino] Mở spark-sql tương tác
	$(COMPOSE_ST) exec spark /opt/spark/bin/spark-sql

# ----- Bước 2: Transform + Test bằng dbt (qua Trino) -----
st-dbt-deps:   ## [spark-trino] Cài dbt_utils (chạy 1 lần)
	$(COMPOSE_ST) exec dbt dbt deps

st-dbt:        ## [spark-trino] Build model dbt silver + gold (--target trino)
	$(COMPOSE_ST) exec dbt dbt build --target trino

st-dbt-test:   ## [spark-trino] Chỉ chạy test dữ liệu
	$(COMPOSE_ST) exec dbt dbt test --target trino

# ----- Bước 3: Truy vấn bằng Trino -----
st-trino:      ## [spark-trino] Mở Trino CLI
	$(COMPOSE_ST) exec trino trino

st-query:      ## [spark-trino] Chạy nhanh 1 query mẫu
	$(COMPOSE_ST) exec trino trino --catalog iceberg --execute \
	"SELECT * FROM analytics.gold_orders_daily ORDER BY order_date LIMIT 20;"
