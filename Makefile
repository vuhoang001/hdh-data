# Các lệnh tắt cho pipeline hdh-data
# Dùng:  make up  ->  make ingest  ->  make dbt  ->  make query

.PHONY: up down logs ps build ingest spark-sql trino dbt dbt-test dbt-deps query clean

up:            ## Khởi động toàn bộ stack (build image lần đầu)
	docker compose up -d --build

down:          ## Dừng stack (giữ dữ liệu)
	docker compose down

clean:         ## Dừng stack + xoá volume (mất sạch dữ liệu MinIO)
	docker compose down -v

ps:            ## Xem trạng thái container
	docker compose ps

logs:          ## Xem log
	docker compose logs -f

# ----- Bước 1: Ingest bằng Spark (CSV -> Iceberg bronze.orders) -----
ingest:
	docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_orders.py

spark-sql:     ## Mở spark-sql tương tác
	docker compose exec spark /opt/spark/bin/spark-sql

# ----- Bước 2: Transform + Test bằng dbt (qua Trino) -----
dbt-deps:
	docker compose exec dbt dbt deps

dbt:           ## Build model dbt (silver + gold)
	docker compose exec dbt dbt build

dbt-test:      ## Chỉ chạy test dữ liệu
	docker compose exec dbt dbt test

# ----- Bước 3: Truy vấn bằng Trino -----
trino:         ## Mở Trino CLI
	docker compose exec trino trino

query:         ## Chạy nhanh 1 câu query mẫu
	docker compose exec trino trino --catalog iceberg --execute \
	"SELECT * FROM analytics.gold_orders_daily ORDER BY order_date LIMIT 20;"
