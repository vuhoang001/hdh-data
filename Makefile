# Các lệnh tắt cho pipeline hdh-data
# Dùng:  make up  ->  make ingest  ->  make dbt  ->  make query

.PHONY: up down logs ps build clean spark-sql trino dbt dbt-test dbt-deps query \
        ingest ingest-orders ingest-order-items ingest-customers ingest-geography \
        ingest-products ingest-payments ingest-shipments ingest-returns ingest-reviews \
        ingest-promotions ingest-inventory ingest-sales-daily ingest-web-traffic

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

# ----- Bước 1: Ingest bằng Spark (CSV -> Iceberg bronze) -----
# Mỗi bảng một target riêng để ingest lại 1 bảng mà không phải chạy lại tất cả.
# SPARK_SUBMIT gom phần lặp; đổi cấu hình spark-submit thì sửa 1 chỗ.
SPARK_SUBMIT = docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze

ingest: ingest-orders ingest-order-items ingest-customers ingest-geography ingest-products \
        ingest-payments ingest-shipments ingest-returns ingest-reviews ingest-promotions \
        ingest-inventory ingest-sales-daily ingest-web-traffic   ## Ingest toàn bộ 13 bảng bronze

ingest-orders:        ## Chỉ ingest bảng orders
	$(SPARK_SUBMIT)/ingest_orders.py

ingest-order-items:   ## Chỉ ingest bảng order_items
	$(SPARK_SUBMIT)/ingest_order_items.py

ingest-customers:     ## Chỉ ingest bảng customers
	$(SPARK_SUBMIT)/ingest_customers.py

ingest-geography:     ## Chỉ ingest bảng geography
	$(SPARK_SUBMIT)/ingest_geography.py

ingest-products:      ## Chỉ ingest bảng products
	$(SPARK_SUBMIT)/ingest_products.py

ingest-payments:      ## Chỉ ingest bảng payments
	$(SPARK_SUBMIT)/ingest_payments.py

ingest-shipments:     ## Chỉ ingest bảng shipments
	$(SPARK_SUBMIT)/ingest_shipments.py

ingest-returns:       ## Chỉ ingest bảng returns
	$(SPARK_SUBMIT)/ingest_returns.py

ingest-reviews:       ## Chỉ ingest bảng reviews
	$(SPARK_SUBMIT)/ingest_reviews.py

ingest-promotions:    ## Chỉ ingest bảng promotions
	$(SPARK_SUBMIT)/ingest_promotions.py

ingest-inventory:     ## Chỉ ingest bảng inventory
	$(SPARK_SUBMIT)/ingest_inventory.py

ingest-sales-daily:   ## Chỉ ingest bảng sales_daily (doanh thu ngày tổng hợp sẵn)
	$(SPARK_SUBMIT)/ingest_sales_daily.py

ingest-web-traffic:   ## Chỉ ingest bảng web_traffic
	$(SPARK_SUBMIT)/ingest_web_traffic.py

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
