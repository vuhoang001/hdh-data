# =============================================================================
# hdh-data — HAI môi trường độc lập, chọn theo tiền tố target:
#
#   duckdb-*  : môi trường NHẸ      (CSV -> DuckDB, chỉ cần Docker, chạy vài giây)
#   lake-*    : môi trường LAKEHOUSE (MinIO+Iceberg+Spark+Trino, giống production)
#
# Luồng nhanh:
#   DuckDB    : make duckdb-up && make duckdb-deps && make duckdb-run && make duckdb-query
#   Lakehouse : make lake-up && make lake-ingest && make lake-dbt-deps && make lake-dbt && make lake-query
# =============================================================================

.DEFAULT_GOAL := help

# .env là nguồn sự thật duy nhất cho cấu hình. Makefile nạp nó để dùng chung các
# đường dẫn container với docker compose — không định nghĩa lại ở đây.
# `-include` để `make help` vẫn chạy được khi chưa có .env.
-include .env

# Container dbt chạy bằng UID/GID của bạn, không phải root. Không có dòng này thì mọi file
# dbt sinh ra trên bind mount (target/, logs/, dbt_packages/, package-lock.yml) sẽ thuộc
# root:root — IDE không sửa được và chính bạn cũng không `rm` được nếu không có sudo.
DOCKER_USER := $(shell id -u):$(shell id -g)

export

# --project-directory . để mọi đường dẫn trong compose tính từ gốc repo và .env được nạp.
COMPOSE_DUCKDB = docker compose --project-directory . -f infra/local/compose.duckdb.yml
COMPOSE_LAKE   = docker compose --project-directory . -f infra/local/compose.lakehouse.yml

# Danh sách bảng bronze đọc TRỰC TIẾP từ ingestion/config/sources.yml — không lặp lại
# ở đây. Thêm bảng mới chỉ cần khai báo trong sources.yml + tạo file connector.
SOURCES_FILE   = ingestion/config/sources.yml
BRONZE_TABLES := $(shell sed -n 's/^[[:space:]]*-[[:space:]]*table:[[:space:]]*//p' $(SOURCES_FILE) 2>/dev/null)
INGEST_TARGETS = $(addprefix lake-ingest-,$(BRONZE_TABLES))

# CHÚ Ý: KHÔNG khai báo $(INGEST_TARGETS) là .PHONY. GNU make bỏ qua việc tìm pattern rule
# cho target phony, nên `make lake-ingest-orders` sẽ báo "Nothing to be done" thay vì chạy.
# Các target này không trùng tên file nào nên vẫn luôn được chạy lại.
.PHONY: help env-check \
        duckdb-up duckdb-deps duckdb-run duckdb-test duckdb-query duckdb-shell \
        duckdb-test-unit duckdb-test-data duckdb-test-store duckdb-test-failures \
        duckdb-down duckdb-clean duckdb-ps \
        lake-up lake-down lake-clean lake-ps lake-logs lake-dbt-deps lake-dbt lake-dbt-test \
        lake-freshness \
        lake-trino lake-spark-sql lake-query lake-ingest \
        ci-local

help:          ## Danh sách lệnh
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Ingest từng bảng: make lake-ingest-<bảng>"
	@echo "  Bảng khai báo ở $(SOURCES_FILE):"
	@echo "    $(BRONZE_TABLES)" | fold -s -w 76 | sed 's/^/    /'

env-check:     ## Kiểm tra .env đã sẵn sàng
	@test -f .env || { echo "Thiếu .env — chạy: cp .env.example .env"; exit 1; }
	@echo ".env OK  (catalog=$(ICEBERG_CATALOG_NAME), bucket=$(WAREHOUSE_BUCKET), analytics=$(ANALYTICS_SCHEMA))"

# =========================================================================
# Môi trường 1: DuckDB — bronze+silver+gold chạy trọn trong dbt-duckdb
# =========================================================================
duckdb-up:     ## [duckdb] Bật container dbt-duckdb (build lần đầu)
	$(COMPOSE_DUCKDB) up -d --build

duckdb-deps:   ## [duckdb] Cài dbt_utils (chạy 1 lần)
	$(COMPOSE_DUCKDB) exec dbt dbt deps

duckdb-run:    ## [duckdb] Build cả pipeline: bronze -> silver -> gold + test
	$(COMPOSE_DUCKDB) exec dbt dbt build --target duckdb

duckdb-test:   ## [duckdb] Chỉ chạy test dữ liệu
	$(COMPOSE_DUCKDB) exec dbt dbt test --target duckdb

# Unit test kiểm CÔNG THỨC SQL bằng dữ liệu bịa (models/gold/_unit_tests.yml), không đụng
# tới dữ liệu thật. Chạy trong 2 giây nên dùng được ngay trong lúc sửa model, thay vì phải
# build lại 714k dòng mới biết công thức đúng hay sai.
duckdb-test-unit:  ## [duckdb] Chỉ chạy unit test (nhanh, không cần dữ liệu)
	$(COMPOSE_DUCKDB) exec dbt dbt test --target duckdb --select "test_type:unit"

# Chỉ chạy các data test (loại trừ unit test) — dùng khi muốn kiểm dữ liệu thật.
duckdb-test-data:  ## [duckdb] Chỉ chạy data test trên dữ liệu thật
	$(COMPOSE_DUCKDB) exec dbt dbt test --target duckdb --exclude "test_type:unit"

# --store-failures ghi CÁC DÒNG SAI của mọi test ra bảng thay vì chỉ đếm trong log.
# Dùng khi đang điều tra: chạy xong thì mở bảng lên xem đúng dòng nào hỏng, không phải
# đi tìm lại câu SQL của test trong target/compiled/.
duckdb-test-store: ## [duckdb] Chạy test + ghi các dòng fail ra bảng để điều tra
	$(COMPOSE_DUCKDB) exec dbt dbt test --target duckdb --store-failures

duckdb-test-failures: ## [duckdb] Liệt kê các bảng chứa dòng fail đã lưu
	$(COMPOSE_DUCKDB) exec dbt python -c \
	  "import duckdb; duckdb.connect('$(DUCKDB_PATH)').sql(\"select table_name, estimated_size as rows from duckdb_tables() where schema_name like '%dbt_test__audit'\").show()"

duckdb-query:  ## [duckdb] Xem thử 1 bảng gold
	$(COMPOSE_DUCKDB) exec dbt dbt show --target duckdb --limit 20 \
	  --inline "select * from $(ANALYTICS_SCHEMA).gold_orders_daily order by order_date"

duckdb-shell:  ## [duckdb] Mở DuckDB CLI trên file warehouse
	$(COMPOSE_DUCKDB) exec dbt python -c \
	  "import duckdb; duckdb.connect('$(DUCKDB_PATH)').sql('show all tables').show()"

duckdb-ps:     ## [duckdb] Trạng thái container
	$(COMPOSE_DUCKDB) ps

duckdb-down:   ## [duckdb] Dừng (giữ file .duckdb)
	$(COMPOSE_DUCKDB) down

duckdb-clean:  ## [duckdb] Dừng + xoá volume (mất file .duckdb)
	$(COMPOSE_DUCKDB) down -v

# =========================================================================
# Môi trường 2: Lakehouse (MinIO + Iceberg + Spark + Trino)
# =========================================================================
lake-up:       ## [lakehouse] Bật toàn bộ stack (build image lần đầu)
	$(COMPOSE_LAKE) up -d --build

lake-ps:       ## [lakehouse] Trạng thái container
	$(COMPOSE_LAKE) ps

lake-logs:     ## [lakehouse] Xem log
	$(COMPOSE_LAKE) logs -f

lake-down:     ## [lakehouse] Dừng stack (giữ dữ liệu)
	$(COMPOSE_LAKE) down

lake-clean:    ## [lakehouse] Dừng + xoá volume MinIO (mất sạch dữ liệu)
	$(COMPOSE_LAKE) down -v

# ----- Bước 1: Ingest bằng Spark (CSV -> Iceberg bronze) -----
SPARK_SUBMIT = $(COMPOSE_LAKE) exec spark /opt/spark/bin/spark-submit $(SPARK_JOBS_DIR)/connectors

lake-ingest: $(INGEST_TARGETS)   ## [lakehouse] Ingest toàn bộ bảng bronze

# Một pattern rule thay cho 13 target lặp lại:
#   make lake-ingest-orders -> spark-submit .../connectors/ingest_orders.py
lake-ingest-%:
	$(SPARK_SUBMIT)/ingest_$*.py

lake-spark-sql: ## [lakehouse] Mở spark-sql tương tác
	$(COMPOSE_LAKE) exec spark /opt/spark/bin/spark-sql

# ----- Bước 2: Transform + Test bằng dbt (qua Trino) -----
lake-dbt-deps: ## [lakehouse] Cài dbt_utils (chạy 1 lần)
	$(COMPOSE_LAKE) exec dbt dbt deps

lake-dbt:      ## [lakehouse] Build model dbt silver + gold (--target trino)
	$(COMPOSE_LAKE) exec dbt dbt build --target trino

lake-dbt-test: ## [lakehouse] Chỉ chạy test dữ liệu
	$(COMPOSE_LAKE) exec dbt dbt test --target trino

# source freshness KHÔNG nằm trong `dbt test` — nó là lệnh riêng, và chỉ chạy được ở đây
# vì ở môi trường DuckDB không có source nào (bronze là model chứ không phải source).
# Nó đọc max(_ingested_at) của từng bảng bronze và so với ngưỡng ở models/silver/_sources.yml.
# Đây là thứ duy nhất phát hiện được "job Spark chết âm thầm": dữ liệu cũ vẫn hợp lệ nên
# mọi test khác vẫn xanh, chỉ là không còn dữ liệu mới chảy vào.
lake-freshness: ## [lakehouse] Kiểm độ tươi của các bảng bronze (source freshness)
	$(COMPOSE_LAKE) exec dbt dbt source freshness --target trino

# ----- Bước 3: Truy vấn bằng Trino -----
lake-trino:    ## [lakehouse] Mở Trino CLI
	$(COMPOSE_LAKE) exec trino trino

lake-query:    ## [lakehouse] Chạy nhanh 1 query mẫu
	$(COMPOSE_LAKE) exec trino trino --catalog $(ICEBERG_CATALOG_NAME) --execute \
	"SELECT * FROM $(ANALYTICS_SCHEMA).gold_orders_daily ORDER BY order_date LIMIT 20;"

# =========================================================================
# CI — chạy đúng chuỗi mà .github/workflows/ci.yml chạy
# =========================================================================
ci-local:      ## Chạy toàn bộ pipeline DuckDB + test (dùng cho CI và kiểm tra nhanh)
	$(COMPOSE_DUCKDB) up -d --build
	$(COMPOSE_DUCKDB) exec -T dbt dbt deps
	$(COMPOSE_DUCKDB) exec -T dbt dbt build --target duckdb
	$(COMPOSE_DUCKDB) down
