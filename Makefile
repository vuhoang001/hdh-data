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
        duckdb-down duckdb-clean duckdb-ps \
        lake-up lake-down lake-clean lake-ps lake-logs lake-dbt-deps lake-dbt lake-dbt-test \
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
