# =============================================================================
# hdh-data — MỘT pipeline, HAI execution engine.
#
#   ENV=dev   (mặc định)   dbt ──► DuckDB ──► Iceberg ──► MinIO
#   ENV=prod               dbt ──► Trino  ──► Iceberg ──► MinIO
#
# Khác nhau ĐÚNG ở: execution engine, namespace, lượng dữ liệu.
# Giống nhau: model dbt, business logic, data contract, test.
#
# Luồng đầy đủ:
#   make setup && make up && make pipeline && make test
#   make ENV=prod up && make ENV=prod pipeline
#
# Mọi target đều nhận ENV=, ví dụ:  make ENV=prod dbt-build
# =============================================================================

.DEFAULT_GOAL := help

# ---- Môi trường --------------------------------------------------------------
ENV ?= dev

# Thứ tự nạp quyết định cái nào thắng: .env.local nạp SAU CÙNG nên override được
# mọi thứ. `-include` (không phải `include`) để `make help` chạy được khi thiếu file.
-include config/.env.shared
-include config/.env.$(ENV)
-include config/.env.local

# Container chạy bằng UID/GID của bạn, không phải root. Không có dòng này thì mọi file
# dbt sinh ra trên bind mount (target/, logs/, dbt_packages/) sẽ thuộc root:root —
# IDE không sửa được và chính bạn cũng không `rm` được nếu không có sudo.
DOCKER_USER := $(shell id -u):$(shell id -g)

export

# ---- docker compose ----------------------------------------------------------
# --project-directory . để mọi đường dẫn trong compose tính từ gốc repo.
# --env-file tường minh: KHÔNG để compose tự nạp .env ở gốc repo, vì file đó là
# cấu hình CŨ và sẽ âm thầm ghi đè namespace của môi trường đang chọn.
LOCAL_ENV := $(if $(wildcard config/.env.local),--env-file config/.env.local,)

COMPOSE = docker compose --project-directory . \
	--env-file config/.env.shared --env-file config/.env.$(ENV) $(LOCAL_ENV) \
	-f infra/compose.base.yml -f infra/compose.$(ENV).yml

# ---- Engine ingestion đổi theo môi trường ------------------------------------
# ĐÂY là chỗ DUY NHẤT trong repo mà môi trường quyết định chương trình nào chạy —
# và nó chỉ chọn RUNTIME, không chọn logic: cả hai engine chạy chính
# transforms/models/bronze/bronze_<bảng>.sql.
ifeq ($(INGESTION_ENGINE),spark)
  INGEST_RUN = $(COMPOSE) exec -T spark /opt/spark/bin/spark-submit $(SPARK_JOBS_DIR)/ingest.py
else
  INGEST_RUN = $(COMPOSE) exec -T dbt python $(INGEST_JOBS_DIR)/ingest.py
endif

# Loader landing chạy trong container `dbt` ở CẢ HAI môi trường, KHÔNG theo
# INGESTION_ENGINE. Nó là job đổi định dạng chạy trên một máy (nguồn -> Parquet), không
# phải execution engine của pipeline — và image Spark cố tình không cài duckdb.
LANDING_RUN = $(COMPOSE) exec -T dbt python $(INGEST_JOBS_DIR)/load_landing.py

DBT_RUN = $(COMPOSE) exec -T dbt dbt

# Danh sách bảng bronze đọc TRỰC TIẾP từ sources.yml — không lặp lại ở đây.
SOURCES_FILE   = ingestion/config/sources.yml
BRONZE_TABLES := $(shell sed -n 's/^[[:space:]]*-[[:space:]]*table:[[:space:]]*//p' $(SOURCES_FILE) 2>/dev/null)
INGEST_TARGETS = $(addprefix ingest-,$(BRONZE_TABLES))

# CHÚ Ý: KHÔNG khai $(INGEST_TARGETS) là .PHONY. GNU make bỏ qua việc tìm pattern rule
# cho target phony, nên `make ingest-orders` sẽ báo "Nothing to be done".
.PHONY: help setup env-check doctor up down clean ps logs \
        landing ingest ingest-list \
        dbt-deps dbt-run dbt-test dbt-build dbt-unit \
        pipeline test test-repo query shell trino-cli spark-sql ci-local

# =========================================================================
# Trợ giúp
# =========================================================================
help:          ## Danh sách lệnh
	@echo "  Môi trường hiện tại: ENV=$(ENV)  (engine: dbt->$(DBT_TARGET), ingest->$(INGESTION_ENGINE))"
	@echo "  Đổi bằng:  make ENV=prod <target>"
	@echo ""
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Ingest từng bảng: make ingest-<bảng>"
	@echo "  Bảng khai ở $(SOURCES_FILE):"
	@echo "    $(BRONZE_TABLES)" | fold -s -w 76 | sed 's/^/    /'

setup:         ## Khởi tạo lần đầu (tạo .env.local, build image, cài dbt deps)
	@test -f config/.env.local || { \
	  cp config/.env.local.example config/.env.local; \
	  echo ">> Đã tạo config/.env.local — sửa secret trong đó nếu cần"; }
	$(MAKE) ENV=$(ENV) up
	$(MAKE) ENV=$(ENV) dbt-deps
	@echo ">> Sẵn sàng. Chạy: make pipeline"

env-check:     ## In cấu hình đang có hiệu lực
	@echo "ENV=$(ENV)  ENVIRONMENT=$(ENVIRONMENT)"
	@echo "  dbt target      : $(DBT_TARGET)"
	@echo "  ingestion engine: $(INGESTION_ENGINE)"
	@echo "  catalog         : $(ICEBERG_CATALOG_NAME) @ $(ICEBERG_REST_URI)"
	@echo "  bucket          : $(WAREHOUSE_BUCKET)  (landing: $(LANDING_PREFIX)/)"
	@echo "  namespace       : $(BRONZE_NAMESPACE) / $(SILVER_NAMESPACE) / $(GOLD_NAMESPACE)"
	@echo "  minio endpoint  : $(MINIO_ENDPOINT)"
	@echo "  sample          : $(SAMPLE_ENABLED)"

doctor:        ## Kiểm tra hạ tầng + cấu hình + kết nối
	@bash scripts/doctor.sh

# =========================================================================
# Vòng đời hạ tầng
# =========================================================================
up:            ## Bật stack của môi trường đang chọn
	$(COMPOSE) up -d --build --remove-orphans

down:          ## Dừng stack (GIỮ dữ liệu)
	$(COMPOSE) down --remove-orphans

clean:         ## Dừng + XOÁ volume (mất sạch dữ liệu MinIO + catalog)
	$(COMPOSE) down -v --remove-orphans

ps:            ## Trạng thái container
	$(COMPOSE) ps

logs:          ## Xem log
	$(COMPOSE) logs -f

# =========================================================================
# Pipeline:  nguồn -> landing -> bronze -> silver -> gold
# =========================================================================
landing:       ## B1. Đẩy nguồn lên landing zone (Parquet trên MinIO)
	$(LANDING_RUN)

ingest: $(INGEST_TARGETS)   ## B2. Landing -> bronze Iceberg (toàn bộ bảng)

# MỘT script chung cho mọi bảng và mọi engine. ingest.py đọc chính
# transforms/models/bronze/bronze_<bảng>.sql (cùng file dbt dùng) rồi chạy nó.
ingest-%:
	$(INGEST_RUN) --table $*

ingest-list:   ## Liệt kê các bảng bronze đã khai trong sources.yml
	$(INGEST_RUN) --list

dbt-deps:      ## Cài dbt_utils (chạy 1 lần)
	$(DBT_RUN) deps

dbt-run:       ## B3. Chỉ build model (không test)
	$(DBT_RUN) run --target $(DBT_TARGET)

dbt-build:     ## B3. Build silver + gold VÀ chạy test sau mỗi model
	$(DBT_RUN) build --target $(DBT_TARGET)

dbt-test:      ## Chỉ chạy test dữ liệu
	$(DBT_RUN) test --target $(DBT_TARGET)

# Unit test kiểm CÔNG THỨC SQL bằng dữ liệu bịa (models/gold/_unit_tests.yml),
# không đụng dữ liệu thật. Chạy trong vài giây nên dùng được ngay lúc đang sửa model.
dbt-unit:      ## Chỉ chạy unit test (nhanh, không cần dữ liệu)
	$(DBT_RUN) test --target $(DBT_TARGET) --select "test_type:unit"

pipeline:      ## Chạy TRỌN pipeline: landing -> bronze -> silver -> gold + test
	$(MAKE) ENV=$(ENV) landing
	$(MAKE) ENV=$(ENV) ingest
	$(MAKE) ENV=$(ENV) dbt-build

# =========================================================================
# Test
# =========================================================================
# Ưu tiên .venv của repo, không thì rơi về python3 hệ thống. `pytest` trần hay thất bại
# vì nó chỉ có trên PATH khi virtualenv đang được activate.
PYTEST := $(shell if [ -x .venv/bin/pytest ]; then echo .venv/bin/pytest; else echo "python3 -m pytest"; fi)

test-repo:     ## Test cấu trúc repo (pytest, không cần Docker)
	$(PYTEST) tests/unit -q

test:          ## Toàn bộ test: cấu trúc repo + dữ liệu
	$(MAKE) test-repo
	$(MAKE) ENV=$(ENV) dbt-test

# =========================================================================
# Truy vấn / shell
# =========================================================================
query:         ## Xem thử một bảng gold
	$(DBT_RUN) show --target $(DBT_TARGET) --limit 20 \
	  --inline "select * from {{ target.schema }}.gold_orders_daily order by order_date"

shell:         ## Mở shell trong container runner
	$(COMPOSE) exec dbt bash

trino-cli:     ## [prod] Mở Trino CLI
	$(COMPOSE) exec trino trino

spark-sql:     ## [prod] Mở spark-sql tương tác
	$(COMPOSE) exec spark /opt/spark/bin/spark-sql

# =========================================================================
# CI — chuỗi mà .github/workflows/ci.yml chạy
# =========================================================================
ci-local:      ## Chạy đúng chuỗi CI (môi trường dev)
	$(MAKE) ENV=dev up
	$(MAKE) ENV=dev dbt-deps
	$(MAKE) ENV=dev pipeline
	$(MAKE) test-repo
	$(MAKE) ENV=dev down
