{#
    Tạo sẵn namespace silver + gold trong catalog TRƯỚC khi dbt build model.

    VÌ SAO CẦN — không phải phòng xa, mà là fix một lỗi đã quan sát được:
    dbt vốn tự tạo schema đích trước khi chạy model (adapter.create_schema). Nhưng với
    catalog Iceberg REST, dbt-duckdb tạo KHÔNG ổn định: một lần build từ trạng thái sạch,
    nó tạo được `<env>_gold` và `dbt_test__audit` nhưng BỎ SÓT `<env>_silver`, khiến model
    silver đầu tiên chết với:

        Runtime Error  Catalog Error: Schema 'dev_silver' does not exist

    Trong khi CREATE SCHEMA vào Iceberg vốn chạy tốt — kể cả bên trong transaction (đã
    kiểm chứng bằng thực nghiệm). Nên cách sửa đúng là tạo tường minh, không phụ thuộc vào
    hành vi tự-tạo của adapter.

    CHẠY Ở CẢ HAI TARGET: DuckDB (dev) và Trino (prod) đều hiểu
    `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>`, nên hook này không rẽ nhánh môi
    trường — chỉ đọc tên namespace từ env, đúng như mọi chỗ khác.

    BRONZE KHÔNG nằm ở đây: nó do tầng ingestion (engines/*) tạo trước khi dbt chạy, và
    dbt chỉ đọc bronze qua source(). Hook này chỉ lo hai tầng mà dbt tự ghi.
#}
{% macro ensure_layer_schemas() %}
    {% if execute %}
        {% set catalog = env_var('ICEBERG_CATALOG_NAME', 'iceberg') %}
        {% set namespaces = [
            env_var('SILVER_NAMESPACE', 'silver'),
            env_var('GOLD_NAMESPACE', 'gold'),
        ] %}
        {% for ns in namespaces %}
            {% call statement('ensure_schema_' ~ ns, auto_begin=False) %}
                create schema if not exists {{ catalog }}.{{ ns }}
            {% endcall %}
        {% endfor %}
    {% endif %}
{% endmacro %}
