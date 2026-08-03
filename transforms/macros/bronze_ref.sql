{#
    bronze(name) — trỏ silver về đúng nguồn bronze theo môi trường đang chạy.

    Hai môi trường tạo bronze bằng hai cách khác nhau:
      - duckdb : bronze là các dbt model đọc thẳng CSV (models/bronze/bronze_*.sql)
      - trino  : bronze do Spark ghi vào Iceberg, dbt đọc qua source('bronze', ...)

    Nhờ macro này, MỌI model silver viết `{{ bronze('orders') }}` là chạy được ở cả hai
    engine — không phải rẽ nhánh trong từng file, không nhân đôi logic silver/gold.
#}
{% macro bronze(name) %}
    {%- if target.type == 'duckdb' -%}
        {{ ref('bronze_' ~ name) }}
    {%- else -%}
        {{ source('bronze', name) }}
    {%- endif -%}
{% endmacro %}
