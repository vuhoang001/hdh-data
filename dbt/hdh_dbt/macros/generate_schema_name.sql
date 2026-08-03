{#
    Dùng thẳng tên schema khai báo trong config, KHÔNG ghép tiền tố target.

    Mặc định dbt sinh schema = <target.schema>_<custom> (vd 'analytics_bronze'). Ở đây ta
    muốn tên namespace sạch đúng như đặt: bronze -> 'bronze', silver/gold (không đặt custom)
    -> 'analytics'. Nhờ đó layout schema ở DuckDB khớp với namespace ở Trino/Iceberg.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
