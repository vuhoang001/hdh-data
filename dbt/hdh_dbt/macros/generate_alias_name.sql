{#
    Bỏ tiền tố 'bronze_' khỏi TÊN BẢNG (không phải tên file model).

    Model để tên file bronze_orders (rõ ràng trong models/bronze/), nhưng bảng sinh ra nên là
    bronze.orders — khớp đúng namespace Spark ghi ở môi trường Trino (iceberg.bronze.orders).
    Nhờ đó câu query `select * from bronze.orders` chạy y hệt ở cả hai môi trường.

    Chỉ bronze_* bị cắt tiền tố; silver_*/gold_* và dim_/fact_ giữ nguyên tên.
#}
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node is not none and node.name.startswith('bronze_') -%}
        {{ node.name[7:] }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
