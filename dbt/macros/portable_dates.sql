{#
    Lớp hàm ngày tháng portable giữa Trino và DuckDB.

    Hai engine dùng tên hàm khác nhau cho cùng một phép tính. Gom sự khác biệt về đây,
    model chỉ gọi macro nên viết một lần chạy cả hai. Chỉ những hàm THỰC SỰ lệch nhau mới
    nằm ở đây — year()/quarter()/month()/day()/date_diff() giống nhau nên dùng thẳng.
#}

{# 'YYYY-MM' — ví dụ '2022-01' #}
{% macro ym(col) %}
    {%- if target.type == 'duckdb' -%}
        strftime({{ col }}, '%Y-%m')
    {%- else -%}
        date_format({{ col }}, '%Y-%m')
    {%- endif -%}
{% endmacro %}

{# Tên tháng đầy đủ — 'January' #}
{% macro month_name(col) %}
    {%- if target.type == 'duckdb' -%}
        strftime({{ col }}, '%B')
    {%- else -%}
        date_format({{ col }}, '%M')
    {%- endif -%}
{% endmacro %}

{# Tên thứ đầy đủ — 'Monday' #}
{% macro day_name(col) %}
    {%- if target.type == 'duckdb' -%}
        strftime({{ col }}, '%A')
    {%- else -%}
        date_format({{ col }}, '%W')
    {%- endif -%}
{% endmacro %}

{# Thứ trong tuần theo ISO: 1 = thứ 2 ... 7 = chủ nhật (thống nhất cả hai engine) #}
{% macro dow(col) %}
    {%- if target.type == 'duckdb' -%}
        isodow({{ col }})
    {%- else -%}
        day_of_week({{ col }})
    {%- endif -%}
{% endmacro %}

{# Số tuần trong năm #}
{% macro week_of_year(col) %}
    {%- if target.type == 'duckdb' -%}
        week({{ col }})
    {%- else -%}
        week_of_year({{ col }})
    {%- endif -%}
{% endmacro %}
