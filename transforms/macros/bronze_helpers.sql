{#
    Hạ tầng dùng chung cho các bronze model của môi trường DuckDB.

    Bronze ở đây làm đúng việc Spark làm ở môi trường Trino: đọc CSV, chuẩn hoá text,
    gắn cờ chất lượng, GIỮ NGUYÊN số dòng nguồn (không lọc). Chỉ khác engine thực thi.
#}

{#
    read_source_csv(file, columns) — đọc 1 CSV nguồn với schema tường minh.

    - columns: dict {tên_cột: kiểu} theo ĐÚNG THỨ TỰ cột trong file. DuckDB áp schema theo
      vị trí và bỏ qua tên ở header (header=true để nhảy dòng đầu), nên đổi được tên cột
      (vd Date -> sale_date) y hệt cách Spark làm.
    - đường dẫn lấy từ var data_dir (mặc định /data, mount từ ./data vào container).
#}
{% macro read_source_csv(file, columns) %}
    read_csv(
        '{{ var("data_dir", "/data") }}/{{ file }}',
        header = true,
        columns = {
            {%- for name, dtype in columns.items() %}
            '{{ name }}': '{{ dtype }}'{% if not loop.last %},{% endif %}
            {%- endfor %}
        }
    )
{% endmacro %}

{#
    invalid_reason(checks) — dựng cột _invalid_reason từ danh sách [điều_kiện, nhãn].

    Gộp mọi lý do lỗi thành 1 chuỗi ngăn cách bởu ', '; concat_ws bỏ qua NULL nên chỉ
    những check đúng mới xuất hiện. Không có lỗi nào -> nullif trả NULL -> _is_valid = true.
    Tương đương F.concat_ws + F.when trong các job Spark, giữ nguyên tên nhãn để truy vết.
#}
{% macro invalid_reason(checks) %}
    nullif(concat_ws(', '
        {%- for cond, label in checks %},
        case when {{ cond }} then '{{ label }}' end
        {%- endfor %}
    ), '')
{% endmacro %}

{# Cột audit kỹ thuật gắn cho mọi bảng bronze (khớp add_audit_columns của Spark) #}
{% macro bronze_audit(file) %}
    '{{ file }}' as _source_file,
    current_timestamp as _ingested_at
{% endmacro %}
