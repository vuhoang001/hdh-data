{#
    SEAM DUY NHẤT giữa hai engine của tầng bronze.

    Mỗi file models/bronze/bronze_<bảng>.sql là NGUỒN DUY NHẤT của logic bronze cho bảng đó.
    Cả hai engine cùng chạy chính đoạn SQL ấy, chỉ khác nhau đúng một chỗ — quan hệ nguồn:

        duckdb : dbt build            -> {{ bronze_source(...) }} nở thành read_csv(...)
        spark  : ingestion/ingest.py  -> {{ bronze_source(...) }} thay bằng tên temp view

    Trước đây bronze bị cài đặt HAI LẦN (13 file .sql + 13 file ingest_*.py) và phải có
    tests/test_bronze_parity.py canh cho hai bên khỏi lệch. Giờ chỉ còn một bản nên lệch
    không còn khả năng xảy ra — không phải "được phát hiện sớm hơn" mà là không tồn tại.

    ĐỔI LẠI: mọi biểu thức trong file bronze phải là SQL portable, chạy giống nhau trên
    DuckDB lẫn Spark SQL. Các hàm đang dùng đều đạt: trim, lower, nullif, concat_ws,
    case/when, is null, not in, not between, year(), month(), current_timestamp.
    Nếu sau này cần cú pháp riêng của một engine thì hoặc viết lại cho portable, hoặc
    dùng SQLGlot để transpile — ĐỪNG tách thành hai bản cài đặt lần nữa.
#}

{#
    bronze_source(file, columns) — khai báo nguồn của một bảng bronze.

    - file    : tên file trong thư mục data (đường dẫn lấy từ var data_dir)
    - columns : dict {tên_cột: kiểu} theo ĐÚNG THỨ TỰ cột trong file.

    Kiểu dùng tên TRUNG LẬP (integer/string/date/double) chứ không phải tên riêng của DuckDB
    hay Spark, vì cùng một khai báo phải map được sang cả hai. Bảng map của DuckDB nằm ngay
    dưới; bảng map của Spark nằm ở ingestion/common/sql_model.py.

    Hậu tố '!' = cột BẮT BUỘC (not null), vd 'order_id': 'integer!'. DuckDB bỏ qua thông tin
    này vì read_csv không diễn đạt được nullability; Spark dùng nó để dựng StructField với
    nullable=False, và Iceberg ghi cột đó thành `required`. Bỏ '!' đi là âm thầm nới lỏng
    schema bảng bronze ở môi trường lakehouse.

    DuckDB áp schema theo VỊ TRÍ và bỏ qua tên ở header (header=true để nhảy dòng đầu), nên
    đổi được tên cột (vd Date -> sale_date) y hệt cách Spark làm. Mặt trái: nguồn đổi THỨ TỰ
    cột thì dữ liệu vào nhầm cột mà không có lỗi nào báo.
#}
{% macro bronze_source(file, columns) %}
    {%- set duckdb_types = {
        'integer': 'INTEGER',
        'string':  'VARCHAR',
        'date':    'DATE',
        'double':  'DOUBLE'
    } -%}

    {#- Kiểu lạ thì dừng ngay lúc compile, không đoán: sai kiểu ở bronze trôi xuống tận gold -#}
    {%- for name, dtype in columns.items() -%}
        {%- if (dtype | replace('!', '')) not in duckdb_types -%}
            {{ exceptions.raise_compiler_error(
                "bronze_source: kiểu '" ~ dtype ~ "' của cột '" ~ name ~ "' (" ~ file ~ ") "
                ~ "không hỗ trợ. Kiểu hợp lệ: " ~ (duckdb_types.keys() | list | join(', '))
                ~ " (thêm '!' vào cuối để đánh dấu cột bắt buộc)."
                ~ " Thêm kiểu mới phải sửa CẢ macro này VÀ ingestion/common/sql_model.py."
            ) }}
        {%- endif -%}
    {%- endfor -%}

    read_csv(
        '{{ var("data_dir", "/data") }}/{{ file }}',
        header = true,
        columns = {
            {%- for name, dtype in columns.items() %}
            '{{ name }}': '{{ duckdb_types[dtype | replace('!', '')] }}'{% if not loop.last %},{% endif %}
            {%- endfor %}
        }
    )
{% endmacro %}
