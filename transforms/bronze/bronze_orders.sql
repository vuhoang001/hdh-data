-- Bronze: data/orders.csv -> bronze.orders
--
-- NGUỒN DUY NHẤT của logic bronze cho bảng orders. Cả hai engine cùng chạy chính file này:
--   duckdb : dbt build           -> bronze_source() nở thành read_csv(...)
--   spark  : ingestion/ingest.py -> bronze_source() thay bằng tên temp view
--
-- Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn.
{% set source_file = 'orders.csv' %}
{% set columns = {
    'order_id': 'integer!',
    'order_date': 'date',
    'customer_id': 'integer',
    'zip': 'string',
    'order_status': 'string',
    'payment_method': 'string',
    'device_type': 'string',
    'order_source': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        order_id,
        order_date,
        customer_id,
        trim(zip)                       as zip,
        lower(trim(order_status))       as order_status,
        lower(trim(payment_method))     as payment_method,
        lower(trim(device_type))        as device_type,
        lower(trim(order_source))       as order_source
    from source
),

-- concat_ws bỏ qua NULL nên chỉ những case đúng mới xuất hiện trong chuỗi lý do.
-- Không lỗi nào -> chuỗi rỗng -> nullif trả NULL -> _is_valid = true.
flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when customer_id is null then 'customer_id_missing' end,
            case when order_date is null then 'order_date_missing' end,
            case when order_status not in ('created','paid','shipped','delivered','returned','cancelled') then 'status_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
