-- Bronze: data/shipments.csv -> bronze.shipments
-- Nguồn duy nhất của logic bronze cho bảng shipments (xem macros/bronze_helpers.sql).
-- Bảng không có cột text nào cần chuẩn hoá — chỉ gắn cờ chất lượng.
{% set source_file = 'shipments.csv' %}
{% set columns = {
    'order_id': 'integer!',
    'ship_date': 'date',
    'delivery_date': 'date',
    'shipping_fee': 'double'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when ship_date is null then 'ship_date_missing' end,
            -- Giao trước khi gửi là bất khả thi về mặt vật lý -> chắc chắn lỗi dữ liệu
            case when delivery_date < ship_date then 'delivery_before_ship' end,
            case when shipping_fee is null or shipping_fee < 0 then 'shipping_fee_invalid' end
        ), '') as _invalid_reason
    from source
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
