-- Bronze: data/order_items.csv -> bronze.order_items
-- Nguồn duy nhất của logic bronze cho bảng order_items (xem macros/bronze_helpers.sql).
{% set source_file = 'order_items.csv' %}
{% set columns = {
    'order_id': 'integer!',
    'product_id': 'integer',
    'quantity': 'integer',
    'unit_price': 'double',
    'discount_amount': 'double',
    'promo_id': 'string',
    'promo_id_2': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        -- Chuỗi rỗng -> NULL để chỉ có một cách biểu diễn "không có khuyến mãi"
        nullif(trim(promo_id), '')   as promo_id,
        nullif(trim(promo_id_2), '') as promo_id_2
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when product_id is null then 'product_id_missing' end,
            case when quantity is null or quantity <= 0 then 'quantity_invalid' end,
            case when unit_price is null or unit_price < 0 then 'unit_price_invalid' end,
            case when discount_amount < 0 then 'discount_negative' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
