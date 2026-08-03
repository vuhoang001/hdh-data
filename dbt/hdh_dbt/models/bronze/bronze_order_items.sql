-- Bronze: data/order_items.csv -> bronze.order_items  (tương đương ingest_order_items.py)
with source as (
    select * from {{ read_source_csv('order_items.csv', {
        'order_id': 'INTEGER',
        'product_id': 'INTEGER',
        'quantity': 'INTEGER',
        'unit_price': 'DOUBLE',
        'discount_amount': 'DOUBLE',
        'promo_id': 'VARCHAR',
        'promo_id_2': 'VARCHAR'
    }) }}
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
        {{ invalid_reason([
            ["product_id is null", "product_id_missing"],
            ["quantity is null or quantity <= 0", "quantity_invalid"],
            ["unit_price is null or unit_price < 0", "unit_price_invalid"],
            ["discount_amount < 0", "discount_negative"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('order_items.csv') }}
from flagged
