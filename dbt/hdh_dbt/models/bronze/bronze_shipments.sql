-- Bronze: data/shipments.csv -> bronze.shipments  (tương đương ingest_shipments.py)
-- Bảng không có cột text nào cần chuẩn hoá — chỉ gắn cờ chất lượng.
with source as (
    select * from {{ read_source_csv('shipments.csv', {
        'order_id': 'INTEGER',
        'ship_date': 'DATE',
        'delivery_date': 'DATE',
        'shipping_fee': 'DOUBLE'
    }) }}
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["ship_date is null", "ship_date_missing"],
            ["delivery_date < ship_date", "delivery_before_ship"],
            ["shipping_fee is null or shipping_fee < 0", "shipping_fee_invalid"]
        ]) }} as _invalid_reason
    from source
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('shipments.csv') }}
from flagged
