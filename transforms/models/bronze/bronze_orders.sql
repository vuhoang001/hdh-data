-- Bronze: data/orders.csv -> bronze.orders  (bản DuckDB, tương đương ingest_orders.py của Spark)
-- Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn.
with source as (
    select * from {{ read_source_csv('orders.csv', {
        'order_id': 'INTEGER',
        'order_date': 'DATE',
        'customer_id': 'INTEGER',
        'zip': 'VARCHAR',
        'order_status': 'VARCHAR',
        'payment_method': 'VARCHAR',
        'device_type': 'VARCHAR',
        'order_source': 'VARCHAR'
    }) }}
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

flagged as (
    select
        *,
        {{ invalid_reason([
            ["customer_id is null", "customer_id_missing"],
            ["order_date is null", "order_date_missing"],
            ["order_status not in ('created','paid','shipped','delivered','returned','cancelled')", "status_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('orders.csv') }}
from flagged
