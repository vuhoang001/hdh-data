-- Bronze: data/returns.csv -> bronze.returns  (tương đương ingest_returns.py)
with source as (
    select * from {{ read_source_csv('returns.csv', {
        'return_id': 'VARCHAR',
        'order_id': 'INTEGER',
        'product_id': 'INTEGER',
        'return_date': 'DATE',
        'return_reason': 'VARCHAR',
        'return_quantity': 'INTEGER',
        'refund_amount': 'DOUBLE'
    }) }}
),

normalized as (
    select
        trim(return_id)             as return_id,
        order_id,
        product_id,
        return_date,
        lower(trim(return_reason))  as return_reason,
        return_quantity,
        refund_amount
    from source
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["order_id is null", "order_id_missing"],
            ["product_id is null", "product_id_missing"],
            ["return_date is null", "return_date_missing"],
            ["return_quantity is null or return_quantity <= 0", "return_quantity_invalid"],
            ["refund_amount is null or refund_amount < 0", "refund_amount_invalid"],
            ["return_reason not in ('changed_mind','defective','late_delivery','not_as_described','wrong_size')", "reason_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('returns.csv') }}
from flagged
