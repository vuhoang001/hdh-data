-- Bronze: data/payments.csv -> bronze.payments  (tương đương ingest_payments.py)
with source as (
    select * from {{ read_source_csv('payments.csv', {
        'order_id': 'INTEGER',
        'payment_method': 'VARCHAR',
        'payment_value': 'DOUBLE',
        'installments': 'INTEGER'
    }) }}
),

normalized as (
    select
        order_id,
        lower(trim(payment_method)) as payment_method,
        payment_value,
        installments
    from source
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["payment_value is null or payment_value <= 0", "payment_value_invalid"],
            ["installments is null or installments < 1", "installments_invalid"],
            ["payment_method not in ('apple_pay','bank_transfer','cod','credit_card','paypal')", "method_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('payments.csv') }}
from flagged
