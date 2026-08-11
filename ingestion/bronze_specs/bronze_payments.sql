-- Bronze: data/payments.csv -> bronze.payments
-- Nguồn duy nhất của logic bronze cho bảng payments (xem macros/bronze_helpers.sql).
{% set source_file = 'payments.csv' %}
{% set columns = {
    'order_id': 'integer!',
    'payment_method': 'string',
    'payment_value': 'double',
    'installments': 'integer'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
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
        nullif(concat_ws(', ',
            case when payment_value is null or payment_value <= 0 then 'payment_value_invalid' end,
            case when installments is null or installments < 1 then 'installments_invalid' end,
            case when payment_method not in ('apple_pay','bank_transfer','cod','credit_card','paypal') then 'method_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
