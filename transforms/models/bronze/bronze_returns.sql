-- Bronze: data/returns.csv -> bronze.returns
-- Nguồn duy nhất của logic bronze cho bảng returns (xem macros/bronze_helpers.sql).
{% set source_file = 'returns.csv' %}
{% set columns = {
    'return_id': 'string!',
    'order_id': 'integer',
    'product_id': 'integer',
    'return_date': 'date',
    'return_reason': 'string',
    'return_quantity': 'integer',
    'refund_amount': 'double'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
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
        nullif(concat_ws(', ',
            case when order_id is null then 'order_id_missing' end,
            case when product_id is null then 'product_id_missing' end,
            case when return_date is null then 'return_date_missing' end,
            case when return_quantity is null or return_quantity <= 0 then 'return_quantity_invalid' end,
            case when refund_amount is null or refund_amount < 0 then 'refund_amount_invalid' end,
            case when return_reason not in ('changed_mind','defective','late_delivery','not_as_described','wrong_size') then 'reason_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
