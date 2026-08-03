-- Bronze: data/reviews.csv -> bronze.reviews
-- Nguồn duy nhất của logic bronze cho bảng reviews (xem macros/bronze_helpers.sql).
{% set source_file = 'reviews.csv' %}
{% set columns = {
    'review_id': 'string!',
    'order_id': 'integer',
    'product_id': 'integer',
    'customer_id': 'integer',
    'review_date': 'date',
    'rating': 'integer',
    'review_title': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        trim(review_id)             as review_id,
        order_id,
        product_id,
        customer_id,
        review_date,
        rating,
        -- review_title giữ nguyên hoa/thường: text người dùng viết; rỗng -> NULL
        nullif(trim(review_title), '') as review_title
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when order_id is null then 'order_id_missing' end,
            case when product_id is null then 'product_id_missing' end,
            case when customer_id is null then 'customer_id_missing' end,
            case when review_date is null then 'review_date_missing' end,
            case when rating is null or rating not between 1 and 5 then 'rating_out_of_range' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
