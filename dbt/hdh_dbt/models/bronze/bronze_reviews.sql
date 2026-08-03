-- Bronze: data/reviews.csv -> bronze.reviews  (tương đương ingest_reviews.py)
with source as (
    select * from {{ read_source_csv('reviews.csv', {
        'review_id': 'VARCHAR',
        'order_id': 'INTEGER',
        'product_id': 'INTEGER',
        'customer_id': 'INTEGER',
        'review_date': 'DATE',
        'rating': 'INTEGER',
        'review_title': 'VARCHAR'
    }) }}
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
        {{ invalid_reason([
            ["order_id is null", "order_id_missing"],
            ["product_id is null", "product_id_missing"],
            ["customer_id is null", "customer_id_missing"],
            ["review_date is null", "review_date_missing"],
            ["rating is null or rating not between 1 and 5", "rating_out_of_range"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('reviews.csv') }}
from flagged
