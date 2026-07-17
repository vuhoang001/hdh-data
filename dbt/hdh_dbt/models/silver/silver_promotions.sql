-- Silver: chương trình khuyến mãi đã làm sạch.
with source as (
    select * from {{ source('bronze', 'promotions') }}
)

select
    promo_id,
    promo_name,
    promo_type,
    discount_value,
    start_date,
    end_date,
    -- NULL nghĩa là "áp dụng mọi category" (40/50 dòng), không phải thiếu dữ liệu
    applicable_category,
    promo_channel,
    -- Cờ 0/1 của nguồn -> boolean: SQL đọc `where is_stackable` tự nhiên hơn `where flag = 1`
    stackable_flag = 1 as is_stackable,
    min_order_value,
    date_diff('day', start_date, end_date) as duration_days
from source
where _is_valid
