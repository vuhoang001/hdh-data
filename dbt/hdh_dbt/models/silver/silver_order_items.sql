with source as (
    select * from {{source('bronze', 'order_items')}}
)

select 
    order_id,
    product_id, 
    quantity, 
    unit_price, 
    coalesce(discount_amount, 0) as discount_amount, 
    quantity * unit_price - coalesce(discount_amount, 0) as line_amount,
    promo_id
from source 
where _is_valid