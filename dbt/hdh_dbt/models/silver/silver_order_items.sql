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
    promo_id,
    -- Khuyến mãi thứ 2, chỉ 206/714.669 dòng (0,03%) có. Một dòng hàng mang 2 khuyến mãi
    -- là quan hệ nhiều-nhiều; giữ nguyên ở đây để gold quyết định xử lý thế nào.
    promo_id_2
from source
where _is_valid