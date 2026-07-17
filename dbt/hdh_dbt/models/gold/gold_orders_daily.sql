-- Gold: số đơn theo ngày.
-- Chưa có revenue vì orders.csv không còn cột tiền — doanh thu nằm ở order_items.csv,
-- thêm vào đây khi có job bronze cho order_items.
with orders as (
    select * from {{ ref('silver_orders') }}
)

select
    order_date,
    count(*)                                           as num_orders,
    count(distinct customer_id)                        as num_customers,
    count(*) filter (where order_status = 'delivered') as num_delivered,
    count(*) filter (where order_status = 'cancelled') as num_cancelled,
    count(*) filter (where order_status = 'returned')  as num_returned
from orders
group by order_date
order by order_date
