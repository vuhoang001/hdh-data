-- Mart: doanh thu theo ngày (chỉ tính đơn completed)
with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_date,
    count(*)                    as num_orders,
    count(distinct customer_id) as num_customers,
    sum(quantity)               as total_quantity,
    sum(amount)                 as revenue
from orders
where status = 'completed'
group by order_date
order by order_date
