-- Gold: doanh thu theo ngày.
-- order_items không có cột ngày nên phải join với orders để lấy order_date.
with orders as (
    select * from {{ ref('silver_orders') }}
),

items as (
    select * from {{ ref('silver_order_items') }}
)

select
    o.order_date,
    count(distinct o.order_id)      as num_orders,
    count(*)                        as num_lines,
    sum(i.quantity)                 as num_units,
    sum(i.line_amount)              as revenue,
    sum(i.discount_amount)          as total_discount,
    sum(i.line_amount) / count(distinct o.order_id) as avg_order_value
from items i
join orders o on i.order_id = o.order_id
group by o.order_date
