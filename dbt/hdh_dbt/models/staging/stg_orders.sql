-- Làm sạch dữ liệu thô: chuẩn hoá kiểu, chỉ lấy đơn hợp lệ
with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    product,
    category,
    quantity,
    unit_price,
    amount,
    lower(status)               as status,
    order_ts,
    order_date
from source
where quantity > 0
  and unit_price >= 0
