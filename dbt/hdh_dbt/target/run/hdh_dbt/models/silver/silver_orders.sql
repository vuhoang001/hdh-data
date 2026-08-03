
  create or replace view
    "iceberg"."analytics"."silver_orders"
  security definer
  as
    -- Silver: chỉ giữ đơn đạt kiểm tra chất lượng ở bronze, bỏ cột metadata kỹ thuật
with source as (
    select * from "iceberg"."bronze"."orders"
)

select
    order_id,
    order_date,
    customer_id,
    zip,
    order_status,
    payment_method,
    device_type,
    order_source
from source
where _is_valid
  ;
