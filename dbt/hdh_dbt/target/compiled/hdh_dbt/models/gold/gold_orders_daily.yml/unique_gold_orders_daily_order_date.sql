
    
    

select
    order_date as unique_field,
    count(*) as n_records

from "iceberg"."analytics"."gold_orders_daily"
where order_date is not null
group by order_date
having count(*) > 1


