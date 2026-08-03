
    
    

select
    customer_id as unique_field,
    count(*) as n_records

from "iceberg"."analytics"."dim_customer"
where customer_id is not null
group by customer_id
having count(*) > 1


