
    
    

with all_values as (

    select
        order_status as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."fact_order_items"
    group by order_status

)

select *
from all_values
where value_field not in (
    'created','paid','shipped','delivered','returned','cancelled'
)


