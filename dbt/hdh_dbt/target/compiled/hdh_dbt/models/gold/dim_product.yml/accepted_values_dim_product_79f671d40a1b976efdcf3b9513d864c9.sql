
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."dim_product"
    group by category

)

select *
from all_values
where value_field not in (
    'casual','genz','outdoor','streetwear'
)


