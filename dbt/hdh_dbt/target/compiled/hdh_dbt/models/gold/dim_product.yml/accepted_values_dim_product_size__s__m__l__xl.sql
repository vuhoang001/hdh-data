
    
    

with all_values as (

    select
        size as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."dim_product"
    group by size

)

select *
from all_values
where value_field not in (
    's','m','l','xl'
)


