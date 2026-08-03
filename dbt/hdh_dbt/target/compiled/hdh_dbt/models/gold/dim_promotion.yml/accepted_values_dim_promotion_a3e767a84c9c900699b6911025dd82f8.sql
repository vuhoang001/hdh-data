
    
    

with all_values as (

    select
        promo_type as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."dim_promotion"
    group by promo_type

)

select *
from all_values
where value_field not in (
    'fixed','percentage','none'
)


