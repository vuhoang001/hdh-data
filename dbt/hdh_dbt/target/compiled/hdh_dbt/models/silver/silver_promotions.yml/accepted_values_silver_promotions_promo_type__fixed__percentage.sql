
    
    

with all_values as (

    select
        promo_type as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_promotions"
    group by promo_type

)

select *
from all_values
where value_field not in (
    'fixed','percentage'
)


