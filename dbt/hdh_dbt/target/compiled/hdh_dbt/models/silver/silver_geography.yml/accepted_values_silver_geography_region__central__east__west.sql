
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_geography"
    group by region

)

select *
from all_values
where value_field not in (
    'central','east','west'
)


