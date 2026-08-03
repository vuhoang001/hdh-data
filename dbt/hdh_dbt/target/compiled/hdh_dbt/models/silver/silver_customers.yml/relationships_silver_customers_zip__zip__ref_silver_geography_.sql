
    
    

with child as (
    select zip as from_field
    from "iceberg"."analytics"."silver_customers"
    where zip is not null
),

parent as (
    select zip as to_field
    from "iceberg"."analytics"."silver_geography"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


