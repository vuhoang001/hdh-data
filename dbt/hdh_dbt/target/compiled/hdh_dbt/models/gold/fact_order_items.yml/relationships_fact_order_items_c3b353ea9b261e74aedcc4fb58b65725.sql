
    
    

with child as (
    select promo_key as from_field
    from "iceberg"."analytics"."fact_order_items"
    where promo_key is not null
),

parent as (
    select promo_id as to_field
    from "iceberg"."analytics"."dim_promotion"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


