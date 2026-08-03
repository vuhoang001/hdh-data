
    
    

with all_values as (

    select
        promo_channel as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_promotions"
    group by promo_channel

)

select *
from all_values
where value_field not in (
    'all_channels','email','in_store','online','social_media'
)


