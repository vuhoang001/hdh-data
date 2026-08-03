
    
    

select
    promo_id as unique_field,
    count(*) as n_records

from "iceberg"."bronze"."promotions"
where promo_id is not null
group by promo_id
having count(*) > 1


