
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    promo_id as unique_field,
    count(*) as n_records

from "iceberg"."analytics"."dim_promotion"
where promo_id is not null
group by promo_id
having count(*) > 1



  
  
      
    ) dbt_internal_test