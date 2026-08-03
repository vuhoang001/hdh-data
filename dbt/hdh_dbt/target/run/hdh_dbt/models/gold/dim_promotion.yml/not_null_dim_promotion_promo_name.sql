
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select promo_name
from "iceberg"."analytics"."dim_promotion"
where promo_name is null



  
  
      
    ) dbt_internal_test