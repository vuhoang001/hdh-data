
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select promo_id
from "iceberg"."bronze"."promotions"
where promo_id is null



  
  
      
    ) dbt_internal_test