
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select discount_value
from "iceberg"."analytics"."silver_promotions"
where discount_value is null



  
  
      
    ) dbt_internal_test