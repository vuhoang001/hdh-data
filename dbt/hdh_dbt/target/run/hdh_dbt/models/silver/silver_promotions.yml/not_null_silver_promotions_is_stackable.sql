
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_stackable
from "iceberg"."analytics"."silver_promotions"
where is_stackable is null



  
  
      
    ) dbt_internal_test