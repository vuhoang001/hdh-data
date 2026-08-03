
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select revenue
from "iceberg"."analytics"."gold_revenue_daily"
where revenue is null



  
  
      
    ) dbt_internal_test