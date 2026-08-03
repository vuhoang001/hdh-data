
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select num_orders
from "iceberg"."analytics"."gold_orders_daily"
where num_orders is null



  
  
      
    ) dbt_internal_test