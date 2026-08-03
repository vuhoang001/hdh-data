
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_amount
from "iceberg"."analytics"."fact_order_items"
where net_amount is null



  
  
      
    ) dbt_internal_test