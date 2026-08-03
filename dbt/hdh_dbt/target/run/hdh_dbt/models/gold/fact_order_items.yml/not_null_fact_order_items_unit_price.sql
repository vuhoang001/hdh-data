
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unit_price
from "iceberg"."analytics"."fact_order_items"
where unit_price is null



  
  
      
    ) dbt_internal_test