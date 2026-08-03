
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_list_price
from "iceberg"."analytics"."dim_product"
where current_list_price is null



  
  
      
    ) dbt_internal_test