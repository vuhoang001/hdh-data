
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select zip
from "iceberg"."bronze"."customers"
where zip is null



  
  
      
    ) dbt_internal_test