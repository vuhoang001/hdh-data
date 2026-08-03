
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "iceberg"."analytics"."dim_date"
where year is null



  
  
      
    ) dbt_internal_test