
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select signup_year
from "iceberg"."analytics"."dim_customer"
where signup_year is null



  
  
      
    ) dbt_internal_test