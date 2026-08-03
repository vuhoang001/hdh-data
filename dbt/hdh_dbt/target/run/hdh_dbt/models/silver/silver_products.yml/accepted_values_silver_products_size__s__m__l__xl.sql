
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        size as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_products"
    group by size

)

select *
from all_values
where value_field not in (
    's','m','l','xl'
)



  
  
      
    ) dbt_internal_test