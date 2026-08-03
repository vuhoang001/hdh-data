
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_products"
    group by category

)

select *
from all_values
where value_field not in (
    'casual','genz','outdoor','streetwear'
)



  
  
      
    ) dbt_internal_test