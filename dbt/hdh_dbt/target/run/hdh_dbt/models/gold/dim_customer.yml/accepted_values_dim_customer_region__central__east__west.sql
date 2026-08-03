
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        region as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."dim_customer"
    group by region

)

select *
from all_values
where value_field not in (
    'central','east','west'
)



  
  
      
    ) dbt_internal_test