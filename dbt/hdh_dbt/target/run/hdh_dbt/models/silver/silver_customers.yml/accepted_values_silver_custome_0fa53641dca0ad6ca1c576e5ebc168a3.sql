
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        acquisition_channel as value_field,
        count(*) as n_records

    from "iceberg"."analytics"."silver_customers"
    group by acquisition_channel

)

select *
from all_values
where value_field not in (
    'direct','email_campaign','organic_search','paid_search','referral','social_media'
)



  
  
      
    ) dbt_internal_test