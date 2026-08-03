
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    order_date as unique_field,
    count(*) as n_records

from "iceberg"."analytics"."gold_revenue_daily"
where order_date is not null
group by order_date
having count(*) > 1



  
  
      
    ) dbt_internal_test