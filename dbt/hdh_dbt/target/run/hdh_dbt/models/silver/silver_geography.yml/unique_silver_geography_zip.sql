
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    zip as unique_field,
    count(*) as n_records

from "iceberg"."analytics"."silver_geography"
where zip is not null
group by zip
having count(*) > 1



  
  
      
    ) dbt_internal_test