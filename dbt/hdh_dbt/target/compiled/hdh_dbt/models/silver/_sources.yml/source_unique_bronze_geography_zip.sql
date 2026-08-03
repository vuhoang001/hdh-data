
    
    

select
    zip as unique_field,
    count(*) as n_records

from "iceberg"."bronze"."geography"
where zip is not null
group by zip
having count(*) > 1


