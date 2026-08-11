-- Silver: bảng tra cứu zip -> city/region/district.
with source as (
    select * from {{ source('bronze', 'geography') }}
)

select
    zip,
    city,
    region,
    district
from source
where _is_valid
