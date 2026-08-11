-- Bronze: data/geography.csv -> bronze.geography
-- Nguồn duy nhất của logic bronze cho bảng geography (xem macros/bronze_helpers.sql).
{% set source_file = 'geography.csv' %}
{% set columns = {
    'zip': 'string!',
    'city': 'string',
    'region': 'string',
    'district': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        trim(zip)               as zip,
        -- city/district giữ nguyên hoa/thường: danh từ riêng, không phải mã phân loại
        trim(city)              as city,
        lower(trim(region))     as region,
        trim(district)          as district
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when zip is null then 'zip_missing' end,
            case when city is null then 'city_missing' end,
            case when region not in ('central','east','west') then 'region_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
