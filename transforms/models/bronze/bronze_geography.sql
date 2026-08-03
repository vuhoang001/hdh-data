-- Bronze: data/geography.csv -> bronze.geography  (tương đương ingest_geography.py)
with source as (
    select * from {{ read_source_csv('geography.csv', {
        'zip': 'VARCHAR',
        'city': 'VARCHAR',
        'region': 'VARCHAR',
        'district': 'VARCHAR'
    }) }}
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
        {{ invalid_reason([
            ["zip is null", "zip_missing"],
            ["city is null", "city_missing"],
            ["region not in ('central','east','west')", "region_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('geography.csv') }}
from flagged
