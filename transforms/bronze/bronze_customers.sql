-- Bronze: data/customers.csv -> bronze.customers
-- Nguồn duy nhất của logic bronze cho bảng customers (xem macros/bronze_helpers.sql).
{% set source_file = 'customers.csv' %}
{% set columns = {
    'customer_id': 'integer!',
    'zip': 'string',
    'city': 'string',
    'signup_date': 'date',
    'gender': 'string',
    'age_group': 'string',
    'acquisition_channel': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        customer_id,
        trim(zip)                           as zip,
        -- city giữ nguyên hoa/thường: danh từ riêng, không phải mã phân loại
        trim(city)                          as city,
        signup_date,
        lower(trim(gender))                 as gender,
        lower(trim(age_group))              as age_group,
        lower(trim(acquisition_channel))    as acquisition_channel
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when signup_date is null then 'signup_date_missing' end,
            case when zip is null then 'zip_missing' end,
            case when gender not in ('female','male','non-binary') then 'gender_unknown' end,
            case when age_group not in ('18-24','25-34','35-44','45-54','55+') then 'age_group_unknown' end,
            case when acquisition_channel not in ('direct','email_campaign','organic_search','paid_search','referral','social_media') then 'channel_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
