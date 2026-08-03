-- Bronze: data/customers.csv -> bronze.customers  (tương đương ingest_customers.py)
with source as (
    select * from {{ read_source_csv('customers.csv', {
        'customer_id': 'INTEGER',
        'zip': 'VARCHAR',
        'city': 'VARCHAR',
        'signup_date': 'DATE',
        'gender': 'VARCHAR',
        'age_group': 'VARCHAR',
        'acquisition_channel': 'VARCHAR'
    }) }}
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
        {{ invalid_reason([
            ["signup_date is null", "signup_date_missing"],
            ["zip is null", "zip_missing"],
            ["gender not in ('female','male','non-binary')", "gender_unknown"],
            ["age_group not in ('18-24','25-34','35-44','45-54','55+')", "age_group_unknown"],
            ["acquisition_channel not in ('direct','email_campaign','organic_search','paid_search','referral','social_media')", "channel_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('customers.csv') }}
from flagged
