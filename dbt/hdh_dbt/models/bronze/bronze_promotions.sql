-- Bronze: data/promotions.csv -> bronze.promotions  (tương đương ingest_promotions.py)
with source as (
    select * from {{ read_source_csv('promotions.csv', {
        'promo_id': 'VARCHAR',
        'promo_name': 'VARCHAR',
        'promo_type': 'VARCHAR',
        'discount_value': 'DOUBLE',
        'start_date': 'DATE',
        'end_date': 'DATE',
        'applicable_category': 'VARCHAR',
        'promo_channel': 'VARCHAR',
        'stackable_flag': 'INTEGER',
        'min_order_value': 'DOUBLE'
    }) }}
),

normalized as (
    select
        trim(promo_id)              as promo_id,
        -- promo_name giữ nguyên hoa/thường: tên chiến dịch, không phải mã phân loại
        trim(promo_name)            as promo_name,
        lower(trim(promo_type))     as promo_type,
        discount_value,
        start_date,
        end_date,
        -- Rỗng nghĩa là "áp dụng mọi category", KHÔNG phải thiếu dữ liệu -> quy về NULL
        nullif(lower(trim(applicable_category)), '') as applicable_category,
        lower(trim(promo_channel))  as promo_channel,
        stackable_flag,
        min_order_value
    from source
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["start_date is null", "start_date_missing"],
            ["end_date is null", "end_date_missing"],
            ["end_date < start_date", "end_before_start"],
            ["discount_value is null or discount_value <= 0", "discount_value_invalid"],
            ["promo_type = 'percentage' and discount_value > 100", "percentage_above_100"],
            ["min_order_value < 0", "min_order_value_negative"],
            ["promo_type not in ('fixed','percentage')", "promo_type_unknown"],
            ["promo_channel not in ('all_channels','email','in_store','online','social_media')", "promo_channel_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('promotions.csv') }}
from flagged
