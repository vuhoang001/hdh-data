-- Bronze: data/promotions.csv -> bronze.promotions
-- Nguồn duy nhất của logic bronze cho bảng promotions (xem macros/bronze_helpers.sql).
{% set source_file = 'promotions.csv' %}
{% set columns = {
    'promo_id': 'string!',
    'promo_name': 'string',
    'promo_type': 'string',
    'discount_value': 'double',
    'start_date': 'date',
    'end_date': 'date',
    'applicable_category': 'string',
    'promo_channel': 'string',
    'stackable_flag': 'integer',
    'min_order_value': 'double'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
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
        nullif(concat_ws(', ',
            case when start_date is null then 'start_date_missing' end,
            case when end_date is null then 'end_date_missing' end,
            -- Kết thúc trước khi bắt đầu -> khoảng thời gian rỗng, khuyến mãi không bao giờ chạy
            case when end_date < start_date then 'end_before_start' end,
            case when discount_value is null or discount_value <= 0 then 'discount_value_invalid' end,
            -- Giảm giá quá 100% nghĩa là trả tiền cho khách để họ mua hàng
            case when promo_type = 'percentage' and discount_value > 100 then 'percentage_above_100' end,
            case when min_order_value < 0 then 'min_order_value_negative' end,
            case when promo_type not in ('fixed','percentage') then 'promo_type_unknown' end,
            case when promo_channel not in ('all_channels','email','in_store','online','social_media') then 'promo_channel_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
