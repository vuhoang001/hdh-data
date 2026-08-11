-- Bronze: data/products.csv -> bronze.products
-- Nguồn duy nhất của logic bronze cho bảng products (xem macros/bronze_helpers.sql).
{% set source_file = 'products.csv' %}
{% set columns = {
    'product_id': 'integer!',
    'product_name': 'string',
    'category': 'string',
    'segment': 'string',
    'size': 'string',
    'color': 'string',
    'price': 'double',
    'cogs': 'double'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        product_id,
        -- product_name giữ nguyên hoa/thường: tên thương mại, không phải mã phân loại
        trim(product_name)      as product_name,
        lower(trim(category))   as category,
        lower(trim(segment))    as segment,
        lower(trim(size))       as size,
        lower(trim(color))      as color,
        price,
        cogs
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when product_name is null then 'product_name_missing' end,
            case when price is null or price <= 0 then 'price_invalid' end,
            case when cogs is null or cogs < 0 then 'cogs_invalid' end,
            -- Giá vốn cao hơn giá bán = bán lỗ. Có thể thật (xả hàng) nhưng thường là lỗi nhập liệu.
            case when cogs > price then 'cogs_above_price' end,
            case when category not in ('casual','genz','outdoor','streetwear') then 'category_unknown' end,
            case when segment not in ('activewear','all-weather','balanced','everyday','performance','premium','standard','trendy') then 'segment_unknown' end,
            case when size not in ('s','m','l','xl') then 'size_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
