-- Bronze: data/products.csv -> bronze.products  (tương đương ingest_products.py)
with source as (
    select * from {{ read_source_csv('products.csv', {
        'product_id': 'INTEGER',
        'product_name': 'VARCHAR',
        'category': 'VARCHAR',
        'segment': 'VARCHAR',
        'size': 'VARCHAR',
        'color': 'VARCHAR',
        'price': 'DOUBLE',
        'cogs': 'DOUBLE'
    }) }}
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
        {{ invalid_reason([
            ["product_name is null", "product_name_missing"],
            ["price is null or price <= 0", "price_invalid"],
            ["cogs is null or cogs < 0", "cogs_invalid"],
            ["cogs > price", "cogs_above_price"],
            ["category not in ('casual','genz','outdoor','streetwear')", "category_unknown"],
            ["segment not in ('activewear','all-weather','balanced','everyday','performance','premium','standard','trendy')", "segment_unknown"],
            ["size not in ('s','m','l','xl')", "size_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('products.csv') }}
from flagged
