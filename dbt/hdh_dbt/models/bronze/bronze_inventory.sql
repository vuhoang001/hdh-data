-- Bronze: data/inventory.csv -> bronze.inventory  (tương đương ingest_inventory.py)
-- Ảnh chụp tồn kho theo tháng. Nguồn đã phi chuẩn hoá sẵn (product_name/category/segment)
-- và có sẵn cột dẫn xuất (year, month) — bronze giữ nguyên, việc bỏ cột thừa là của silver.
with source as (
    select * from {{ read_source_csv('inventory.csv', {
        'snapshot_date': 'DATE',
        'product_id': 'INTEGER',
        'stock_on_hand': 'INTEGER',
        'units_received': 'INTEGER',
        'units_sold': 'INTEGER',
        'stockout_days': 'INTEGER',
        'days_of_supply': 'DOUBLE',
        'fill_rate': 'DOUBLE',
        'stockout_flag': 'INTEGER',
        'overstock_flag': 'INTEGER',
        'reorder_flag': 'INTEGER',
        'sell_through_rate': 'DOUBLE',
        'product_name': 'VARCHAR',
        'category': 'VARCHAR',
        'segment': 'VARCHAR',
        'year': 'INTEGER',
        'month': 'INTEGER'
    }) }}
),

normalized as (
    select
        snapshot_date,
        product_id,
        stock_on_hand,
        units_received,
        units_sold,
        stockout_days,
        days_of_supply,
        fill_rate,
        stockout_flag,
        overstock_flag,
        reorder_flag,
        sell_through_rate,
        trim(product_name)      as product_name,
        lower(trim(category))   as category,
        lower(trim(segment))    as segment,
        year,
        month
    from source
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["stock_on_hand < 0", "stock_on_hand_negative"],
            ["units_received < 0", "units_received_negative"],
            ["units_sold < 0", "units_sold_negative"],
            ["stockout_days < 0", "stockout_days_negative"],
            ["fill_rate not between 0 and 1", "fill_rate_out_of_range"],
            ["sell_through_rate not between 0 and 1", "sell_through_out_of_range"],
            ["stockout_flag not in (0, 1)", "stockout_flag_invalid"],
            ["overstock_flag not in (0, 1)", "overstock_flag_invalid"],
            ["reorder_flag not in (0, 1)", "reorder_flag_invalid"],
            ["year <> year(snapshot_date)", "year_mismatch"],
            ["month <> month(snapshot_date)", "month_mismatch"],
            ["category not in ('casual','genz','outdoor','streetwear')", "category_unknown"],
            ["segment not in ('activewear','all-weather','balanced','everyday','performance','premium','standard','trendy')", "segment_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('inventory.csv') }}
from flagged
