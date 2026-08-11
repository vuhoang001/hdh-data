-- Bronze: data/inventory.csv -> bronze.inventory
-- Nguồn duy nhất của logic bronze cho bảng inventory (xem macros/bronze_helpers.sql).
--
-- Ảnh chụp tồn kho theo tháng. Nguồn đã phi chuẩn hoá sẵn (product_name/category/segment)
-- và có sẵn cột dẫn xuất (year, month) — bronze giữ nguyên, việc bỏ cột thừa là của silver.
{% set source_file = 'inventory.csv' %}
{% set columns = {
    'snapshot_date': 'date!',
    'product_id': 'integer!',
    'stock_on_hand': 'integer',
    'units_received': 'integer',
    'units_sold': 'integer',
    'stockout_days': 'integer',
    'days_of_supply': 'double',
    'fill_rate': 'double',
    'stockout_flag': 'integer',
    'overstock_flag': 'integer',
    'reorder_flag': 'integer',
    'sell_through_rate': 'double',
    'product_name': 'string',
    'category': 'string',
    'segment': 'string',
    'year': 'integer',
    'month': 'integer'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
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
        nullif(concat_ws(', ',
            case when stock_on_hand < 0 then 'stock_on_hand_negative' end,
            case when units_received < 0 then 'units_received_negative' end,
            case when units_sold < 0 then 'units_sold_negative' end,
            case when stockout_days < 0 then 'stockout_days_negative' end,
            -- fill_rate và sell_through_rate là tỷ lệ -> bắt buộc nằm trong [0, 1]
            case when fill_rate not between 0 and 1 then 'fill_rate_out_of_range' end,
            case when sell_through_rate not between 0 and 1 then 'sell_through_out_of_range' end,
            -- Các cờ chỉ được nhận 0 hoặc 1
            case when stockout_flag not in (0, 1) then 'stockout_flag_invalid' end,
            case when overstock_flag not in (0, 1) then 'overstock_flag_invalid' end,
            case when reorder_flag not in (0, 1) then 'reorder_flag_invalid' end,
            -- year/month là cột dẫn xuất từ snapshot_date. Lệch nhau nghĩa là nguồn tính sai,
            -- và mọi report nhóm theo year/month sẽ ra số sai mà không ai biết.
            case when year <> year(snapshot_date) then 'year_mismatch' end,
            case when month <> month(snapshot_date) then 'month_mismatch' end,
            case when category not in ('casual','genz','outdoor','streetwear') then 'category_unknown' end,
            case when segment not in ('activewear','all-weather','balanced','everyday','performance','premium','standard','trendy') then 'segment_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
