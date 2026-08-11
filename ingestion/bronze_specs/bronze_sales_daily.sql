-- Bronze: data/sales.csv -> bronze.sales_daily
-- Nguồn duy nhất của logic bronze cho bảng sales_daily (xem macros/bronze_helpers.sql).
--
-- Doanh thu/giá vốn TỔNG HỢP SẴN theo ngày do nguồn cung cấp — KHÔNG phải bảng giao dịch,
-- và KHÔNG khớp với doanh thu tính từ order_items. Đừng "sửa" cho khớp: đây là hai nguồn
-- số độc lập, chênh lệch giữa chúng là thứ cần điều tra.
--
-- Cột nguồn viết hoa (Date, Revenue, COGS); `Date` trùng từ khoá SQL nên đổi tên khi đọc.
{% set source_file = 'sales.csv' %}
{% set columns = {
    'sale_date': 'date!',
    'revenue': 'double',
    'cogs': 'double'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when sale_date is null then 'sale_date_missing' end,
            case when revenue is null or revenue < 0 then 'revenue_invalid' end,
            case when cogs is null or cogs < 0 then 'cogs_invalid' end
        ), '') as _invalid_reason
    from source
)

select
    *,
    _invalid_reason is null as _is_valid,
    -- KHÔNG gắn cờ lỗi cho cogs > revenue: 10% số ngày bán lỗ là sự thật kinh doanh, không
    -- phải rác. Cho ra cột riêng để phân tích được thay vì loại bỏ.
    cogs > revenue          as _margin_negative,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
