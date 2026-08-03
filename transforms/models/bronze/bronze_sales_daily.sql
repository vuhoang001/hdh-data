-- Bronze: data/sales.csv -> bronze.sales_daily  (tương đương ingest_sales_daily.py)
--
-- Doanh thu/giá vốn TỔNG HỢP SẴN theo ngày do nguồn cung cấp — KHÔNG phải bảng giao dịch,
-- và KHÔNG khớp với doanh thu tính từ order_items. Đừng "sửa" cho khớp: đây là hai nguồn
-- số độc lập, chênh lệch giữa chúng là thứ cần điều tra.
--
-- Cột nguồn viết hoa (Date, Revenue, COGS); `Date` trùng từ khoá SQL nên đổi tên khi đọc.
with source as (
    select * from {{ read_source_csv('sales.csv', {
        'sale_date': 'DATE',
        'revenue': 'DOUBLE',
        'cogs': 'DOUBLE'
    }) }}
),

flagged as (
    select
        *,
        {{ invalid_reason([
            ["sale_date is null", "sale_date_missing"],
            ["revenue is null or revenue < 0", "revenue_invalid"],
            ["cogs is null or cogs < 0", "cogs_invalid"]
        ]) }} as _invalid_reason
    from source
)

select
    *,
    _invalid_reason is null as _is_valid,
    -- KHÔNG gắn cờ lỗi cho cogs > revenue: 10% số ngày bán lỗ là sự thật kinh doanh, không
    -- phải rác. Cho ra cột riêng để phân tích được thay vì loại bỏ.
    cogs > revenue as _margin_negative,
    {{ bronze_audit('sales.csv') }}
from flagged
