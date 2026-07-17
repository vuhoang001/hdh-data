-- Silver: sản phẩm đã làm sạch.
--
-- CẢNH BÁO về `price`: đây là giá NIÊM YẾT HIỆN TẠI, không phải giá đã bán.
-- Kiểm chứng trên dữ liệu thật: chỉ 76/714.669 dòng hàng (0,01%) có unit_price khớp
-- price ở đây. Muốn tính doanh thu, LUÔN dùng order_items.unit_price — giá tại thời
-- điểm giao dịch nằm ở fact, không nằm ở dimension.
with source as (
    select * from {{ source('bronze', 'products') }}
)

select
    product_id,
    product_name,
    category,
    segment,
    size,
    color,
    price,
    cogs,
    price - cogs as list_margin
from source
where _is_valid
