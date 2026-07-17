-- Dimension sản phẩm.
--
-- CHÚ Ý TÊN CỘT: `price` của nguồn được đổi thành `current_list_price` một cách có chủ ý.
--
-- Kiểm chứng trên dữ liệu thật: chỉ 76/714.669 dòng hàng (0,01%) có unit_price khớp với
-- price ở đây. Riêng sản phẩm 536 năm 2015 đã bán ở dải 9.352 -> 11.289 trong khi price
-- ghi 11.059. Nghĩa là cột này là GIÁ NIÊM YẾT HIỆN TẠI, không phải giá đã bán.
--
-- Nếu để tên `price`, sớm muộn sẽ có người viết `sum(quantity * price)` để tính doanh thu
-- và ra số sai hoàn toàn — mà không có lỗi nào báo. Tên dài hơn nhưng nói đúng sự thật.
--
-- Nguyên tắc Kimball đằng sau: FACT giữ số đo tại thời điểm giao dịch (unit_price),
-- DIMENSION giữ thuộc tính mô tả hiện tại (current_list_price). Đừng lẫn hai thứ.
with products as (
    select * from {{ ref('silver_products') }}
)

select
    -- Khoá tự nhiên: product_id của nguồn đã sạch (2.412/2.412 unique)
    product_id,

    -- Thuộc tính mô tả
    product_name,
    category,
    segment,
    size,
    color,

    -- Thuộc tính giá HIỆN TẠI — không dùng để tính doanh thu lịch sử
    price       as current_list_price,
    cogs        as current_unit_cost,
    list_margin as current_list_margin
from products
