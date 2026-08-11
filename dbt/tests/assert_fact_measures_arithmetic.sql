-- SINGULAR TEST — số đo trên fact phải nhất quán với nhau về mặt số học.
--
-- Luật: net_amount = gross_amount - discount_amount, VÀ gross_amount = quantity * unit_price.
--
-- Vì sao cần dù hai cột này do chính SQL tính ra: đúng là hôm nay nó tautology. Nhưng
-- gross_amount tính ở fact_order_items còn net_amount (line_amount) tính ở
-- silver_order_items — HAI FILE KHÁC NHAU. Ngày ai đó sửa công thức chiết khấu ở một
-- bên mà quên bên kia, không có test cột đơn lẻ nào phát hiện được: cả hai cột vẫn
-- not_null, vẫn >= 0, vẫn "trông hợp lý". Test này là cái duy nhất nổ.
--
-- Dùng ngưỡng 0.01 chứ không so bằng: gross_amount và net_amount là double, phép nhân
-- và trừ trên số thực dấu phẩy động không cho kết quả khớp đến bit cuối.
--
-- PASS khi trả về 0 dòng.
select
    order_id,
    product_key,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount
from {{ ref('fact_order_items') }}
where abs(net_amount - (gross_amount - discount_amount)) > 0.01
   or abs(gross_amount - (quantity * unit_price)) > 0.01
