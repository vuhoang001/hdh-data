-- FACT TRUNG TÂM của star schema.
--
-- HẠT (grain): MỘT DÒNG = MỘT DÒNG HÀNG TRONG MỘT ĐƠN. 714.669 dòng.
-- Đây là câu đầu tiên phải viết cho mọi bảng fact, và là quyết định quan trọng nhất:
-- mọi cột trong bảng phải đúng với hạt này, không được lẫn cột ở hạt khác (ví dụ tổng
-- tiền của cả đơn — đó là hạt "đơn", không phải hạt "dòng hàng").
--
-- Fact chứa đúng 3 loại cột, không gì khác:
--   1. KHOÁ NGOẠI  -> trỏ tới dimension, luôn có hậu tố _key
--   2. DEGENERATE  -> khoá nghiệp vụ / thuộc tính không đáng làm dimension riêng
--   3. SỐ ĐO       -> con số cộng được
with items as (
    select * from {{ ref('silver_order_items') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
)

select
    -- === 1. KHOÁ NGOẠI TỚI DIMENSION ==========================================
    -- Không cột nào được NULL. promo_key dùng coalesce để đảm bảo điều đó.
    o.order_date                      as date_key,
    o.customer_id                     as customer_key,
    i.product_id                      as product_key,
    coalesce(i.promo_id, 'NO_PROMO')  as promo_key,

    -- === 2. DEGENERATE DIMENSION ==============================================
    -- order_id: khoá nghiệp vụ, không có thuộc tính nào để làm thành dimension riêng
    -- (order_date đã sang dim_date, customer_id đã sang dim_customer). Giữ lại trên fact
    -- để đếm số đơn và truy ngược về hệ thống nguồn.
    i.order_id,

    -- Các thuộc tính của đơn: mỗi cột chỉ 4-6 giá trị, làm 4 dimension tí hon thì thừa.
    -- Sách Kimball gọi cách gom chúng lại là "junk dimension"; ở quy mô này để thẳng
    -- trên fact là đủ và đơn giản hơn.
    o.order_status,
    o.payment_method,
    o.device_type,
    o.order_source,

    -- promo thứ 2 (chỉ 206/714.669 dòng = 0,03%). Một dòng hàng có thể mang 2 khuyến mãi,
    -- tức quan hệ nhiều-nhiều — về lý thuyết cần bridge table. Với 0,03% thì không đáng;
    -- giữ làm degenerate và ghi nhận hạn chế: phân tích theo promo_key chỉ tính promo thứ 1.
    i.promo_id_2                      as promo_id_2_degenerate,

    -- === 3. SỐ ĐO (measures) ==================================================
    -- Tất cả đều ADDITIVE: cộng được theo MỌI dimension (ngày, khách, sản phẩm, promo).
    -- Đây là loại số đo tốt nhất và là lý do star schema hoạt động.
    i.quantity,
    i.quantity * i.unit_price         as gross_amount,      -- tiền trước giảm giá
    i.discount_amount,                                      -- tiền giảm
    i.line_amount                     as net_amount,        -- gross - discount = doanh thu thật

    -- unit_price là NON-ADDITIVE: cộng giá đơn vị lại với nhau ra số vô nghĩa.
    -- Giữ lại vì cần cho phân tích (giá bán trung bình), nhưng chỉ được dùng với avg().
    -- Đây LÀ giá tại thời điểm giao dịch — khác dim_product.current_list_price (chỉ 0,01% khớp).
    i.unit_price
from items i
-- inner join an toàn: đã kiểm chứng mọi đơn đều có ít nhất 1 dòng hàng (646.945/646.945),
-- và 0 dòng hàng mồ côi. Không mất dòng nào.
join orders o on i.order_id = o.order_id
