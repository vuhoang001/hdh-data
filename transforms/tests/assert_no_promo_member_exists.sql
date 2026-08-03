-- SINGULAR TEST — thành viên nhân tạo 'NO_PROMO' phải tồn tại trong dim_promotion.
--
-- 61% dòng hàng không có khuyến mãi. Fact dùng coalesce(promo_id, 'NO_PROMO') để tránh
-- khoá ngoại NULL, nên toàn bộ thiết kế đó phụ thuộc vào việc dim_promotion CÓ dòng
-- 'NO_PROMO'. Mất dòng đó thì test relationships trên fact.promo_key sẽ fail hàng loạt
-- 438.353 dòng — thông báo lỗi khổng lồ mà không nói ra nguyên nhân thật.
--
-- Test này nổ trước và nói thẳng ra nguyên nhân. Đây là ví dụ của loại test kiểm tra
-- "một dòng BẮT BUỘC phải có" — dbt không có generic test nào cho việc đó, vì mọi generic
-- test đều duyệt trên các dòng ĐANG CÓ, còn đây là kiểm tra dòng ĐANG THIẾU.
--
-- PASS khi trả về 0 dòng.
select
    'NO_PROMO thiếu trong dim_promotion' as failure_reason,
    count(*)                             as rows_found
from {{ ref('dim_promotion') }}
where promo_id = 'NO_PROMO'
having count(*) <> 1
