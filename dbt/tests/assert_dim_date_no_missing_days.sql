-- SINGULAR TEST — dim_date không được THIẾU NGÀY NÀO.
--
-- Đây là test sống còn của một date dimension: cả lý do tồn tại của nó là trả lời được
-- "ngày nào KHÔNG có đơn hàng". Nếu bản thân dim_date thủng lỗ, câu hỏi đó trả lời sai mà
-- không có dấu hiệu gì — mọi report vẫn chạy, chỉ là thiếu vài dòng.
--
-- unique + not_null trên date_key KHÔNG bắt được lỗi này: 4.000 ngày rời rạc vẫn unique và
-- vẫn not_null. Phải so SỐ DÒNG với ĐỘ DÀI KHOẢNG thì mới biết có thủng hay không.
--
-- date_diff giống nhau ở cả DuckDB và Trino nên viết thẳng, không cần macro portable.
--
-- PASS khi trả về 0 dòng.
with bounds as (
    select
        min(date_key) as first_day,
        max(date_key) as last_day,
        count(*)      as actual_days
    from {{ ref('dim_date') }}
)

select
    first_day,
    last_day,
    actual_days,
    date_diff('day', first_day, last_day) + 1 as expected_days
from bounds
where actual_days <> date_diff('day', first_day, last_day) + 1
