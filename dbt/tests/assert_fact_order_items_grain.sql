-- Test quan trọng nhất của bảng fact: HẠT không được thay đổi.
--
-- fact_order_items join silver_order_items với silver_orders. Nếu join nhân dòng (khoá bên
-- orders không unique) hoặc làm mất dòng (có dòng hàng mồ côi), số dòng sẽ lệch — và đó là
-- lỗi nghiêm trọng nhất có thể xảy ra với fact: mọi số đo sau đó đều sai, mà không có test
-- cột đơn lẻ nào phát hiện được.
--
-- Viết tay thay vì dùng dbt_utils.equal_rowcount vì macro đó sinh ra `group by <alias>`,
-- cú pháp Postgres mà Trino không hỗ trợ.
--
-- Test singular: PASS khi trả về 0 dòng.
with fact as (
    select count(*) as n from {{ ref('fact_order_items') }}
),

silver as (
    select count(*) as n from {{ ref('silver_order_items') }}
)

select
    fact.n   as fact_rows,
    silver.n as silver_rows
from fact
cross join silver
where fact.n <> silver.n
