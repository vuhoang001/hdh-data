-- Silver: chỉ giữ khách đạt kiểm tra chất lượng ở bronze, bỏ cột metadata kỹ thuật.
-- KHÔNG join sang geography ở đây: silver giữ từng bảng đúng hình dạng nguồn, việc phi
-- chuẩn hoá (gộp city/region/district vào khách) là chuyện của dim_customer ở gold.
with source as (
    select * from {{ source('bronze', 'customers') }}
)

select
    customer_id,
    zip,
    city,
    signup_date,
    gender,
    age_group,
    acquisition_channel
from source
where _is_valid
