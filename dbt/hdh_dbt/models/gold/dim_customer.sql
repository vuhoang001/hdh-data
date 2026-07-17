-- Dimension khách hàng.
--
-- ĐÂY LÀ CHỖ SNOWFLAKE BỊ LÀM PHẲNG: nguồn có customers -> geography (2 tầng chuẩn hoá).
-- Star schema cố tình phi chuẩn hoá — gộp city/region/district vào thẳng dim_customer.
-- Đổi lại: dữ liệu địa lý bị lặp cho ~122k khách thay vì lưu gọn ở 40k zip, nhưng mọi
-- report "doanh thu theo vùng" chỉ còn 1 join thay vì 2.
--
-- Đánh đổi này là ĐÚNG với dimension: chúng nhỏ, đọc nhiều, ghi ít. 122k dòng lặp vài
-- cột text là cái giá rẻ để đổi lấy query đơn giản hơn.
--
-- SCD Type 1 (ghi đè, không lưu lịch sử): nguồn chỉ có 1 dòng/khách, không có cột
-- valid_from/valid_to nào, nên KHÔNG THỂ dựng lại lịch sử đã mất. Muốn Type 2 thì phải
-- bắt đầu chụp snapshot từ hôm nay trở đi (dbt snapshot), không hồi tố được.
with customers as (
    select * from {{ ref('silver_customers') }}
),

geography as (
    select * from {{ ref('silver_geography') }}
)

select
    -- Khoá tự nhiên: customer_id của nguồn đã sạch tuyệt đối (121.930/121.930 unique)
    c.customer_id,

    -- Thuộc tính nhân khẩu
    c.gender,
    c.age_group,
    c.acquisition_channel,

    -- Thuộc tính thời điểm đăng ký
    c.signup_date,
    year(c.signup_date)     as signup_year,
    date_format(c.signup_date, '%Y-%m') as signup_year_month,

    -- Thuộc tính địa lý (phi chuẩn hoá từ geography)
    c.zip,
    c.city,
    g.region,
    g.district
from customers c
-- left join dù đã kiểm chứng 0 dòng mồ côi: nếu tương lai có zip lạ, ta muốn MẤT thông tin
-- địa lý của khách đó chứ không muốn MẤT LUÔN khách khỏi dimension.
left join geography g on c.zip = g.zip
