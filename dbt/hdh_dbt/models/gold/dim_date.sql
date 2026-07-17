-- Dimension ngày — bảng duy nhất KHÔNG sinh ra từ dữ liệu nguồn mà tự tạo.
--
-- Tại sao cần? Ba lý do:
--   1. Trả lời được câu hỏi "ngày nào KHÔNG có đơn nào" — dữ liệu nguồn không chứa
--      những ngày đó, nên không group by nào tìm ra được.
--   2. Gom mọi logic lịch về một chỗ: định nghĩa "cuối tuần", "quý" viết một lần ở đây
--      thay vì lặp lại (và lệch nhau) trong từng report.
--   3. Cho phép lọc/nhóm theo thuộc tính lịch mà không cần hàm ngày tháng trong query.
--
-- Khoảng: 2012-01-01 (trước signup_date sớm nhất là 2012-01-17) -> hết 2022-12-31.
-- end_date của date_spine là mốc LOẠI TRỪ nên phải để 2023-01-01.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2012-01-01' as date)",
        end_date="cast('2023-01-01' as date)"
    ) }}
),

renamed as (
    select cast(date_day as date) as date_key
    from spine
)

select
    date_key,
    year(date_key)                          as year,
    quarter(date_key)                       as quarter,
    month(date_key)                         as month,
    date_format(date_key, '%M')             as month_name,
    day(date_key)                           as day_of_month,
    day_of_week(date_key)                   as day_of_week,   -- Trino: 1=thứ 2 ... 7=chủ nhật
    date_format(date_key, '%W')             as day_name,
    day_of_week(date_key) >= 6              as is_weekend,
    week_of_year(date_key)                  as week_of_year,
    date_format(date_key, '%Y-%m')          as year_month
from renamed
