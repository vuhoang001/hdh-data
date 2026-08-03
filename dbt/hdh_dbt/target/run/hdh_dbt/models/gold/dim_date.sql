
  
    

    create table "iceberg"."analytics"."dim_date__dbt_tmp"
      
      
    as (
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
    





with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 4018
    order by generated_number



),

all_periods as (

    select (
        date_add('day', row_number() over (order by generated_number) - 1, cast('2012-01-01' as date))
    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast('2023-01-01' as date)

)

select * from filtered


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
    );

  