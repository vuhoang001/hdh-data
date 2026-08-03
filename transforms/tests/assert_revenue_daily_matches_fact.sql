-- SINGULAR TEST — ĐỐI CHIẾU THEO NGÀY giữa bảng tổng hợp và bảng fact.
--
-- gold_revenue_daily và fact_order_items đi từ cùng một nguồn (silver_order_items join
-- silver_orders) nhưng bằng hai câu SQL riêng biệt. Chúng PHẢI cho cùng con số.
--
-- Khác gì với generic test sum_equals đặt trên gold_revenue_daily.revenue? sum_equals chỉ
-- so TỔNG TOÀN BỘ — hai lỗi ngược dấu ở hai ngày khác nhau sẽ triệt tiêu nhau và test vẫn
-- pass. Test này so TỪNG NGÀY nên bắt được cả lỗi "gán nhầm doanh thu sang ngày bên cạnh".
--
-- full outer join, không phải inner: inner join chỉ so những ngày CÓ Ở CẢ HAI bên, tức là
-- bỏ lọt đúng cái lỗi nguy hiểm nhất — một ngày biến mất khỏi bảng tổng hợp.
--
-- PASS khi trả về 0 dòng.
with fact_by_day as (
    select
        date_key                as order_date,
        sum(net_amount)         as revenue,
        sum(quantity)           as num_units,
        count(distinct order_id) as num_orders
    from {{ ref('fact_order_items') }}
    group by date_key
),

gold as (
    select
        order_date,
        revenue,
        num_units,
        num_orders
    from {{ ref('gold_revenue_daily') }}
)

select
    coalesce(f.order_date, g.order_date) as order_date,
    f.revenue                            as fact_revenue,
    g.revenue                            as gold_revenue,
    f.num_units                          as fact_units,
    g.num_units                          as gold_units,
    f.num_orders                         as fact_orders,
    g.num_orders                         as gold_orders
from fact_by_day f
full outer join gold g on f.order_date = g.order_date
where f.order_date is null                              -- ngày có ở fact nhưng mất ở gold
   or g.order_date is null                              -- ngày có ở gold nhưng không có ở fact
   or abs(f.revenue - g.revenue) > 0.01
   or f.num_units  <> g.num_units
   or f.num_orders <> g.num_orders
