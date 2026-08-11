-- Dimension khuyến mãi.
--
-- ĐIỂM QUAN TRỌNG: dimension này có thêm 1 dòng NHÂN TẠO — 'NO_PROMO'.
--
-- Lý do: 61% dòng hàng (438.353/714.669) không dùng khuyến mãi, nên promo_id là NULL.
-- Star schema có nguyên tắc: KHOÁ NGOẠI TRONG FACT KHÔNG ĐƯỢC NULL. Vì sao?
--   - `join` thường sẽ âm thầm vứt 61% số dòng hàng -> doanh thu hụt hơn nửa.
--   - Người dùng buộc phải nhớ dùng `left join` cho riêng dimension này, còn các
--     dimension khác thì `join` — không nhất quán, sớm muộn cũng có người quên.
--   - `group by promo_name` sẽ gom mọi đơn không khuyến mãi vào một ô NULL vô nghĩa,
--     thay vì một nhãn đọc được.
--
-- Giải pháp Kimball: tạo một "thành viên không xác định" trong dimension, rồi fact dùng
-- coalesce(promo_id, 'NO_PROMO'). Nhờ đó MỌI join trong star này đều là inner join an
-- toàn, và report hiện "Không áp dụng khuyến mãi" thay vì ô trống.
with promotions as (
    select
        promo_id,
        promo_name,
        promo_type,
        discount_value,
        start_date,
        end_date,
        applicable_category,
        promo_channel,
        is_stackable,
        min_order_value,
        duration_days
    from {{ ref('silver_promotions') }}
),

no_promo_member as (
    select
        'NO_PROMO'                      as promo_id,
        'Không áp dụng khuyến mãi'      as promo_name,
        'none'                          as promo_type,
        cast(0 as double)               as discount_value,
        cast(null as date)              as start_date,
        cast(null as date)              as end_date,
        cast(null as varchar)           as applicable_category,
        'none'                          as promo_channel,
        false                           as is_stackable,
        cast(0 as double)               as min_order_value,
        cast(null as bigint)            as duration_days
)

select * from promotions
union all
select * from no_promo_member
