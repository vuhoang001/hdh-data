{#
    GENERIC TEST TỰ VIẾT — sum_equals  (test ĐỐI CHIẾU / reconciliation)

    Dùng khi nào: một bảng tổng hợp phải cộng lại đúng bằng bảng chi tiết sinh ra nó.
    Đây là chiều chất lượng ACCURACY — chiều duy nhất dbt không có test sẵn, vì "đúng"
    chỉ định nghĩa được bằng cách so với một nguồn khác.

    Vì sao quan trọng: mọi test cột đơn lẻ (not_null, unique, accepted_range) vẫn PASS
    khi model tổng hợp bị mất dòng do join hỏng hoặc filter thừa. Chỉ có đối chiếu tổng
    mới phát hiện "báo cáo thiếu mất 3% doanh thu".

    Cách dùng trong YAML:
        columns:
          - name: revenue
            data_tests:
              - sum_equals:
                  arguments:
                    compare_model: ref('fact_order_items')
                    compare_column: net_amount
                    tolerance: 0.01

    tolerance: dung sai tuyệt đối. Bắt buộc phải có vì cộng kiểu double theo hai thứ tự
    khác nhau (theo ngày vs theo dòng) cho kết quả lệch nhau ở vài chữ số cuối — đó là
    tính chất của số thực dấu phẩy động, không phải lỗi dữ liệu.
#}
{% test sum_equals(model, column_name, compare_model, compare_column, tolerance=0.01) %}

with this_side as (
    select sum({{ column_name }}) as total from {{ model }}
),

other_side as (
    select sum({{ compare_column }}) as total from {{ compare_model }}
)

select
    this_side.total                     as this_total,
    other_side.total                    as compare_total,
    this_side.total - other_side.total  as difference
from this_side
cross join other_side
where abs(this_side.total - other_side.total) > {{ tolerance }}

{% endtest %}
