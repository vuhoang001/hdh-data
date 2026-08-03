{#
    GENERIC TEST TỰ VIẾT — not_future_date

    Dùng khi nào: một cột ngày nghiệp vụ (ngày đặt hàng, ngày đăng ký) không bao giờ được
    nằm ở tương lai. Ngày tương lai gần như luôn là lỗi parse (đọc nhầm dd/mm thành mm/dd)
    hoặc lỗi timezone ở hệ thống nguồn — và không test sẵn nào của dbt bắt được.

    Vì sao viết thành GENERIC thay vì singular: luật này áp cho nhiều cột ở nhiều bảng
    (silver_orders.order_date, silver_customers.signup_date, dim_date.date_key...).
    Singular test phải copy câu SQL cho từng bảng; generic viết một lần dùng mọi nơi.

    Cách dùng trong YAML:
        columns:
          - name: order_date
            data_tests:
              - not_future_date

        # hoặc so với một mốc khác current_date:
          - name: start_date
            data_tests:
              - not_future_date:
                  arguments:
                    relative_to: "cast('2023-01-01' as date)"

    Quy tắc bất di bất dịch của mọi test dbt: câu SQL TRẢ VỀ CÁC DÒNG SAI. 0 dòng = pass.
#}
{% test not_future_date(model, column_name, relative_to='current_date') %}

select
    {{ column_name }} as offending_value
from {{ model }}
where {{ column_name }} > {{ relative_to }}

{% endtest %}
