-- Bronze: data/web_traffic.csv -> bronze.web_traffic
-- Nguồn duy nhất của logic bronze cho bảng web_traffic (xem macros/bronze_helpers.sql).
-- `date` ở header đổi thành `traffic_date`: `date` vừa là kiểu vừa là từ khoá SQL.
{% set source_file = 'web_traffic.csv' %}
{% set columns = {
    'traffic_date': 'date!',
    'sessions': 'integer',
    'unique_visitors': 'integer',
    'page_views': 'integer',
    'bounce_rate': 'double',
    'avg_session_duration_sec': 'double',
    'traffic_source': 'string'
} %}

with source as (
    select * from {{ bronze_source(source_file, columns) }}
),

normalized as (
    select
        traffic_date,
        sessions,
        unique_visitors,
        page_views,
        bounce_rate,
        avg_session_duration_sec,
        lower(trim(traffic_source)) as traffic_source
    from source
),

flagged as (
    select
        *,
        nullif(concat_ws(', ',
            case when traffic_date is null then 'traffic_date_missing' end,
            case when sessions is null or sessions < 0 then 'sessions_invalid' end,
            case when unique_visitors < 0 then 'unique_visitors_negative' end,
            case when page_views < 0 then 'page_views_negative' end,
            -- Một người có thể vào nhiều phiên, nhưng một phiên không thể có nhiều người.
            case when unique_visitors > sessions then 'visitors_above_sessions' end,
            -- Số trang xem không thể ít hơn số phiên: mỗi phiên xem ít nhất 1 trang.
            case when page_views < sessions then 'page_views_below_sessions' end,
            case when bounce_rate not between 0 and 1 then 'bounce_rate_out_of_range' end,
            case when avg_session_duration_sec < 0 then 'duration_negative' end,
            case when traffic_source not in ('direct','email_campaign','organic_search','paid_search','referral','social_media') then 'source_unknown' end
        ), '') as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    '{{ source_file }}'     as _source_file,
    current_timestamp       as _ingested_at
from flagged
