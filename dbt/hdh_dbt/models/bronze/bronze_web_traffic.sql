-- Bronze: data/web_traffic.csv -> bronze.web_traffic  (tương đương ingest_web_traffic.py)
-- `date` ở header đổi thành `traffic_date`: `date` vừa là kiểu vừa là từ khoá SQL.
with source as (
    select * from {{ read_source_csv('web_traffic.csv', {
        'traffic_date': 'DATE',
        'sessions': 'INTEGER',
        'unique_visitors': 'INTEGER',
        'page_views': 'INTEGER',
        'bounce_rate': 'DOUBLE',
        'avg_session_duration_sec': 'DOUBLE',
        'traffic_source': 'VARCHAR'
    }) }}
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
        {{ invalid_reason([
            ["traffic_date is null", "traffic_date_missing"],
            ["sessions is null or sessions < 0", "sessions_invalid"],
            ["unique_visitors < 0", "unique_visitors_negative"],
            ["page_views < 0", "page_views_negative"],
            ["unique_visitors > sessions", "visitors_above_sessions"],
            ["page_views < sessions", "page_views_below_sessions"],
            ["bounce_rate not between 0 and 1", "bounce_rate_out_of_range"],
            ["avg_session_duration_sec < 0", "duration_negative"],
            ["traffic_source not in ('direct','email_campaign','organic_search','paid_search','referral','social_media')", "source_unknown"]
        ]) }} as _invalid_reason
    from normalized
)

select
    *,
    _invalid_reason is null as _is_valid,
    {{ bronze_audit('web_traffic.csv') }}
from flagged
