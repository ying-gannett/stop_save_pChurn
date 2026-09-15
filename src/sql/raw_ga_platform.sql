-- Preprocess engaged session counts over platform from the GA4 events data. 

  select
    event_date,
    COALESCE(LOWER(d050_user_anonymous_id), LOWER(d049_user_anonymous_id)) AS anonymous_id,
    d031_website_id AS site_code,
    d005_platform AS platform,
    count(distinct session_id) session_cnt
  from `gannett-enterprise-data.google_analytics_cz.ga4_events_refined`
  where event_date = GREATEST(DATE('2025-12-29'), DATE('{run_date}'))   -- 90 days before stop save test launching date (2026-03-29)
    and engaged_session_event = '1'
    group by all

