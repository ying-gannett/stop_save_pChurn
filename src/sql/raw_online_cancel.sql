-- Prepare raw entering cancel flow events from GA4 data
with raw as (
  select
    event_date,
    d049_user_anonymous_id as anonymous_id,
    d031_website_id as website_id,
    LOWER(d027_event_detail) as event_detail,
  from `gannett-enterprise-data.google_analytics_cz.ga4_events_refined`
  where event_date = DATE('{run_date}')
),
tag_event as (
  select distinct
    * except(event_detail),
    if(event_detail like '%managesubscription%' and event_detail like '%cancel%', 1, 0) as entered_acc_mng,
    if(event_detail like '%snap|cancelsubscription%' and event_detail not like '%winback-zuora%', 1, 0) as confirmed_cancel,
    if(event_detail like '%stopsave%' and event_detail like '%success%', 1, 0) as success_save
  from raw
),
tag_user_event as (
  select
    event_date, anonymous_id, website_id,
    max(entered_acc_mng) as entered_acc_mng,
    max(confirmed_cancel) as confirmed_cancel,
    max(success_save) as success_save
  from tag_event
  group by 1, 2, 3
),
sub_main as (
  select distinct
    id_subscrip,
    consumer_id,
    effective_date, end_date,
    mdm_product_id
  from `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_main`,
  unnest(consumer_ids) as consumer_id
),
anony as (
  SELECT DISTINCT 
    consumer_id,
    anonymous_id
  FROM `gannett-enterprise-data.consumers_nonpii_cz.known_consumers`
  UNPIVOT
    exclude NULLS (anonymous_id FOR anon IN (anonymous_id_1,
        anonymous_id_2,
        anonymous_id_3))
),
s_map as (
  select distinct
    sub_main.id_subscrip,
    sub_main.effective_date, sub_main.end_date,
    anony.anonymous_id,
    p.website_id
  from sub_main 
  join anony on
    sub_main.consumer_id = anony.consumer_id
  join `gannett-enterprise-data.mdm_cz.product` p on
    sub_main.mdm_product_id = p.product_id
)
select
  t.*,
  s.id_subscrip
from tag_user_event t
join s_map s on
  t.anonymous_id = s.anonymous_id and
  t.website_id = s.website_id and
  t.event_date between s.effective_date and s.end_date
