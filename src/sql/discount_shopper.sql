create or replace table `gannett-datascience.test_results_zone.ss_test_discount_shopper`
as
with cleanup as (
  SELECT distinct    
    lower(trim(subscription)) as billing_account, -- zuora_subscriptionid (Bart) = billing_account (BQ)
    currentrate as current_rate, 
    newrate as new_rate, -- pricing rate
    stopsave as offered_rate, -- new calculated stopsave rate
    date(effective) as pricing_effective_date,
    if(modeltype='PCHURN', 'Three-Offer Cohort', 'Two-Offer Cohort') as cohort, 
    case 
      when grouptype='MIDPOINT' then 'Midpoint'
      when grouptype='CONTROL' then 'Control'
      else 'Tiered'
    end as Treatment,
    case 
      when filedate = '2026-04-08' then date('2026-03-29')  -- filedate 4/8 uses inference_date 3/29
      when filedate = '2026-04-09' then date('2026-04-05')  -- filedate 4/9 uses inference_date 4/5
      else date_trunc(filedate, week(Sunday))               -- once per week going forward
    end as inference_date,
    -- account, term, length, filedate, ebill, paymentmethod, product, reason, brandid, marketid, grouptype,
    -- pricegroup as int_of_currentrate
  FROM `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
),
raw as (  -- cleaned ss_test_applied
  select 
    *, 
    date_add(inference_date, interval 5 day) as email_date 
  from cleanup
),
lk as (
  SELECT distinct 
    lower(trim(l.billing_account)) as billing_account, 
    l.circ_idsubscrip as id_subscrip,
    l.product_type
  from `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l 
  where l.billing_system = 'ZUORA' and circ_site != 'PLAY'
),
ss_applied as (   -- link billing_account and id_subscrip
  select 
    lk.id_subscrip,
    raw.*
  from raw 
  left join lk on
  raw.billing_account = lk.billing_account   
  where raw.cohort = 'Two-Offer Cohort'
  union all
  select 
    p.id_subscrip,
    raw.*
  from raw 
  left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` p on
  raw.billing_account = lower(trim(p.billing_account))   
  and raw.inference_date = p.inference_date
  where raw.cohort = 'Three-Offer Cohort' 
),
call_center as (  -- called in after email date 
  select distinct       
    ss_applied.billing_account,
    ss_applied.id_subscrip,
    1 as called_in,
    c.event_date as __called_in_date,
  from ss_applied
  join `gannett-datascience.test_activation_zone.ss_call_center` c on -- called in
    ss_applied.billing_account = lower(trim(c.Account))
    and ss_applied.id_subscrip = c.idSubscrip
    and c.event_date >= ss_applied.email_date
),
online as (   -- opened online cancel page after email date
  select distinct
    ss_applied.billing_account, 
    ss_applied.id_subscrip,
    1 as opened_cancel_page,
    c.event_date as __open_cancel_page_date,
  from ss_applied
  join(  -- opened online cancel page
    select 
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where entered_acc_mng = 1
  ) c on
    ss_applied.id_subscrip = c.id_subscrip
    and c.event_date >= ss_applied.email_date
),
cb1 as (
  select 
    *,
    DENSE_RANK() OVER (
      PARTITION BY billing_account, id_subscrip
      ORDER BY event_date ASC
    ) AS rank_num,
  from (
    select distinct
      c.billing_account, c.id_subscrip, 
      c.__called_in_date as event_date, "Called In" as event_type
    from call_center c
    union all 
    select distinct
      o.billing_account, o.id_subscrip, 
      o.__open_cancel_page_date as event_date, "Opened Cancel Page" as event_type
    from online o
  )
),
cb2 as (
  select
    billing_account, id_subscrip, contact_order, 
    string_agg(distinct event_type, ' & ' order by event_type asc) as contact_types,
    ARRAY_AGG(event_date) as contact_date
  from (
    select 
      *,
      CASE rank_num
        WHEN 1 THEN '1st'
        WHEN 2 THEN '2nd'
        WHEN 3 THEN '3rd'
        WHEN 4 THEN '4th'
        WHEN 5 THEN '5th'
        WHEN 6 THEN '6th'
        WHEN 7 THEN '7th'
        WHEN 8 THEN '8th'
        WHEN 9 THEN '9th'
        ELSE CAST(rank_num AS STRING)
      END AS contact_order
    from cb1
  )
  group by 1, 2, 3
)
select
  t.*,
  coalesce(contact_order, 'No Action yet') as contact_order,
  coalesce(contact_types, 'No Action yet') as contact_types,
  cb2.contact_date
from `gannett-datascience.test_results_zone.ss_test_result_v3-1` t
left join cb2 on
t.billing_account = cb2.billing_account
and t.id_subscrip = cb2.id_subscrip
order by billing_account, contact_order
