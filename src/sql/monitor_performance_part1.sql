-- Experiment config result: test_results_zone.stop_save_test_applied_Bart
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- Two-Offer Cohort: modeltype=MIDPOINT
    -- Treatment: MIDPOINT:CONTROL=1:1
  -- Three-Offer Cohort: modeltype=PCHURN
    -- Treatment: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.
-- check stop_save_test_applied_Bart is available for last friday

-- create or replace table `gannett-datascience.test_results_zone.ss_test_result_v2`
-- as
-- create or replace table `gannett-datascience.test_results_zone.ss_test_result_v3-1`
-- as
with cleanup as (
  SELECT distinct    
    lower(trim(subscription)) as billing_account, -- zuora_subscriptionid (Bart) = billing_account (BQ)
    currentrate as current_rate, 
    newrate as new_rate, -- pricing rate
    stopsave as offered_rate, -- new calculated stopsave rate
    date(effective) as pricing_effective_date,
    if(modeltype='PCHURN', 'Three-Offer Cohort', 'Two-Offer Cohort') as cohort, 
    grouptype,
    case 
      when filedate = '2026-04-08' then date('2026-03-29')  -- filedate 4/8 uses inference_date 3/29
      when filedate = '2026-04-09' then date('2026-04-05')  -- filedate 4/9 uses inference_date 4/5
      else date_trunc(filedate, week(Sunday))               -- once per week going forward
    end as inference_date,
    -- account, term, length, filedate, ebill, paymentmethod, product, reason, brandid, marketid, 
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
call_center as (  -- cancel attempts and confirmed cancels after email date 
  select distinct       
    ss_applied.billing_account,
    ss_applied.id_subscrip,
    1 as called,
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __call_attempt_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __call_cancel_date
  from ss_applied
  join `gannett-datascience.test_activation_zone.ss_call_center` c on -- attempt to cancel
    ss_applied.billing_account = lower(trim(c.Account))
    and ss_applied.id_subscrip = c.idSubscrip
    and c.event_date >= ss_applied.email_date
  left join (
    select * from `gannett-datascience.test_activation_zone.ss_call_center`
    where Saves__Digital_to_Digital_ = 0
  ) cc on
    ss_applied.billing_account = lower(trim(cc.Account))
    and ss_applied.id_subscrip = c.idSubscrip
    and cc.event_date >= ss_applied.email_date
),
online as (   -- first confirmed online cancel after email date
  select
    ss_applied.billing_account, 
    ss_applied.id_subscrip,
    1 as opened_online_cancel,
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __ol_attempt_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __ol_cancel_date
  from ss_applied
  join(  -- cancel attempt
    select 
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where entered_acc_mng = 1
  ) c on
    ss_applied.id_subscrip = c.id_subscrip
    and c.event_date >= ss_applied.email_date
  left join (  -- raw online cancels
    select
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where confirmed_cancel = 1
  ) cc on
    ss_applied.id_subscrip = cc.id_subscrip
    and cc.event_date >= ss_applied.email_date
),
cb1 as (
  select distinct
    ss_applied.*, 
    coalesce(c.called, 0) as call_attempt,
    c.__call_attempt_date,
    if(__call_cancel_date is null, 0, 1) as call_cencelled,
    c.__call_cancel_date,
    coalesce(o.opened_online_cancel, 0) as online_attempt,
    o.__ol_attempt_date,
    if(__ol_cancel_date is null, 0, 1) as online_canceled,
    o.__ol_cancel_date,
    case 
      when c.__call_attempt_date is not null and o.__ol_attempt_date is not null
      then least(c.__call_attempt_date, o.__ol_attempt_date)
      else coalesce(c.__call_attempt_date, o.__ol_attempt_date)
    end as least_attempt_date,
    case 
      when c.__call_cancel_date is not null and o.__ol_cancel_date is not null 
      then least(c.__call_cancel_date, o.__ol_cancel_date)
      else coalesce(c.__call_cancel_date, o.__ol_cancel_date)
    end as least_cancel_date,
    case 
      when c.__call_cancel_date is not null and o.__ol_cancel_date is not null 
      then greatest(c.__call_cancel_date, o.__ol_cancel_date)
      else coalesce(c.__call_cancel_date, o.__ol_cancel_date)
    end as greatest_cancel_date,
  from ss_applied
  left join call_center c on
    ss_applied.billing_account = c.billing_account
    and ss_applied.id_subscrip = c.id_Subscrip
  left join online o on
    ss_applied.billing_account = o.billing_account
    and ss_applied.id_subscrip = o.id_subscrip
)
select 
  billing_account, id_subscrip,
  email_date, pricing_effective_date,
  current_rate, new_rate, offered_rate, 
  cohort,
  case 
    when grouptype='MIDPOINT' then 'Midpoint'
    when grouptype='CONTROL' then 'Control'
    else 'Tiered'
  end as Treatment,
  call_attempt, __call_attempt_date, call_cencelled, __call_cancel_date,
  online_attempt, __ol_attempt_date, online_canceled, __ol_cancel_date,
  least_attempt_date, least_cancel_date, greatest_cancel_date,
  cancel_types,
  case
    when contains_substr(cancel_types, 'Call Center') then 'Call Center'
    when contains_substr(cancel_types, 'Online') then 'Online'
    else 'No Action yet'
  end as Channel,
  churned,
  inference_date, src_risk_tier, pchurn_truth,
from (
  select distinct
    b.*, -- except(id_subscrip),
    case 
      when call_attempt+online_attempt=0 then 'No action yet'
      when call_cencelled+online_canceled=0 and call_attempt+online_attempt=2 
        then if(__call_attempt_date>=__ol_attempt_date, 'Call Center Saved', 'Online Saved')
      when call_cencelled+online_canceled=0 and call_attempt=1 then 'Call Center Saved'
      when call_cencelled+online_canceled=0 and online_attempt=1 then 'Online Saved'
      when call_cencelled+online_canceled=2 
        then if(__call_cancel_date<=__ol_cancel_date, 'Call Center Cancelled', 'Online Cancelled')
      when call_cencelled=1 then 'Call Center Cancelled'
      else 'Online Cancelled'
    end as cancel_types,
    if(call_cencelled+online_canceled=0, 0, 1) as churned,
    y.risk_tier as src_risk_tier,
    z.churn_truth as pchurn_truth,
  from cb1 b
  left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
    lower(trim(y.billing_account)) = lower(trim(b.billing_account)) 
    and y.inference_date = b.inference_date
  left join `gannett-enterprise-data.models_sz.source_pchurn_segments` z on
    y.inference_date = z.inference_date
    and y.id_subscrip = z.id_subscrip
);