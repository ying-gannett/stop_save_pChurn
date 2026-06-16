-- Experiment config result: test_results_zone.stop_save_test_applied_Bart
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- Two-Offer Cohort: modeltype=MIDPOINT
    -- Treatment: MIDPOINT:CONTROL=1:1
  -- Three-Offer Cohort: modeltype=PCHURN
    -- Treatment: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.

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
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __called_in_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __call_cancel_date
  from ss_applied
  join `gannett-datascience.test_activation_zone.ss_call_center` c on -- called in
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
online as (   -- opened online cancel page after email date
  select
    ss_applied.billing_account, 
    ss_applied.id_subscrip,
    1 as opened_cancel_page,
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __open_cancel_page_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __ol_cancel_date
  from ss_applied
  join(  -- opened online cancel page
    select 
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where entered_acc_mng = 1
  ) c on
    ss_applied.id_subscrip = c.id_subscrip
    and c.event_date >= ss_applied.email_date
  left join (  -- confirmed cancel online
    select
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where confirmed_cancel = 1
  ) cc on
    ss_applied.id_subscrip = cc.id_subscrip
    and cc.event_date >= ss_applied.email_date
),
perm_stop as (
  select
    id_subscrip, 1 as perm_stoped, transaction_date as perm_stop_date, create_date
  from `gannett-enterprise-data.consumers_rfz.subscriptions_trans_start_stop`
  where is_perm_stop = 1
),
cb1 as (
  select distinct
    ss_applied.*, 
    coalesce(c.called_in, 0) as called_in,
    coalesce(o.opened_cancel_page, 0) as opened_cancel_page,
    c.__called_in_date, o.__open_cancel_page_date, 
    c.__call_cancel_date, o.__ol_cancel_date,
    s.create_date as __create_date,
    coalesce(s.perm_stoped, 0) as perm_stoped,
    s.perm_stop_date
  from ss_applied
  left join call_center c on
    ss_applied.billing_account = c.billing_account
    and ss_applied.id_subscrip = c.id_Subscrip
  left join online o on
    ss_applied.billing_account = o.billing_account
    and ss_applied.id_subscrip = o.id_subscrip
  left join perm_stop s on
    ss_applied.id_subscrip = s.id_subscrip
    and s.create_date >= ss_applied.email_date
)
select
  b.*,
  case
    when __earlist_contact_date is null then 'No Action yet'
    when perm_stoped=1 and __earlist_cancel_date is not null then 'stoped'
    when perm_stoped=1 and __earlist_cancel_date is null then 'unknown stoped'
    when perm_stoped=0 and __earlist_cancel_date is null then 'saved'
    when perm_stoped=0 and __earlist_cancel_date is not null then 'unknown saved'
    else 'others'
  end as status,
  case
    when perm_stoped=1 and __earlist_cancel_date is not null then 1
    when perm_stoped=0 and __earlist_cancel_date is null then 0
    else null
  end as churned,
  y.risk_tier as src_risk_tier,
  z.churn_truth as pchurn_truth,
  if(__earlist_contact_date<pricing_effective_date, 'Contact Before Pricing', 'Contact On/After Pricing') as contact_timing
from (
  select distinct
    billing_account, id_subscrip,
    cohort, Treatment,
    email_date, pricing_effective_date,
    current_rate, new_rate, offered_rate, 
    called_in, opened_cancel_page, 
    __called_in_date, __open_cancel_page_date, 
    case 
      when called_in + opened_cancel_page=0 then 'No Action yet'
      when called_in + opened_cancel_page=2 
        then if(__called_in_date >= __open_cancel_page_date, 'Online first', 'Called-In first')
      when called_in=1 then 'Called-In Cancel Flow'
      else 'Online Cancel Flow'
    end as contact_channel,
    case 
      when called_in + opened_cancel_page=2 then least(__called_in_date, __open_cancel_page_date)
      else coalesce(__called_in_date, __open_cancel_page_date)
    end as __earlist_contact_date,
    __call_cancel_date, __ol_cancel_date,
    case 
      when __call_cancel_date is not null and __ol_cancel_date is not null 
      then least(__call_cancel_date, __ol_cancel_date)
      else coalesce(__call_cancel_date, __ol_cancel_date)
    end as __earlist_cancel_date,
    __create_date,
    perm_stoped, perm_stop_date,
    inference_date
  from cb1
) b
left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
  lower(trim(y.billing_account)) = lower(trim(b.billing_account)) 
  and y.inference_date = b.inference_date
left join `gannett-enterprise-data.models_sz.source_pchurn_segments` z on
  y.inference_date = z.inference_date
  and y.id_subscrip = z.id_subscrip;



