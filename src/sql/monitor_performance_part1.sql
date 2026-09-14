-- Experiment config result: test_results_zone.stop_save_test_applied_Bart
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- Two-Offer Cohort: modeltype=MIDPOINT
    -- Treatment: MIDPOINT:CONTROL=1:1
  -- Three-Offer Cohort: modeltype=PCHURN
    -- Treatment: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.

create or replace table `gannett-datascience.test_results_zone.ss_test_result_v3-1`
as
with raw as (  -- cleaned ss_test_applied
  select 
    *, 
    date_add(inference_date, interval 5 day) as email_date 
  from (
    SELECT distinct    
      lower(trim(subscription)) as billing_account, -- zuora_subscriptionid = billing_account
      pricegroup,
      currentrate as start_price, 
      newrate as step_up_price,
      stopsave as stop_save_price,
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
    FROM `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
    -- where filedate != '2026-08-21'
  )
),
lk as (   -- remove
  SELECT distinct 
    lower(trim(l.billing_account)) as billing_account, 
    l.circ_idsubscrip as id_subscrip,
    l.product_type
  from `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l 
  where l.billing_system = 'ZUORA' and circ_site != 'PLAY'
),
ss_applied as (   -- 81973. remove. link billing_account and id_subscrip
  select * from (
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
  )
  where id_subscrip is not null 
  QUALIFY 
  COUNT(DISTINCT id_subscrip) OVER(PARTITION BY billing_account) = 1
  and 
  COUNT(DISTINCT email_date) OVER(PARTITION BY billing_account) = 1
),
gcp_events as (
  select * from `gannett-datascience.test_results_zone.ss_test_result_v3-0_gcp_event` 
  -- todo: WIP EDE-14782 checking why no action while vol perm is tracked. Temp exclude them until issue resolved.  
  where conflict_tag = 'No'  
),
call_center as (  -- Delete: called in after email date 
  select distinct       
    ss_applied.billing_account,
    ss_applied.id_subscrip,
    1 as called_in,
    count(distinct c.event_date) over (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as call_counts,
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __min_called_in_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __min_call_cancel_date
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
online as (   -- Delete: opened online cancel page after email date
  select
    ss_applied.billing_account, 
    ss_applied.id_subscrip,
    1 as opened_cancel_page,
    count(distinct c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as click_cancel_counts,
    min(c.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __min_open_cancel_page_date,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account, ss_applied.id_subscrip) as __min_ol_cancel_date
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
perm_stop as (  -- Delete. 
  select
    id_subscrip, 1 as perm_stoped, transaction_date as perm_stop_date, create_date, stop_code, stop_reason
  from `gannett-enterprise-data.consumers_rfz.subscriptions_trans_start_stop`
  where is_perm_stop = 1
  and not REGEXP_CONTAINS(stop_reason, r'Chargeback|Dup Start')
),
vol_perm_stop as (  -- Delete. 
  select
    p1.* except(stop_code), 
    IFNULL(perm_stop_voluntary, FALSE) as is_vol_perm
  from perm_stop p1
  left join `gannett-enterprise-data.mdm_cz.subscriptions_stop_reasons` mdm
    on lower(trim(p1.stop_code)) = lower(trim(mdm.stop_code))
),
cb1 as (
  select distinct
    ss_applied.*, 
    coalesce(c.called_in, 0) as called_in,
    coalesce(c.call_counts, 0) as call_counts,
    coalesce(o.opened_cancel_page, 0) as opened_cancel_page,
    coalesce(o.click_cancel_counts, 0) as click_cancel_counts,
    c.__min_called_in_date, o.__min_open_cancel_page_date, 
    c.__min_call_cancel_date, o.__min_ol_cancel_date,
    s.create_date as __create_date,
    coalesce(s.perm_stoped, 0) as perm_stoped,
    IFNULL(s.is_vol_perm, FALSE) as is_vol_perm,
    s.perm_stop_date, s.stop_reason
  from ss_applied
  left join call_center c on
    ss_applied.billing_account = c.billing_account
    and ss_applied.id_subscrip = c.id_Subscrip
  left join online o on
    ss_applied.billing_account = o.billing_account
    and ss_applied.id_subscrip = o.id_subscrip
  left join vol_perm_stop s on
    ss_applied.id_subscrip = s.id_subscrip
    and s.create_date >= ss_applied.email_date
)
select
  b.*,
  case
    when stop_int=0 and __earlist_contact_date is null then 'No Action yet'
    when stop_int=0 and __earlist_cancel_date is null then 'Saved'
    when stop_int=0 and __earlist_cancel_date is not null then 'conflict-unknown saved'
    when stop_int=1 and __earlist_contact_date is null then 'conflict-no contact vol perm stop'
    when stop_int=1 and __earlist_cancel_date is not null then 'Vol Perm Stopped'
    when stop_int=1 and __earlist_cancel_date is null then 'conflict-unknown stopped'
    when stop_int=2 and __earlist_contact_date is null then 'Involuntary Perm Stopped'
    when stop_int=2 and __earlist_contact_date is not null then 'conflict-contact invol perm stop'
    else 'others'
  end as status,
  case
    when stop_int=1 and __earlist_cancel_date is not null then 1
    when stop_int=2 and __earlist_contact_date is null then 2
    when stop_int=0 and __earlist_contact_date is null then 0
    when stop_int=0 and __earlist_cancel_date is null then 0
    else null
  end as churned,
  y.risk_tier as src_risk_tier,
  zf.frequency, zf.breadth, zf.tenure, zf.tt_cost,
  if(__earlist_contact_date<pricing_effective_date, 'Contact Before Pricing', 'Contact On/After Pricing') as contact_timing
from (
  select distinct
    billing_account, id_subscrip,
    cohort, Treatment,
    email_date, pricing_effective_date,
    pricegroup,
    start_price, step_up_price, stop_save_price, 
    called_in, call_counts, opened_cancel_page, click_cancel_counts,
    __min_called_in_date, __min_open_cancel_page_date, 
    case 
      when called_in + opened_cancel_page=0 then 'No Action yet'
      when called_in + opened_cancel_page=2 
        then if(__min_called_in_date >= __min_open_cancel_page_date, 'Online first', 'Called-In first')
      when called_in=1 then 'Called-In Cancel Flow'
      else 'Online Cancel Flow'
    end as contact_channel,
    case 
      when called_in + opened_cancel_page=2 then least(__min_called_in_date, __min_open_cancel_page_date)
      else coalesce(__min_called_in_date, __min_open_cancel_page_date)
    end as __earlist_contact_date,
    __min_call_cancel_date, __min_ol_cancel_date,
    case 
      when __min_call_cancel_date is not null and __min_ol_cancel_date is not null 
      then least(__min_call_cancel_date, __min_ol_cancel_date)
      else coalesce(__min_call_cancel_date, __min_ol_cancel_date)
    end as __earlist_cancel_date,
    __create_date,
    perm_stoped, is_vol_perm, 
    case
      when perm_stoped=0 and is_vol_perm is False then 0
      when perm_stoped=1 and is_vol_perm is False then 2
      when perm_stoped=1 and is_vol_perm is True then 1
      else -999
    end as stop_int,
    perm_stop_date, stop_reason,
    inference_date
  from cb1
) b
left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
  lower(trim(y.billing_account)) = lower(trim(b.billing_account)) 
  and y.inference_date = b.inference_date
left join `gannett-enterprise-data.models_sz.source_pchurn_staging` zf on
  b.inference_date = zf.inference_date
  and b.id_subscrip = zf.id_subscrip;


