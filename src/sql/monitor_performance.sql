-- test_results_zone.stop_save_test_applied_Bart  -- Sampled cohort for ss_test: 
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- modeltype=MIDPOINT(pchurn not covered) 
    -- grouptype: MIDPOINT:CONTROL=1:1
  -- modeltype=PCHURN(pchurn covered)
    -- grouptype: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.
-- check stop_save_test_applied_Bart is available for last friday
create or replace table `gannett-datascience.test_results_zone.ss_test_result_v2`
partition by inference_date
as
with ss_applied as (  -- cleaned ss_test_applied
  select 
    *, 
    date_add(inference_date, interval 5 day) as email_date 
  from (  
    SELECT distinct   
      account, 
      lower(trim(subscription)) as billing_account, -- zuora_subscriptionid (Bart) = billing_account (BQ)
      currentrate as current_rate, -- rate at the time of pricing
      pricegroup, -- int(currentrate)
      newrate as new_rate, -- the rate they were priced to
      term as __term, length as __length,
      filedate as __filedate, 
      case 
        when filedate = '2026-04-08' then date('2026-03-29')  -- filedate 4/8 uses inference_date 3/29
        when filedate = '2026-04-09' then date('2026-04-05')  -- filedate 4/9 uses inference_date 4/5
        else date_trunc(filedate, week(Sunday))               -- once per week going forward
      end as inference_date,
      date(effective) as __pricing_effective_date,
      stopsave as stop_save_rate, -- new calculated stopsave rate
      modeltype, 
      case
        when grouptype='RISK1' then '1.Low'
        when grouptype='RISK2' then '2.Med-Low'
        when grouptype='RISK3' then '3.Medium'
        when grouptype='RISK4' then '4.Med-High'
        when grouptype='RISK5' then '5.High'
        else concat('0.', grouptype)
      end as grouptype,
      -- ebill, paymentmethod, product, reason, brandid, marketid, 
    FROM `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
  )
  where concat(inference_date, billing_account) not in (  -- remove billing account who have 1+ id_subscrip
    select distinct 
      concat(inference_date, lower(trim(billing_account)))
    from `gannett-datascience.test_activation_zone.stop_save_test_Bart` s
    group by 1 having count(distinct id_subscrip) > 1 
  )
),
call_center as (  -- cancel attempts and confirmed cancels after email date
  select distinct       
    ss_applied.billing_account,
    1 as called,
    min(cc.Date) OVER (PARTITION BY ss_applied.billing_account) as __call_cancel_date
  from ss_applied
  join `gannett-datascience.test_activation_zone.call_center_data` c on -- attempt to cancel
    ss_applied.billing_account = lower(trim(c.Account))
    and c.Date >= ss_applied.email_date
  left join (
    select * from `gannett-datascience.test_activation_zone.call_center_data`
    where Saves__Digital_to_Digital_ = 0
  ) cc on
    ss_applied.billing_account = lower(trim(cc.Account))
    and cc.Date >= ss_applied.email_date
),
sub_status as (   -- curated subscription status
  SELECT distinct 
    lower(trim(l.billing_account)) as billing_account, m.id_subscrip,
    m.effective_date, m.end_date, m.status
  FROM `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_main` m 
  join `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l on
    m.id_subscrip = l.circ_idsubscrip
  join `gannett-enterprise-data.mdm_cz.product` p on 
    m.mdm_product_id = p.product_id
  join `gannett-enterprise-data.mdm_cz.publication` pu on
    p.publication_id = pu.publication_id
  where pu.publication_name != 'USA TODAY Play' and l.billing_system = 'ZUORA'
),
online as (   -- first confirmed online cancel after email date
  select
    ss_applied.billing_account, 
    1 as opened_online_cancel,
    min(cc.event_date) OVER (PARTITION BY ss_applied.billing_account) as __ol_cancel_date
  from ss_applied
  join sub_status on
    ss_applied.billing_account = sub_status.billing_account
  join(  -- cancel attempt
    select 
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where entered_acc_mng = 1
  ) c on
    sub_status.id_subscrip = c.id_subscrip
    and c.event_date >= ss_applied.email_date
  left join (  -- raw online cancels
    select
      id_subscrip, event_date, 
    from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
    where confirmed_cancel = 1
  ) cc on
    sub_status.id_subscrip = cc.id_subscrip
    and cc.event_date >= ss_applied.email_date
),
raw_combine as (
  select 
    ss_applied.*, 
    coalesce(c.called, 0) as call_cancel_attempt,
    if(__call_cancel_date is null, 0, 1) as call_cencelled,
    c.__call_cancel_date,
    coalesce(o.opened_online_cancel, 0) as online_cancel_attempt,
    if(__ol_cancel_date is null, 0, 1) as online_canceled,
    o.__ol_cancel_date,
  from ss_applied
  left join call_center c on
    ss_applied.billing_account = c.billing_account
  left join online o on
    ss_applied.billing_account = o.billing_account
)
select distinct
  b.*,
  case 
    when call_cancel_attempt+call_cencelled+online_cancel_attempt+online_canceled=0 then 'No action yet'
    when call_cencelled=1 then 'Call Center Cancelled'
    when online_canceled=1 then 'Online Cancelled'
    when call_cancel_attempt=1 then 'Call Center Saved'
    else 'Online Saved'
  end as cancel_types,
  if(call_cencelled+online_canceled=0, 0, 1) as churned,
  y.risk_tier as src_risk_tier
from raw_combine b
left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
  lower(trim(y.billing_account)) = lower(trim(b.billing_account)) 
  and y.inference_date = b.inference_date
