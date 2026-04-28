-- test_results_zone.stop_save_test_applied_Bart  -- Sampled cohort for ss_test: 
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- modeltype=MIDPOINT(pchurn not covered) 
    -- grouptype: MIDPOINT:CONTROL=1:1
  -- modeltype=PCHURN(pchurn covered)
    -- grouptype: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.

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
      subscription as billing_account, -- zuora_subscriptionid (Bart) = billing_account (BQ)
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
      concat(inference_date, billing_account)
    from `gannett-datascience.test_activation_zone.stop_save_test_Bart` s
    group by 1 having count(distinct id_subscrip) > 1 
  )
),
sub_status as (   -- curated subscription status
  SELECT distinct 
    l.billing_account, m.id_subscrip,
    m.effective_date, m.end_date, m.status
  FROM `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_main` m 
  join `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l on
    m.id_subscrip = l.circ_idsubscrip
  join `gannett-enterprise-data.mdm_cz.product` p on 
    m.mdm_product_id = p.product_id
  join `gannett-enterprise-data.mdm_cz.publication` pu on
    p.publication_id = pu.publication_id
  where l.billing_system = 'ZUORA' and pu.publication_name != 'USA TODAY Play'
),
churned as (  -- first churn after email_date
  select    
    ss_applied.billing_account, sub_status.effective_date as __churn_date, sub_status.status
  from ss_applied
  join sub_status on
    ss_applied.billing_account = sub_status.billing_account
    and sub_status.effective_date >= ss_applied.email_date 
  where sub_status.status = 'Inactive'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY billing_account ORDER BY effective_date ASC) = 1
),
call_center as (  -- first call center contact after email date
  select distinct       
    ss_applied.billing_account, 
    con.idSYSDATE as __contact_call_date, 
  from ss_applied
  join sub_status on
    ss_applied.billing_account = sub_status.billing_account
  join `gannett-enterprise-data.consumers_circ_prz.f_contact_consolidated` con on
    sub_status.id_subscrip = con.idSUBSCRIP
    and con.idSYSDATE >= ss_applied.email_date 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY billing_account ORDER BY idSYSDATE ASC) = 1
),
oo_cancel as (  -- confirmed online cancel events
  select
    id_subscrip, event_date, 
  from `gannett-datascience.test_activation_zone.ss_test_online_cancel_raw`
  where confirmed_cancel = 1
),
online as (   -- first confirmed online cancel after email date
  select
    ss_applied.billing_account, 
    oo_cancel.event_date as __oo_cancel_date,
  from ss_applied
  join sub_status on
    ss_applied.billing_account = sub_status.billing_account
  join oo_cancel on
    sub_status.id_subscrip = oo_cancel.id_subscrip
    and oo_cancel.event_date >= ss_applied.email_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY billing_account ORDER BY event_date ASC) = 1
),
raw_combine as (
  select 
    ss_applied.*, 
    c.__contact_call_date,
    if(c.__contact_call_date is null, 'Not called', 'Called') as __contact_call_status,
    o.__oo_cancel_date,
    if(o.__oo_cancel_date is null, 'Not Online Cancelled', 'Online Cancelled') as __online_cancel_status,
    churned.__churn_date,
    coalesce(churned.status, 'Active') as __sub_status,
  from ss_applied
  left join churned on
    ss_applied.billing_account = churned.billing_account
  left join call_center c on
    ss_applied.billing_account = c.billing_account
  left join online o on
    ss_applied.billing_account = o.billing_account
)
select 
  *,
  case 
    when __sub_status='Active' then 'Not churned'
    when __sub_status='Inactive' 
      and __contact_call_status='Called' 
      and __online_cancel_status='Online Cancelled' 
      then if(__contact_call_date<=__oo_cancel_date,'Online Cancelled', 'Call Center Cancelled')
    when __sub_status='Inactive' 
      and __contact_call_status='Called' 
      then 'Call Center Cancelled'
    when __sub_status='Inactive' 
      and __online_cancel_status='Online Cancelled'
      then 'Online Cancelled'
    else 'Untracked Cancelled'
  end as churn_flag,
from raw_combine