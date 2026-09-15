

-- Experiment config result: test_results_zone.stop_save_test_applied_Bart
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- Two-Offer Cohort: modeltype=MIDPOINT
    -- Treatment: MIDPOINT:CONTROL=1:1
  -- Three-Offer Cohort: modeltype=PCHURN
    -- Treatment: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.

create or replace table `gannett-datascience.test_results_zone.ss_test_result_p1_p2_combined`
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
        when filedate = '2026-04-08' then date('2026-03-29')  -- filedate 4/8 ~ inference_date 3/29
        when filedate = '2026-04-09' then date('2026-04-05')  -- filedate 4/9 ~ inference_date 4/5
        when filedate = '2026-08-31' then date('2026-08-23')  -- filedate 8/31 ~ inference_date 8/23
        when filedate = '2026-09-07' then date('2026-08-30')  -- filedate 9/7 ~ inference_date 8/30
        else date_trunc(filedate, week(Sunday))               -- once per week for the rest weeks
      end as inference_date,
      -- account, term, length, filedate, ebill, paymentmethod, product, reason, brandid, marketid, grouptype,
    FROM `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
  )
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
b as (
  select distinct
    ss_applied.* except(id_subscrip), 
    ss_applied.id_subscrip as id_subscrip_manual, 
    g.* except(zuora_billing_account, pricing_notice_date, pricing_effective_date, pre_pricing_monthly_price, target_monthly_price) 
  from ss_applied
  left join `gannett-datascience.test_results_zone.ss_test_result_v3-0_gcp_event` g on
    ss_applied.billing_account = g.zuora_billing_account
),
p1 as (
  select
    b.*,
    y.risk_tier as src_risk_tier,
    zf.frequency, zf.breadth, zf.tenure, zf.tt_cost,
    concat(b.Treatment, ' - ', y.risk_tier) as treatment_plus_tier,
    cast(REGEXP_EXTRACT(b.pricegroup, r'(\d+)') as int64) as pricegroup_order
  from b
  left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
    lower(trim(y.billing_account)) = b.billing_account
    and y.inference_date = b.inference_date
  left join `gannett-enterprise-data.models_sz.source_pchurn_staging` zf on
    b.inference_date = zf.inference_date
    and b.id_subscrip = zf.id_subscrip
),
pays as ( 
  select 
    *
  from (
    SELECT 
      lower(trim(p.account)) as billing_account,  p.id_subscrip,
      invoice_number,
      amount_without_tax+balance as billing_amount,   -- known issue: 0.9% null payment amount (263/28182)
      balance,
      id_payment_date,
      case
        WHEN id_payment_date is not null and id_decline_date is null then 'Paid' -- Normal payment
        WHEN id_payment_date is not null and id_decline_date is not null and id_payment_date>=id_decline_date then 'Paid' -- Payment Date is after Decline
        WHEN id_payment_date is null and id_decline_date is null and amount=0 and status='Posted' then 'Paid' -- First Invoice Free
        WHEN id_payment_date is null and id_decline_date is not null then 'Not Paid' -- Normal decline
        WHEN id_payment_date is not null and id_decline_date is not null and id_payment_date<id_decline_date then 'Not Paid' -- Payment reverse
        WHEN id_payment_date is null and id_decline_date is null then 'Not Paid'
        ELSE 'Other'
      END AS payment_status,
    FROM `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_invoice_payment` p
    where id_payment_date >= '2026-04-03'
  )
  where payment_status = 'Paid' 
  and balance >= 0
),
paid as (
  select    -- 3027 paid users
    billing_account, id_subscrip,
    sum(bill_amount_raw) as tt_paid_raw,
    sum(bill_amount_fix_gamer) as tt_paid_single_saved,
    if(sum(lower_than_ss_tag)>0, 1, 0) as paid_lower_than_ss,   -- 255 + 1
    if(sum(same_as_target_tag)>0, 1, 0) as paid_target,         -- 52 + 1
  from (
    select
      p1.billing_account, p1.id_subscrip, p1.stop_save_price, p1.step_up_price,
      p.billing_amount as bill_amount_raw,
      if(p.billing_amount < p1.stop_save_price, p1.stop_save_price, p.billing_amount) as bill_amount_fix_gamer, -- as if lower than stop-save is banned
      if(p.billing_amount < p1.stop_save_price, 1, 0) as lower_than_ss_tag,
        p1.email_date, p1.pricing_effective_date, p1.earlist_contact_date, p1.contact_groups, p.id_payment_date,
      if(p.billing_amount = p1.step_up_price, 1, 0) as same_as_target_tag  -- if they contacted, why pay target
    from p1
    join pays p on
      p1.billing_account = p.billing_account
      and p1.id_subscrip = p.id_subscrip
      and p.id_payment_date > p1.earlist_contact_date   -- payments after contact
      and p.id_payment_date >= p1.pricing_effective_date   -- payments on or after effective date
    where p1.churn_code = 0   -- exclude churned users's payments 
  )  
  group by 1, 2 
)
select 
  * except(paid_lower_than_ss, Repeat_StopSaves),
  case 
    when paid_lower_than_ss=1 and Repeat_StopSaves='not repeate stop-saves' 
    then if(contains_substr(contact_groups, '|'), 'seek offer both channels', 'seek offer 2+')
    else Repeat_StopSaves
  end as Repeat_StopSaves, 
from (
  select distinct
    p1.*, 
    coalesce(p.tt_paid_raw, 0) as revenue_raw,
    coalesce(p.tt_paid_single_saved, 0) as revenue_single_saved,
    coalesce(p.paid_lower_than_ss, 0) as paid_lower_than_ss,
    coalesce(p.paid_target, 0) as paid_target
  from p1
  left join paid p on
    p1.billing_account = p.billing_account
    and p1.id_subscrip = p.id_subscrip   
)
where paid_target = 0   -- exclude 60 users who contacted but pay target price 
  and conflict_tag = 'No'  -- exclude 1964(Y/N = 1964/101834 | 2%) vol perm without contact associated. EDE-14782 closed and can't explain. 
  and churn_code is not null  -- 28 priced users not covered in consumer_events 



