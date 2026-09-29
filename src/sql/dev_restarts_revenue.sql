--------Explore restarts' revenue----------------
-- 1. Observed restarts with monthly, 3-months, 6-months, annual rate plan
-- 2. restarts could make update to 4 payments since restart until today
-- 3. id_payment_date aren't always perfect match rate mapping effective/end date
-- 4. when billing amount diff rate mapping, respect rate mapping
-- 5. try extract rate info from description

with raw as (  -- cleaned ss_test_applied
  select 
    *, 
    date_add(inference_date, interval 5 day) as email_date 
  from (
    SELECT distinct    
      lower(trim(subscription)) as billing_account, -- zuora_subscriptionid = billing_account
      pricegroup,
      cast(currentrate AS NUMERIC) as start_price, 
      cast(newrate AS NUMERIC) as step_up_price,
      cast(stopsave AS NUMERIC) as stop_save_price,
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
    zf.frequency, zf.breadth, zf.tenure, cast(zf.tt_cost AS NUMERIC) as tt_cost,
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
      payment_term
    FROM `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_invoice_payment` p
    where id_payment_date >= '2026-04-03'
  )
  where payment_status = 'Paid' 
  and balance >= 0
),
restarts as (
  SELECT 
    p1.id_subscrip as id_subscrip_origin, -- perm_stop_sys_date, perm_stop_date, 
    p1.stop_save_price,
    history.id_subscrip as restart_idsub, history.event_date as restart_date
  FROM p1,
  UNNEST(restart_history) AS history
  where Repeat_Restarts != 'not repeat restarts'
),
restart_pay as (
  -- select distinct 
  --   id_subscrip_origin, stop_save_price,
  --   sum(bill_amount_raw) as tt_paid_raw,        
  --   count(distinct id_payment_date) as cnt_payments,
  -- from (
    select
      r.*,
      p.id_payment_date, 
      p.billing_amount as bill_amount_raw,
    from restarts r
    join pays p on
      r.restart_idsub = p.id_subscrip
      and p.id_payment_date >= r.restart_date
    order by 1, 3
  -- )  
  -- group by 1, 2
)
select 
  rs.* except(conflict_tag, balance),
  r.* except(id_subscrip)
from (
  select r.id_subscrip, r.effective_date, r.end_date, m.monthly_price, m.description
  from `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_rate_new` r
  join `gannett-enterprise-data.consumers_rfz.rate_mapping_combined` m on
  lower(trim(r.rate_key_system)) = lower(trim(m.rate_key_system))
  and r.rate_key_value = m.rate_key_value
) r
join restart_pay rs on
  r.id_subscrip = rs.restart_idsub
  and rs.id_payment_date between r.effective_date and r.end_date
;