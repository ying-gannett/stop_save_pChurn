create or replace table `gannett-datascience.test_results_zone.ss_test_result_v3-2`
as
with payment as ( 
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
p1 as (
  select 
    *, 
    concat(Treatment, ' - ', src_risk_tier) as treatment_plus_tier,
    cast(REGEXP_EXTRACT(pricegroup, r'(\d+)') as int64) as pricegroup_order
  from `gannett-datascience.test_results_zone.ss_test_result_v3-1`
  where id_subscrip is not null 
  and billing_account != 'mc-s000207628' 
  and id_subscrip not in (49964082, 79445242, 76087016)   -- first two were priced twice, third one perm stopped twice
  and churned in (0, 1, 2)   -- conflicts
),
paid as (
  select
    billing_account, id_subscrip,
    sum(bill_amount_raw) as tt_paid_raw,
    sum(bill_amount_save_once) as tt_paid_save_once,
    if(sum(pay_less_than_ss)>0, 1, 0) as pay_less_than_ss,
    if(sum(skpi_gap_wip)>0, 1, 0) as skpi_gap_wip,
  from (
    select
      p1.billing_account, p1.id_subscrip,
      p.billing_amount as bill_amount_raw,
      if(p.billing_amount < p1.stop_save_price, p1.stop_save_price, p.billing_amount) as bill_amount_save_once,    -- 9 payments in this case
      if(p.billing_amount < p1.stop_save_price, 1, 0) as pay_less_than_ss,
      if(p.billing_amount = p1.step_up_price, 1, 0) as skpi_gap_wip  -- 29 payments in this cases. Asking for SKPI data
    from p1
    join payment p on
      p1.billing_account = p.billing_account
      and p1.id_subscrip = p.id_subscrip
      and p.id_payment_date > p1.__earlist_contact_date   -- payments after contact
      and p.id_payment_date >= p1.pricing_effective_date   -- payments on or after effective date
  )  
  group by 1, 2    
)
select 
  * except (tt_paid_raw, tt_paid_save_once, skpi_gap_wip),
  case
    when status='No Action yet' then '0 contact'
    when call_counts>1 or click_cancel_counts>1 or pay_less_than_ss=1 then '2+ contact'
    else '1 contact'
  end as repeatedly_contacted,
  if(churned=0, tt_paid_raw, 0) as tt_paid_raw,
  if(churned=0, tt_paid_save_once, 0) as tt_paid_save_once,
from (
  select distinct
    p1.*, 
    coalesce(p.tt_paid_raw, 0) as tt_paid_raw,
    coalesce(p.tt_paid_save_once, 0) as tt_paid_save_once,
    coalesce(p.pay_less_than_ss, 0) as pay_less_than_ss,
    coalesce(p.skpi_gap_wip, 0) as skpi_gap_wip
  from p1
  left join paid p on
    p1.billing_account = p.billing_account
    and p1.id_subscrip = p.id_subscrip   
)
-- where skpi_gap_wip = 0 or churned = 1   -- exclude skpi_gap_wip=1 and churned=0



