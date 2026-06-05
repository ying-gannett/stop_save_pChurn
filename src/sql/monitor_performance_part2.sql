-- create or replace table `gannett-datascience.test_results_zone.ss_test_result_v3-2`
-- as
with p1 as (
  select * from `gannett-datascience.test_results_zone.ss_test_result_v3-1`
  where Channel != 'No Action yet'
),
payment as (  -- temp solution for 0.9% null payment amount (263/28182)
  select 
    *
  from (
    SELECT 
      lower(trim(p.account)) as billing_account,  p.id_subscrip,
      invoice_number,
      amount as payment_amount,   -- temp solution for 0.9% null payment amount (263/28182)
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
),
paid as (
  select
    p1.billing_account, p1.id_subscrip,
    -- p.id_payment_date, p.amount,
    sum(p.payment_amount) as tt_payment
  from p1
  join payment p on
    p1.billing_account = p.billing_account
    and p1.id_subscrip = p.id_subscrip
    and p.id_payment_date > p1.pricing_effective_date   -- actually reached the price-increase effective date
    and p.id_payment_date > p1.least_attempt_date   -- had the opportunity to accept or reject the stop-save offer
  group by 1, 2    
)
select 
  * except (tt_payment),
  if(churned=0, tt_payment, 0) as tt_payment
from (
  select distinct
    p1.*, 
    coalesce(p.tt_payment, 0) as tt_payment
  from p1
  left join paid p on
    p1.billing_account = p.billing_account
    and p1.id_subscrip = p.id_subscrip   
)