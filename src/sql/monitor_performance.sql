-- test_results_zone.stop_save_test_applied_Bart  -- Sampled cohort for ss_test: 
  -- balanced by site and price level, 
  -- quarterly/annuals are excluded
  -- modeltype=MIDPOINT(pchurn not covered) 
    -- grouptype: MIDPOINT:CONTROL=1:1
  -- modeltype=PCHURN(pchurn covered)
    -- grouptype: MIDPOINT:CONTRO:TIERED=1:1:1
    -- TIERED: RISK1-5 maintain pchurn ratio.
-- check stop_save_test_applied_Bart is available for last friday
with raw as (  -- cleaned ss_test_applied
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
    raw.*,
    lk.id_subscrip
  from raw 
  left join lk on
  raw.billing_account = lk.billing_account   
  where raw.modeltype = 'MIDPOINT'
  union all
  select 
    raw.*,
    p.id_subscrip
  from raw 
  left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` p on
  raw.billing_account = lower(trim(p.billing_account))   
  and raw.inference_date = p.inference_date
  where raw.modeltype = 'PCHURN' 
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
payment as (  -- temp solution for 0.9% null payment amount (263/28182)
  select 
    *, 
    coalesce(payment_amount, bill_amount) as amount   -- temp solution for 0.9% null payment amount (263/28182)
  from (
    SELECT 
      lower(trim(p.account)) as billing_account,  p.id_subscrip,
      invoice_number,
      amount as bill_amount,
      payment_amount,
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
    ss_applied.billing_account, 
    ss_applied.id_subscrip,
    sum(p.amount) as tt_paid_amount
  from ss_applied
  join payment p on
    ss_applied.billing_account = p.billing_account
    and ss_applied.id_subscrip = p.id_subscrip
    and p.id_payment_date >= ss_applied.email_date
  group by 1, 2    
),
raw_combine as (
  select 
    ss_applied.*, 
    coalesce(c.called, 0) as call_cancel_attempt,
    c.__call_attempt_date,
    if(__call_cancel_date is null, 0, 1) as call_cencelled,
    c.__call_cancel_date,
    coalesce(o.opened_online_cancel, 0) as online_cancel_attempt,
    o.__ol_attempt_date,
    if(__ol_cancel_date is null, 0, 1) as online_canceled,
    o.__ol_cancel_date,
    coalesce(p.tt_paid_amount, 0) as tt_paid_amount
  from ss_applied
  left join call_center c on
    ss_applied.billing_account = c.billing_account
    and ss_applied.id_subscrip = c.id_Subscrip
  left join online o on
    ss_applied.billing_account = o.billing_account
    and ss_applied.id_subscrip = o.id_subscrip
  left join paid p on
    ss_applied.billing_account = p.billing_account
    and ss_applied.id_subscrip = p.id_subscrip   
)
  select distinct
    b.* except(id_subscrip),
    case 
      when call_cancel_attempt+online_cancel_attempt=0 then 'No action yet'
      when call_cancel_attempt+online_cancel_attempt=2 and call_cencelled+online_canceled=0 then 'Both attempt and Saved'
        -- then if(__call_attempt_date>=__ol_attempt_date, 'Call Center Saved', 'Online Saved')
      when call_cencelled+online_canceled=0 and call_cancel_attempt=1 then 'Call Center Saved'
      when call_cencelled+online_canceled=0 and online_cancel_attempt=1 then 'Online Saved'
      when call_cencelled+online_canceled=2 then 'Both Cancelled'
        -- then if(__call_cancel_date<=__ol_cancel_date, 'Call Center Cancelled', 'Online Cancelled')
      when call_cencelled=1 then 'Call Center Cancelled'
      else 'Online Cancelled'
    end as cancel_types,
    if(call_cencelled+online_canceled=0, 0, 1) as churned,
    y.risk_tier as src_risk_tier,
    z.churn_truth,
  from raw_combine b
  left join `gannett-datascience.test_activation_zone.stop_save_test_Bart` y on
    lower(trim(y.billing_account)) = lower(trim(b.billing_account)) 
    and y.inference_date = b.inference_date
  left join `gannett-enterprise-data.models_sz.source_pchurn_segments` z on
    y.inference_date = z.inference_date
    and y.id_subscrip = z.id_subscrip