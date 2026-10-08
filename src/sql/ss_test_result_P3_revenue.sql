-- Estimate the revenue opportunity from closing the repeat-restart loophole.
--
-- Actual paid invoice total value: restarters' total paid invoice value since restart until current date.
-- Expected Fixed Revenue: stop save rate * actual paid invoice counts.
-- service_period_type: Classification of the service period (e.g., Monthly Contract, Longer Contract, no payment)
-- had_restart_stop_save: Flag indicating whether the restarter had a stop-save rate applied during their restart period.
--
-- This is a scenario analysis, not a causal estimate. The 100% fix scenario assumes
-- every repeat restarter would have remained on the original monthly stop-save rate.

CREATE TEMP TABLE restart_revenue_work AS
WITH origins AS ( -- 591 origin id_sub
  SELECT
    inference_date,
    id_subscrip AS origin_id_subscrip,
    website_id,
    contact_channels,
    cohort,
    Treatment,
    src_risk_tier,
    CAST(stop_save_price AS NUMERIC) AS stop_save_price,
    Repeat_Restarts,
    perm_stop_date,
    new_subid_counts,
    restart_history,
  FROM `{{p2_combined_table}}`
  WHERE Repeat_Restarts IN (
    'repeat restart via intro',
    'repeat restart via winback'
  )
),
restart_pairs AS (  -- 591 origin id_sub, 592 restart id_sub
  SELECT
    o.origin_id_subscrip,
    history.id_subscrip AS restart_id_subscrip,
    MIN(history.event_date) AS restart_date
  FROM origins o,
  UNNEST(o.restart_history) AS history
  WHERE history.id_subscrip IS NOT NULL
  GROUP BY
    o.origin_id_subscrip,
    history.id_subscrip
),
classified_invoices AS (
  SELECT
    r.origin_id_subscrip,
    r.restart_id_subscrip,
    r.restart_date,
    p.id_invoice,
    p.id_payment_date,
    p.amount_without_tax + p.balance AS paid_invoice_value,
    SAFE.PARSE_DATE('%Y%m%d', CAST(p.service_start_date AS STRING)) AS service_start,
    DATE_ADD(
      SAFE.PARSE_DATE('%Y%m%d', CAST(p.service_end_date AS STRING)),
      INTERVAL 1 DAY
    ) AS service_end_exclusive,
    CASE
      WHEN p.id_payment_date IS NOT NULL AND p.id_decline_date IS NULL THEN 'Paid'
      WHEN p.id_payment_date IS NOT NULL
        AND p.id_decline_date IS NOT NULL
        AND p.id_payment_date >= p.id_decline_date THEN 'Paid'
      WHEN p.id_payment_date IS NULL
        AND p.id_decline_date IS NULL
        AND p.amount = 0
        AND p.status = 'Posted' THEN 'Paid'
      WHEN p.id_payment_date IS NULL AND p.id_decline_date IS NOT NULL THEN 'Not Paid'
      WHEN p.id_payment_date IS NOT NULL
        AND p.id_decline_date IS NOT NULL
        AND p.id_payment_date < p.id_decline_date THEN 'Not Paid'
      WHEN p.id_payment_date IS NULL AND p.id_decline_date IS NULL THEN 'Not Paid'
      ELSE 'Other'
    END AS payment_status
  FROM restart_pairs r
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_invoice_payment` p
    ON p.id_subscrip = r.restart_id_subscrip
  WHERE balance >= 0
),
restart_paid_invoices AS (   -- 588 restarters paid 812 invoice
  SELECT 
    *,
    CAST(DATE_DIFF(service_end_exclusive, service_start, DAY) AS NUMERIC) as service_period
  FROM classified_invoices
  WHERE payment_status = 'Paid'
  AND service_start < service_end_exclusive
  and service_start >= restart_date
),
labelled_rate_plan AS (
  SELECT
    p.* except(payment_status),
    case
      when service_period > 40 then 'Longer Contract'
      else 'Monthly Contract'
    end as service_period_type,
    r.effective_date, r.end_date, 
    m.description,
    COUNTIF(
      REGEXP_CONTAINS(
        UPPER(COALESCE(m.description, '')), 
        r'STOP[ -]?SAVE'
      )
    ) over (partition by p.restart_id_subscrip) > 0 AS had_restart_stop_save
  FROM restart_paid_invoices p
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_rate_new` r
    ON r.id_subscrip = p.restart_id_subscrip
  JOIN `gannett-enterprise-data.consumers_rfz.rate_mapping_combined` m
    ON LOWER(TRIM(r.rate_key_system)) = LOWER(TRIM(m.rate_key_system))
    AND r.rate_key_value = m.rate_key_value
  where p.service_end_exclusive between r.effective_date and r.end_date
  order by restart_id_subscrip, service_start, effective_date
),
metric as (
  select distinct 
    origin_id_subscrip,
    COUNT(DISTINCT id_invoice) OVER (PARTITION BY origin_id_subscrip) AS actual_paid_invoice_cnt,
    COALESCE(SUM(paid_invoice_value) OVER (PARTITION BY origin_id_subscrip), 0) AS actual_paid_invoice_value,
    FIRST_VALUE(service_period_type) OVER (
      PARTITION BY origin_id_subscrip ORDER BY service_start ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_service_period_type, 
    any_value(had_restart_stop_save) OVER (PARTITION BY origin_id_subscrip) as had_restart_stop_save
  from labelled_rate_plan
)
select
  h.*,
  COALESCE(i.actual_paid_invoice_value, 0) as actual_paid_invoice_value,
  COALESCE(i.actual_paid_invoice_cnt * h.stop_save_price, 0) as expected_fixed_revenue,
  COALESCE(i.first_service_period_type, 'no payment') as service_period_type,
  COALESCE(i.had_restart_stop_save, false) as had_restart_stop_save
from origins h
left join metric i on 
  h.origin_id_subscrip = i.origin_id_subscrip

CREATE OR REPLACE TABLE
  `{{p2_revenue_detail_table}}`
OPTIONS (
  description = 'Account-level expected fixed revenue and paid-invoice revenue for repeat restarters.'
)
AS
SELECT *
FROM restart_revenue_work;
