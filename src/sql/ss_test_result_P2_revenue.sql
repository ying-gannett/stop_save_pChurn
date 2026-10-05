-- Estimate the revenue opportunity from closing the repeat-restart loophole.
--
-- Primary comparison: earned pre-tax revenue over identical observation windows.
-- Secondary comparison: unprorated pre-tax paid-invoice value over those windows.
--
-- This is a scenario analysis, not a causal estimate. The 100% fix scenario assumes
-- every repeat restarter would have remained on the original monthly stop-save rate.

DECLARE as_of_date DATE DEFAULT DATE '2026-09-30';

CREATE TEMP TABLE restart_revenue_work AS
WITH horizon_definitions AS (
  SELECT '30_days' AS horizon, 30 AS horizon_days
  UNION ALL
  SELECT '60_days', 60
  UNION ALL
  SELECT '90_days', 90
  UNION ALL
  SELECT 'to_date', CAST(NULL AS INT64)
),
origins AS (
  SELECT
    inference_date,
    id_subscrip AS origin_id_subscrip,
    website_id,
    cohort,
    Treatment,
    Repeat_Restarts,
    stop_save_price,
    new_subid_counts,
    restart_history,
    perm_stop_date AS analysis_start_date,
    REPLACE(Repeat_Restarts, 'repeat restart via ', '') AS restart_type
  FROM `gannett-datascience.stop_save_refactor_staging.ss_test_result_p1_p2_combined`
  WHERE Repeat_Restarts IN (
    'repeat restart via intro',
    'repeat restart via winback'
  )
),
restart_pairs AS (
  SELECT
    o.origin_id_subscrip,
    history.id_subscrip AS restart_id_subscrip,
    MIN(history.event_date) AS restart_date
  FROM origins o
  CROSS JOIN UNNEST(o.restart_history) AS history
  WHERE history.id_subscrip IS NOT NULL
    AND history.event_date <= as_of_date
  GROUP BY
    o.origin_id_subscrip,
    history.id_subscrip
),
origin_horizons AS (
  SELECT
    o.* EXCEPT (restart_history),
    h.horizon,
    h.horizon_days,
    as_of_date,
    CASE
      WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
      ELSE DATE_ADD(o.analysis_start_date, INTERVAL h.horizon_days DAY)
    END AS window_end_exclusive,
    CASE
      WHEN h.horizon_days IS NULL
        THEN DATE_DIFF(DATE_ADD(as_of_date, INTERVAL 1 DAY), o.analysis_start_date, DAY)
      ELSE h.horizon_days
    END AS exposure_days
  FROM origins o
  CROSS JOIN horizon_definitions h
  WHERE o.analysis_start_date <= as_of_date
    AND (
      h.horizon_days IS NULL
      OR DATE_DIFF(DATE_ADD(as_of_date, INTERVAL 1 DAY), o.analysis_start_date, DAY)
        >= h.horizon_days
    )
),
invoice_classified AS (
  SELECT
    p.id_invoice,
    p.id_subscrip AS restart_id_subscrip,
    r.restart_date,
    p.id_payment_date,
    p.amount_without_tax + p.balance AS paid_invoice_value,
    p.service_start_date,
    p.service_end_date,
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
  FROM (
    SELECT DISTINCT restart_id_subscrip, restart_date
    FROM restart_pairs
  ) r
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_invoice_payment` p
    ON p.id_subscrip = r.restart_id_subscrip
  WHERE balance >= 0
),
paid_invoices AS (
  select 
    *,
    CAST(DATE_DIFF(service_end_exclusive, service_start, DAY) AS NUMERIC) as service_period
  from (
    SELECT
      *,
      SAFE.PARSE_DATE('%Y%m%d', CAST(service_start_date AS STRING)) AS service_start,
      DATE_ADD(
        SAFE.PARSE_DATE('%Y%m%d', CAST(service_end_date AS STRING)),
        INTERVAL 1 DAY
      ) AS service_end_exclusive,
    FROM invoice_classified
    WHERE payment_status = 'Paid'
  )
  WHERE service_start >= restart_date
    AND service_start < service_end_exclusive
),
rate_plan_flags AS (
  SELECT
    p.origin_id_subscrip,
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'YEAR|12M')) > 0
      AS had_annual_plan_by_as_of_date,
    COUNTIF(
      REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'\bFOR\b')
      OR r.monthly_rate <= 1.10
    ) > 0 AS had_promotional_plan_by_as_of_date,
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'STOP[ -]?SAVE')) > 0
      AS had_restart_stop_save_by_as_of_date
  FROM restart_pairs p
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_rate_new` r
    ON r.id_subscrip = p.restart_id_subscrip
  LEFT JOIN `gannett-enterprise-data.consumers_rfz.rate_mapping_combined` m
    ON LOWER(TRIM(r.rate_key_system)) = LOWER(TRIM(m.rate_key_system))
    AND r.rate_key_value = m.rate_key_value
  WHERE r.effective_date BETWEEN p.restart_date AND as_of_date
  GROUP BY p.origin_id_subscrip
),
invoice_metrics AS (
  SELECT
    h.*,
    CAST(stop_save_price AS NUMERIC)
      * CAST(exposure_days AS NUMERIC)
      / CAST(30.4375 AS NUMERIC) AS full_fix_revenue,
    COUNT(DISTINCT i.id_invoice) AS all_paid_invoice_count,
    COUNT(DISTINCT IF(
      i.id_payment_date < h.window_end_exclusive,
      i.id_invoice,
      NULL
    )) AS current_window_paid_invoice_count,
    COALESCE(SUM(
      CASE
        WHEN i.service_end_exclusive <= h.window_end_exclusive then i.paid_invoice_value
        WHEN i.service_start <= h.window_end_exclusive
        THEN i.paid_invoice_value * SAFE_DIVIDE(
          CAST(DATE_DIFF(h.window_end_exclusive, i.service_start, DAY) AS NUMERIC),
          i.service_period
        )
        ELSE 0
      END
    ), 0) AS current_earned_revenue,
    COALESCE(SUM(
      CASE
        WHEN i.id_payment_date < h.window_end_exclusive
        THEN i.paid_invoice_value
        ELSE 0
      END
    ), 0) AS current_paid_invoice_value
  FROM origin_horizons h
  JOIN restart_pairs p
    ON h.origin_id_subscrip = p.origin_id_subscrip
  LEFT JOIN paid_invoices i
    ON p.restart_id_subscrip = i.restart_id_subscrip
  GROUP BY ALL
),
metrics_base AS (
  SELECT
    i.*,
    full_fix_revenue * CAST(0.25 AS NUMERIC) AS fix_revenue_25pct,
    full_fix_revenue * CAST(0.50 AS NUMERIC) AS fix_revenue_50pct,
    full_fix_revenue * CAST(0.75 AS NUMERIC) AS fix_revenue_75pct,
    COALESCE(r.had_annual_plan_by_as_of_date, FALSE) AS had_annual_plan_by_as_of_date,
    COALESCE(r.had_promotional_plan_by_as_of_date, FALSE)
      AS had_promotional_plan_by_as_of_date,
    COALESCE(r.had_restart_stop_save_by_as_of_date, FALSE)
      AS had_restart_stop_save_by_as_of_date,
  FROM invoice_metrics i
  LEFT JOIN rate_plan_flags r
    USING (origin_id_subscrip)
),
metrics AS (
  SELECT
    *,
    full_fix_revenue - current_earned_revenue AS incremental_revenue_100pct,
    fix_revenue_25pct- current_earned_revenue AS incremental_revenue_25pct,
    fix_revenue_50pct - current_earned_revenue AS incremental_revenue_50pct,
    fix_revenue_75pct - current_earned_revenue AS incremental_revenue_75pct,
    SAFE_DIVIDE(current_earned_revenue, full_fix_revenue)
      AS break_even_acceptance_rate
  FROM metrics_base
)
SELECT
  *,
  ARRAY_TO_STRING(ARRAY(
    SELECT flag
    FROM UNNEST([
      IF(all_paid_invoice_count = 0, 'no_paid_invoices', NULL)
    ]) AS flag
    WHERE flag IS NOT NULL
  ), ' | ') AS data_quality_flags
FROM metrics;

CREATE OR REPLACE TABLE
  `gannett-datascience.stop_save_refactor_staging.dev_restarts_revenue_detail`
OPTIONS (
  description = 'Account-level earned and paid-invoice revenue scenarios for repeat restarters.'
)
AS
SELECT *
FROM restart_revenue_work;

CREATE OR REPLACE TABLE
  `gannett-datascience.stop_save_refactor_staging.dev_restarts_revenue_summary`
OPTIONS (
  description = 'Aggregated repeat-restart loophole scenarios by restart type and experiment segment.'
)
AS
SELECT
  as_of_date,
  horizon,
  horizon_days,
  restart_type,
  cohort,
  Treatment,
  COUNT(distinct origin_id_subscrip) AS origin_subscriptions,
  SUM(COALESCE(new_subid_counts, 0)) AS restart_subscriptions,
  COUNTIF(data_quality_flags != '') AS origins_with_quality_flags,
  ROUND(SUM(full_fix_revenue), 2) AS full_fix_revenue,
  ROUND(SUM(current_earned_revenue), 2) AS current_earned_revenue,
  ROUND(SUM(current_paid_invoice_value), 2) AS current_paid_invoice_value,
  ROUND(SUM(fix_revenue_25pct), 2) AS fix_revenue_25pct,
  ROUND(SUM(fix_revenue_50pct), 2) AS fix_revenue_50pct,
  ROUND(SUM(fix_revenue_75pct), 2) AS fix_revenue_75pct,
  ROUND(SUM(incremental_revenue_25pct), 2) AS incremental_revenue_25pct,
  ROUND(SUM(incremental_revenue_50pct), 2) AS incremental_revenue_50pct,
  ROUND(SUM(incremental_revenue_75pct), 2) AS incremental_revenue_75pct,
  ROUND(SUM(incremental_revenue_100pct), 2) AS incremental_revenue_100pct,
  SAFE_DIVIDE(SUM(current_earned_revenue), SUM(full_fix_revenue))
    AS weighted_break_even_acceptance_rate
FROM `gannett-datascience.stop_save_refactor_staging.dev_restarts_revenue_detail`
GROUP BY ALL;

SELECT *
FROM `gannett-datascience.stop_save_refactor_staging.dev_restarts_revenue_summary`
ORDER BY horizon_days, horizon, restart_type, cohort, Treatment;
