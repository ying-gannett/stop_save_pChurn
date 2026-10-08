-- Measure paid revenue for the original and restarted subscriptions over
-- consistent 30-, 60-, 90-day, and to-date horizons.
--
-- Original-subscription windows start after contact and pricing take effect.
-- Restart windows start when the original permanent stop takes effect. The
-- restart component also estimates revenue if repeat restarters had instead
-- accepted the original monthly stop-save price.
--
-- This is a scenario analysis, not a causal estimate. The 100% fix scenario
-- assumes every repeat restarter would have remained at the stop-save price.

DECLARE as_of_date DATE DEFAULT DATE '{{revenue_as_of_date}}';

CREATE TEMP TABLE p2_revenue_work AS
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
    perm_stop_date,
    GREATEST(
      pricing_effective_date,
      DATE_ADD(earlist_contact_date, INTERVAL 1 DAY)
    ) AS original_analysis_start_date,
    CASE
      WHEN Repeat_Restarts IN (
        'repeat restart via intro',
        'repeat restart via winback'
      )
        THEN REPLACE(Repeat_Restarts, 'repeat restart via ', '')
    END AS restart_type
  FROM `{{p2_combined_table}}`
),
restart_pairs AS (
  SELECT
    o.origin_id_subscrip,
    history.id_subscrip AS restart_id_subscrip,
    MIN(history.event_date) AS restart_date
  FROM origins o
  CROSS JOIN UNNEST(o.restart_history) AS history
  WHERE o.Repeat_Restarts IN (
      'repeat restart via intro',
      'repeat restart via winback'
    )
    AND history.id_subscrip IS NOT NULL
    AND history.event_date <= as_of_date
  GROUP BY
    o.origin_id_subscrip,
    history.id_subscrip
),
component_horizons AS (
  SELECT
    o.* EXCEPT (restart_history, original_analysis_start_date),
    'original' AS revenue_component,
    o.original_analysis_start_date AS analysis_start_date,
    h.horizon,
    h.horizon_days,
    as_of_date,
    CASE
      WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
      ELSE DATE_ADD(o.original_analysis_start_date, INTERVAL h.horizon_days DAY)
    END AS window_end_exclusive,
    LEAST(
      CASE
        WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
        ELSE DATE_ADD(o.original_analysis_start_date, INTERVAL h.horizon_days DAY)
      END,
      COALESCE(
        o.perm_stop_date,
        CASE
          WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
          ELSE DATE_ADD(o.original_analysis_start_date, INTERVAL h.horizon_days DAY)
        END
      )
    ) AS actual_window_end_exclusive,
    CASE
      WHEN h.horizon_days IS NULL
        THEN DATE_DIFF(
          DATE_ADD(as_of_date, INTERVAL 1 DAY),
          o.original_analysis_start_date,
          DAY
        )
      ELSE h.horizon_days
    END AS exposure_days
  FROM origins o
  CROSS JOIN horizon_definitions h
  WHERE o.original_analysis_start_date <= as_of_date
    AND (
      h.horizon_days IS NULL
      OR DATE_DIFF(
        DATE_ADD(as_of_date, INTERVAL 1 DAY),
        o.original_analysis_start_date,
        DAY
      )
        >= h.horizon_days
    )

  UNION ALL

  SELECT
    o.* EXCEPT (restart_history, original_analysis_start_date),
    'restart' AS revenue_component,
    o.perm_stop_date AS analysis_start_date,
    h.horizon,
    h.horizon_days,
    as_of_date,
    CASE
      WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
      ELSE DATE_ADD(o.perm_stop_date, INTERVAL h.horizon_days DAY)
    END AS window_end_exclusive,
    CASE
      WHEN h.horizon_days IS NULL THEN DATE_ADD(as_of_date, INTERVAL 1 DAY)
      ELSE DATE_ADD(o.perm_stop_date, INTERVAL h.horizon_days DAY)
    END AS actual_window_end_exclusive,
    CASE
      WHEN h.horizon_days IS NULL
        THEN DATE_DIFF(DATE_ADD(as_of_date, INTERVAL 1 DAY), o.perm_stop_date, DAY)
      ELSE h.horizon_days
    END AS exposure_days
  FROM origins o
  JOIN (
    SELECT DISTINCT origin_id_subscrip
    FROM restart_pairs
  ) r
    USING (origin_id_subscrip)
  CROSS JOIN horizon_definitions h
  WHERE o.perm_stop_date <= as_of_date
    AND (
      h.horizon_days IS NULL
      OR DATE_DIFF(DATE_ADD(as_of_date, INTERVAL 1 DAY), o.perm_stop_date, DAY)
        >= h.horizon_days
    )
),
invoice_classified AS (
  SELECT
    s.origin_id_subscrip,
    s.revenue_component,
    s.revenue_id_subscrip,
    s.component_start_date,
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
  FROM (
    SELECT
      origin_id_subscrip,
      'original' AS revenue_component,
      origin_id_subscrip AS revenue_id_subscrip,
      original_analysis_start_date AS component_start_date
    FROM origins
    WHERE original_analysis_start_date <= as_of_date

    UNION ALL

    SELECT
      origin_id_subscrip,
      'restart' AS revenue_component,
      restart_id_subscrip AS revenue_id_subscrip,
      restart_date AS component_start_date
    FROM restart_pairs
  ) s
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_invoice_payment` p
    ON p.id_subscrip = s.revenue_id_subscrip
  WHERE p.balance >= 0
),
paid_invoices AS (
  SELECT
    *,
    CAST(DATE_DIFF(service_end_exclusive, service_start, DAY) AS NUMERIC)
      AS service_period_days
  FROM invoice_classified
  WHERE payment_status = 'Paid'
    AND service_start >= component_start_date
    AND service_start < service_end_exclusive
    AND (
      revenue_component = 'restart'
      OR id_payment_date >= component_start_date
    )
),
rate_plan_flags AS (
  SELECT
    s.origin_id_subscrip,
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'YEAR|12M')) > 0
      AS had_annual_plan_by_as_of_date,
    COUNTIF(
      REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'\bFOR\b')
      OR r.monthly_rate <= 1.10
    ) > 0 AS had_promotional_plan_by_as_of_date,
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(m.description, '')), r'STOP[ -]?SAVE')) > 0
      AS had_restart_stop_save_by_as_of_date
  FROM restart_pairs s
  JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_rate_new` r
    ON r.id_subscrip = s.restart_id_subscrip
  LEFT JOIN `gannett-enterprise-data.consumers_rfz.rate_mapping_combined` m
    ON LOWER(TRIM(r.rate_key_system)) = LOWER(TRIM(m.rate_key_system))
    AND r.rate_key_value = m.rate_key_value
  WHERE r.effective_date BETWEEN s.restart_date AND as_of_date
  GROUP BY s.origin_id_subscrip
),
original_metrics AS (
  SELECT
    h.*,
    COUNT(DISTINCT i.id_invoice) AS all_paid_invoice_count,
    COUNT(DISTINCT IF(
      i.id_payment_date < h.actual_window_end_exclusive,
      i.id_invoice,
      NULL
    )) AS current_window_paid_invoice_count,
    COALESCE(SUM(
      CASE
        WHEN i.id_payment_date >= h.actual_window_end_exclusive THEN 0
        WHEN i.service_end_exclusive <= h.actual_window_end_exclusive
          THEN i.paid_invoice_value
        WHEN i.service_start <= h.actual_window_end_exclusive
          THEN i.paid_invoice_value * SAFE_DIVIDE(
            CAST(DATE_DIFF(h.actual_window_end_exclusive, i.service_start, DAY) AS NUMERIC),
            i.service_period_days
          )
        ELSE 0
      END
    ), 0) AS current_earned_revenue,
    COALESCE(SUM(
      CASE
        WHEN i.id_payment_date < h.actual_window_end_exclusive
          THEN i.paid_invoice_value
        ELSE 0
      END
    ), 0) AS current_paid_invoice_value,
    CAST(NULL AS NUMERIC) AS full_fix_revenue
  FROM component_horizons h
  LEFT JOIN paid_invoices i
    ON h.origin_id_subscrip = i.origin_id_subscrip
    AND h.revenue_component = i.revenue_component
  WHERE h.revenue_component = 'original'
  GROUP BY ALL
),
restart_metrics AS (
  SELECT
    h.*,
    COUNT(DISTINCT i.id_invoice) AS all_paid_invoice_count,
    COUNT(DISTINCT IF(
      i.id_payment_date < h.window_end_exclusive,
      i.id_invoice,
      NULL
    )) AS current_window_paid_invoice_count,
    COALESCE(SUM(
      CASE
        WHEN i.service_end_exclusive <= h.window_end_exclusive
          THEN i.paid_invoice_value
        WHEN i.service_start <= h.window_end_exclusive
          THEN i.paid_invoice_value * SAFE_DIVIDE(
            CAST(DATE_DIFF(h.window_end_exclusive, i.service_start, DAY) AS NUMERIC),
            i.service_period_days
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
    ), 0) AS current_paid_invoice_value,
    CAST(h.stop_save_price AS NUMERIC)
      * CAST(h.exposure_days AS NUMERIC)
      / CAST(30.4375 AS NUMERIC) AS full_fix_revenue
  FROM component_horizons h
  LEFT JOIN paid_invoices i
    ON h.origin_id_subscrip = i.origin_id_subscrip
    AND h.revenue_component = i.revenue_component
  WHERE h.revenue_component = 'restart'
  GROUP BY ALL
),
component_metrics AS (
  SELECT * FROM original_metrics
  UNION ALL
  SELECT * FROM restart_metrics
),
metrics_base AS (
  SELECT
    m.*,
    full_fix_revenue * CAST(0.25 AS NUMERIC) AS fix_revenue_25pct,
    full_fix_revenue * CAST(0.50 AS NUMERIC) AS fix_revenue_50pct,
    full_fix_revenue * CAST(0.75 AS NUMERIC) AS fix_revenue_75pct,
    CASE WHEN revenue_component = 'restart'
      THEN COALESCE(r.had_annual_plan_by_as_of_date, FALSE)
    END AS had_annual_plan_by_as_of_date,
    CASE WHEN revenue_component = 'restart'
      THEN COALESCE(r.had_promotional_plan_by_as_of_date, FALSE)
    END AS had_promotional_plan_by_as_of_date,
    CASE WHEN revenue_component = 'restart'
      THEN COALESCE(r.had_restart_stop_save_by_as_of_date, FALSE)
    END AS had_restart_stop_save_by_as_of_date
  FROM component_metrics m
  LEFT JOIN rate_plan_flags r
    USING (origin_id_subscrip)
),
metrics AS (
  SELECT
    *,
    full_fix_revenue - current_earned_revenue AS incremental_revenue_100pct,
    fix_revenue_25pct - current_earned_revenue AS incremental_revenue_25pct,
    fix_revenue_50pct - current_earned_revenue AS incremental_revenue_50pct,
    fix_revenue_75pct - current_earned_revenue AS incremental_revenue_75pct,
    SAFE_DIVIDE(current_earned_revenue, full_fix_revenue)
      AS break_even_acceptance_rate
  FROM metrics_base
)
SELECT
  *,
  IF(all_paid_invoice_count = 0, 'no_paid_invoices', '') AS data_quality_flags
FROM metrics;

CREATE OR REPLACE TABLE `{{p2_revenue_detail_table}}`
OPTIONS (
  description = 'Paid original and restart subscription revenue by observation horizon.'
)
AS
SELECT *
FROM p2_revenue_work;

CREATE OR REPLACE TABLE `{{p2_revenue_summary_table}}`
OPTIONS (
  description = 'Experiment revenue and repeat-restart scenarios by component and horizon.'
)
AS
WITH component_summary AS (
  SELECT
    as_of_date,
    horizon,
    horizon_days,
    revenue_component,
    restart_type,
    cohort,
    Treatment,
    COUNT(DISTINCT origin_id_subscrip) AS origin_subscriptions,
    SUM(IF(revenue_component = 'restart', COALESCE(new_subid_counts, 0), 0))
      AS restart_subscriptions,
    COUNTIF(data_quality_flags != '') AS origins_with_quality_flags,
    ROUND(SUM(current_earned_revenue), 2) AS current_earned_revenue,
    ROUND(SUM(current_paid_invoice_value), 2) AS current_paid_invoice_value,
    ROUND(SUM(full_fix_revenue), 2) AS full_fix_revenue,
    ROUND(SUM(fix_revenue_25pct), 2) AS fix_revenue_25pct,
    ROUND(SUM(fix_revenue_50pct), 2) AS fix_revenue_50pct,
    ROUND(SUM(fix_revenue_75pct), 2) AS fix_revenue_75pct,
    ROUND(SUM(incremental_revenue_25pct), 2) AS incremental_revenue_25pct,
    ROUND(SUM(incremental_revenue_50pct), 2) AS incremental_revenue_50pct,
    ROUND(SUM(incremental_revenue_75pct), 2) AS incremental_revenue_75pct,
    ROUND(SUM(incremental_revenue_100pct), 2) AS incremental_revenue_100pct,
    SAFE_DIVIDE(
      SUM(IF(full_fix_revenue IS NOT NULL, current_earned_revenue, 0)),
      SUM(full_fix_revenue)
    ) AS weighted_break_even_acceptance_rate
  FROM `{{p2_revenue_detail_table}}`
  GROUP BY ALL
),
all_components_summary AS (
  SELECT
    as_of_date,
    horizon,
    horizon_days,
    'all_components' AS revenue_component,
    restart_type,
    cohort,
    Treatment,
    COUNT(DISTINCT origin_id_subscrip) AS origin_subscriptions,
    SUM(IF(revenue_component = 'restart', COALESCE(new_subid_counts, 0), 0))
      AS restart_subscriptions,
    COUNTIF(data_quality_flags != '') AS origins_with_quality_flags,
    ROUND(SUM(current_earned_revenue), 2) AS current_earned_revenue,
    ROUND(SUM(current_paid_invoice_value), 2) AS current_paid_invoice_value,
    ROUND(SUM(full_fix_revenue), 2) AS full_fix_revenue,
    ROUND(SUM(fix_revenue_25pct), 2) AS fix_revenue_25pct,
    ROUND(SUM(fix_revenue_50pct), 2) AS fix_revenue_50pct,
    ROUND(SUM(fix_revenue_75pct), 2) AS fix_revenue_75pct,
    ROUND(SUM(incremental_revenue_25pct), 2) AS incremental_revenue_25pct,
    ROUND(SUM(incremental_revenue_50pct), 2) AS incremental_revenue_50pct,
    ROUND(SUM(incremental_revenue_75pct), 2) AS incremental_revenue_75pct,
    ROUND(SUM(incremental_revenue_100pct), 2) AS incremental_revenue_100pct,
    SAFE_DIVIDE(
      SUM(IF(full_fix_revenue IS NOT NULL, current_earned_revenue, 0)),
      SUM(full_fix_revenue)
    ) AS weighted_break_even_acceptance_rate
  FROM `{{p2_revenue_detail_table}}`
  GROUP BY
    as_of_date,
    horizon,
    horizon_days,
    restart_type,
    cohort,
    Treatment
)
SELECT * FROM component_summary
UNION ALL
SELECT * FROM all_components_summary;

SELECT *
FROM `{{p2_revenue_summary_table}}`
ORDER BY horizon_days, horizon, revenue_component, restart_type, cohort, Treatment;
