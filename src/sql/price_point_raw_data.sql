-- Rebuilds the "Price Point start-end price point raw data" pivot export
-- (Looker Studio report, tab 4 "Price point distribution") directly from BigQuery.
--
-- Mirrors:
--   * the report's custom-SQL data source (only the columns this export needs)
--   * calculated field  gamer_flag
--   * calculated field  Total Paid = SUM(revenue_single_saved)
--   * dropdown filters  is_two_payment_cycle_ago = 1
--                       contact_channels IN ('No Action yet','Called','Contacted both ways')
--   * pivot rows/columns -> one output row per dimension combination, metrics Record Count + Total Paid
--   * workbook helper fields Contact Status and Gamer Status

WITH src AS (  -- = Looker Studio data source (custom SQL)
  SELECT
    pricegroup,
    Cohort,
    Treatment,
    start_price,
    step_up_price,
    stop_save_price,
    src_risk_tier,
    cnt_payments,
    is_two_payment_cycle_ago,
    Repeat_Restarts,
    Repeat_StopSaves,
    revenue_single_saved,
    IF(CONTAINS_SUBSTR(contact_channels, 'both'), 'Contacted both ways', contact_channels) AS contact_channels,
    CASE
      WHEN pricegroup_order <= 4  THEN '1. <=4.99'
      WHEN pricegroup_order <= 9  THEN '2. <=9.99'
      WHEN pricegroup_order <= 14 THEN '3. <=14.99'
      WHEN pricegroup_order <= 16 THEN '4. <=16.99'
      WHEN pricegroup_order <= 19 THEN '5. <=19.99'
      ELSE '6. <=24.99'
    END AS grouped_start_price_group,
    CASE
      WHEN churn_code = 1 THEN 'Vol Perm Stopped'
      WHEN churn_code = 0 THEN 'Saved'
      ELSE 'No Action yet'
    END AS Status,
    CASE
      WHEN Repeat_Restarts = 'not repeat restarts'
       AND Repeat_StopSaves = 'not repeate stop-saves' THEN 'Not Gamer'
      WHEN Repeat_StopSaves = 'not repeate stop-saves' THEN 'Repeat Restart'
      WHEN CONTAINS_SUBSTR(Repeat_StopSaves, "not") THEN 'Seek lower rate, still paid offered rate'  -- CONTAINS_TEXT is case-sensitive
      ELSE 'Seek and paid lower rate'
    END AS gamer_flag,
  FROM `gannett-datascience.test_results_zone.ss_test_P2_combined_results`
  WHERE churn_code IS NULL OR churn_code != 2
),
aggregated AS (
SELECT
  grouped_start_price_group,
  pricegroup,
  Cohort,
  Treatment,
  start_price,
  step_up_price,
  stop_save_price,
  IFNULL(src_risk_tier, '-')                 AS src_risk_tier,  -- the export writes nulls as '-'
  Status,
  gamer_flag,
  COUNT(*)                                   AS `Record Count`,
  ROUND(IFNULL(SUM(revenue_single_saved), 0), 4) AS `Total Paid`,
  cnt_payments
FROM src
WHERE is_two_payment_cycle_ago = 1
  AND contact_channels IN ('No Action yet', 'Called', 'Contacted both ways')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13
)
SELECT  -- column order matches the workbook's "price point raw data" table
  grouped_start_price_group,
  pricegroup,
  Cohort,
  Treatment,
  start_price,
  step_up_price,
  stop_save_price,
  src_risk_tier,
  Status,
  gamer_flag,
  `Record Count`,
  `Total Paid`,
  cnt_payments,
  IF(Status = 'No Action yet', 'No Action yet', 'Contacted') AS `Contact Status`,
  IF(gamer_flag = 'Not Gamer', 'Not Gamer', 'Gamer')        AS `Gamer Status`
FROM aggregated
ORDER BY 1, 2, 5, 6, 3, 4, 7, 8, 13, 9, 10
