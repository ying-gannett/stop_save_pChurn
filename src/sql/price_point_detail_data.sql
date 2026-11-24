-- One row per mature subscription, ready for price-point analysis in pandas.

SELECT
  billing_account,
  id_subscrip,
  inference_date,
  email_date,
  pricing_effective_date,
  cohort,
  Treatment AS treatment,
  pricegroup,
  CASE
    WHEN pricegroup_order <= 4  THEN '1. <=4.99'
    WHEN pricegroup_order <= 9  THEN '2. <=9.99'
    WHEN pricegroup_order <= 14 THEN '3. <=14.99'
    WHEN pricegroup_order <= 16 THEN '4. <=16.99'
    WHEN pricegroup_order <= 19 THEN '5. <=19.99'
    ELSE '6. <=24.99'
  END AS start_price_group,
  start_price,
  step_up_price,
  stop_save_price,
  IFNULL(src_risk_tier, '-') AS risk_tier,
  market,
  tier AS market_tier,
  is_T1T2,
  is_selected_80Mkts,
  IF(CONTAINS_SUBSTR(contact_channels, 'both'),
     'Contacted both ways', contact_channels) AS contact_channel,
  contact_timing,
  IF(contact_channels = 'No Action yet', 0, 1) AS contacted,
  churn_code,
  CASE
    WHEN Repeat_Restarts = 'not repeat restarts'
     AND Repeat_StopSaves = 'not repeat stop-saves' THEN 'Not Gamer'
    WHEN Repeat_StopSaves = 'not repeat stop-saves' THEN 'Repeat Restart'
    WHEN CONTAINS_SUBSTR(Repeat_StopSaves, 'not')
      THEN 'Seek lower rate, still paid offered rate'
    ELSE 'Seek and paid lower rate'
  END AS gamer_type,
  CAST(revenue_single_saved AS FLOAT64) AS revenue_single_saved,
  cnt_payments
FROM `gannett-datascience.test_results_zone.ss_test_P3_usages_features`
WHERE is_two_payment_cycle_ago = 1
  AND (churn_code IS NULL OR churn_code != 2)
