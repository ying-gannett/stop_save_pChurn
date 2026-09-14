-- 0. verify unique key 
  select 
  'GCP source', count(*) row_cnt, count(distinct zuora_billing_account) account_cnt, count(distinct id_subscrip) idsub_cnt   
  -- from `gannett-datascience.test_results_zone.ss_test_result_v3-1_source_gcp_event`
  from `gannett-datascience.test_results_zone.ss_test_result_v3-0_gcp_event`      -- 133,229
  union all
  select
  'Manual source', count(*), count(distinct billing_account), count(distinct id_subscrip)  
  -- from `gannett-datascience.test_results_zone.ss_test_result_v3-2` y             -- 73977 unique account/idsub. 
  from `gannett-datascience.test_results_zone.ss_test_result_p1_p2_combined` y      -- 75048 unique account/idsub. 
  where y.id_subscrip is not null and y.billing_account != 'mc-s000207628' and y.id_subscrip not in (49964082, 79445242);
--

-- 1. verify overlap of two sources: GCP miss 0.4%(272out of 73977)
  SELECT 
    if(g.id_subscrip is null, 'Not found in GCP consumer_events table', 'Overlap of GCP and manual extraction') as is_Overlap,
    count(*), count(distinct y.id_subscrip) 
  FROM `gannett-datascience.test_results_zone.ss_test_result_v3-2` y
  left join `gannett-datascience.test_results_zone.ss_test_result_v3-1_source_gcp_event` g on
    g.id_subscrip = y.id_subscrip
  group by 1;
--

-- 2. Compare two versions from different source tables: reasonablelly match
  select
    count(distinct id_subscrip) as vol,   -- 73705
      COUNTIF(email_date IS NOT DISTINCT FROM pricing_notice_date) AS match_email_date_cnt,   -- pass
      COUNTIF(email_date IS DISTINCT FROM pricing_notice_date) AS different_email_date_cnt,   
      COUNTIF(pricing_effective_date_y IS NOT DISTINCT FROM pricing_effective_date_g) AS match_pricing_date_cnt,  -- pass
      COUNTIF(pricing_effective_date_y IS DISTINCT FROM pricing_effective_date_g) AS different_pricing_date_cnt,  
    COUNTIF(start_price IS NOT DISTINCT FROM pre_pricing_monthly_price) AS match_start_price_cnt,   -- check!! 0 match
    COUNTIF(start_price IS DISTINCT FROM pre_pricing_monthly_price) AS different_start_price_cnt,  
      COUNTIF(step_up_price IS NOT DISTINCT FROM target_monthly_price) AS match_step_up_price_cnt,   -- pass
      COUNTIF(step_up_price IS DISTINCT FROM target_monthly_price) AS different_step_up_price_cnt,  
    COUNTIF(contact_channel_y IS NOT DISTINCT FROM contact_channel_g) AS match_contact_cnt,  -- 97.4%(71789) match vs 1916 diff
    COUNTIF(contact_channel_y IS DISTINCT FROM contact_channel_g) AS different_contact_cnt,
    COUNTIF(contact_groups_y IS NOT DISTINCT FROM contact_groups_g) AS match_contact_group,  -- 97.4%(71580) match vs 2125 miss
    COUNTIF(contact_groups_y IS DISTINCT FROM contact_groups_g) AS different_contact_group,
    COUNTIF(__create_date IS NOT DISTINCT FROM perm_stop_sys_date) AS match_stop_sys_date_cnt,    -- 99.1%(73081) match vs 624 miss
    COUNTIF(__create_date IS DISTINCT FROM perm_stop_sys_date) AS different_stop_sys_date_cnt,   
      COUNTIF(perm_stop_date_y IS NOT DISTINCT FROM perm_stop_date_g) AS match_stop_date_cnt,    -- 99.1%(73081) match vs 624 miss
      COUNTIF(perm_stop_date_y IS DISTINCT FROM perm_stop_date_g) AS different_stop_date_cnt,   
    COUNTIF(is_vol_perm_y IS NOT DISTINCT FROM is_vol_perm_g) AS match_is_vol_perm_cnt,    -- 99.3%(73169) match vs 536 miss
    COUNTIF(is_vol_perm_y IS DISTINCT FROM is_vol_perm_g) AS different_is_vol_perm_cnt,   
  from (
    select
      y.id_subscrip,
      y.email_date, y.pricing_effective_date as pricing_effective_date_y, y.start_price, y.step_up_price, 
      g.pricing_notice_date, g.pricing_effective_date as pricing_effective_date_g, g.pre_pricing_monthly_price, g.target_monthly_price,
      case 
        when y.contact_channel='No Action yet' then 0
        when y.contact_channel='Called-In Cancel Flow' then 1
        when y.contact_channel='Online Cancel Flow' then 2
      else 3 end as contact_channel_y,
      case 
        when g.contact_channels='No Action yet' then 0
        when g.contact_channels='Called' then 1
        when g.contact_channels='OL Cancel' then 2
      else 3 end as contact_channel_g,
      case 
        when y.call_counts + y.click_cancel_counts = 0 then 'No Action yet'
        when y.call_counts>1 and y.click_cancel_counts>1 then '2+ Called | 2+ OL Contact'
        when y.call_counts>1 and y.click_cancel_counts=1 then '2+ Called | 1 OL Contact'
        when y.call_counts>1 and y.click_cancel_counts=0 then '2+ Called'
        when y.click_cancel_counts>1 and y.call_counts=1 then '1 Called | 2+ OL Contact'
        when y.click_cancel_counts>1 and y.call_counts=0 then '2+ OL Contact'
        when y.click_cancel_counts=1 and y.call_counts=1 then '1 Called | 1 OL Contact'
        when y.click_cancel_counts=1 then '1 OL Contact'
        else '1 Called'
      end as contact_groups_y,
      g.contact_groups as contact_groups_g,
      y.__create_date, y.perm_stop_date as perm_stop_date_y, y.is_vol_perm as is_vol_perm_y, 
      g.perm_stop_sys_date, g.perm_stop_date as perm_stop_date_g, coalesce(g.is_vol_perm, false) as is_vol_perm_g
    from `gannett-datascience.test_results_zone.ss_test_result_v3-2` y    
    join `gannett-datascience.test_results_zone.ss_test_result_v3-1_source_gcp_event` g on
      y.id_subscrip = g.id_subscrip
    where y.id_subscrip is not null and y.billing_account != 'mc-s000207628' and y.id_subscrip not in (49964082, 79445242, 76087016)
  );
--

-- sample list for Q4 !!!!
  select distinct
    id_subscrip, pricing_notice_date, perm_stop_sys_date, perm_stop_date, is_vol_perm, pricing_effective_date
  FROM `gannett-datascience.test_results_zone.ss_test_result_v3-1_source_gcp_event`
  where 
  id_subscrip in (    -- 1. limit to 76297 valid experiment idsub 
    select distinct id_subscrip from `gannett-datascience.test_results_zone.ss_test_result_v3-1` y 
    where y.id_subscrip is not null and y.billing_account != 'mc-s000207628' and y.id_subscrip not in (49964082, 79445242, 76087016)
  )
  and perm_stop_date is not null    -- 2. 6448 perm stop of 69183
  and is_vol_perm is true           -- 3. 5291 vol perm of 6492
  and contact_channels = "No Action yet"; -- source_gcp_event intentionally excluded these. Need to change query to re-get this number!!

  select
    id_subscrip, email_date, __create_date, perm_stop_date, is_vol_perm, stop_reason, pricing_effective_date
  from `gannett-datascience.test_results_zone.ss_test_result_v3-1` y 
  where y.id_subscrip is not null and y.billing_account != 'mc-s000207628' and y.id_subscrip not in (49964082, 79445242, 76087016) and perm_stop_date is not null   -- 6355 perm stop of 53,309
  -- and is_vol_perm is true   -- 5451 vol perm of 6355
  and contact_channel = "No Action yet"     -- 1647 not contacted after pricing email
  and __create_date >= email_date;          -- 1647/5451 created perm without contact after pricing.(30%)

--

-- start price is different
  with src as (   -- raw events
  select *
  FROM `gannett-enterprise-data.consumers_nonpii_cz.consumer_events`
  WHERE event_timestamp >= '2026-04-03' -- first batch of pricing email in the stop-save test was sent
  and event_name in ("pricing_action_target", "contact_stop_save", "stop_save", 'snap_cancel_subscription_click_after_reason_selection', "stop_permanent", "start_new")
  )

  SELECT 
    consumer_id, event_attributes
    -- json_value(event_attributes, '$.account_number') as zuora_billing_account,
    -- cast(json_value(event_attributes, '$.id_subscrip') as int64) as id_subscrip,
    -- json_value(event_attributes, '$.website_id') as website_id,
    -- DATE_TRUNC(SAFE_CAST(json_value(event_attributes, '$.notice_date') as date), WEEK(FRIDAY)) as pricing_notice_date,    -- same as date(event_timestamp)
    -- SAFE_CAST(json_value(event_attributes, '$.pricing_effective_date') as date) as pricing_effective_date,
    -- cast(json_value(event_attributes, '$.pre_pricing_monthly_price') as float64) as pre_pricing_monthly_price,
    -- cast(json_value(event_attributes, '$.target_monthly_price') as float64) as target_monthly_price,
  FROM src
  WHERE event_name = "pricing_action_target"
  and json_value(event_attributes, '$.target') = "1"
  and cast(json_value(event_attributes, '$.id_subscrip') as int64) = 73868135;

  select *
  from `gannett-datascience.test_results_zone.ss_test_result_v3-2` y 
  where id_subscrip = 73868135;


  