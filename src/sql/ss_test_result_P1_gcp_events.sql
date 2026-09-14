-- Check data issue: why 15.5% created perm without contact after pricing? -- EDE-14782
-- todo: Repeat restarts vs winbacks = 180 days vs 90 days

-- Part 1. Sourcing from GCP consumer_events
  declare launch_date Date default '2026-04-03';  -- first batch of pricing email in the stop-save test was sent
  create or replace table `gannett-datascience.test_results_zone.ss_test_result_v3-0_gcp_event`   -- 133,227 up to 08-19-2026
  as
  with src as (   -- raw events
    select *
    FROM `gannett-enterprise-data.consumers_nonpii_cz.consumer_events`
    WHERE event_timestamp >= launch_date
    and event_name in ("pricing_action_target", "contact_stop_save", "stop_save", 'snap_cancel_subscription_click_after_reason_selection', "stop_permanent", "start_new")
  ),
  priced as (     -- notify pricing of digital users every Sunday
    select * from (
      SELECT distinct 
        consumer_id,
        lower(trim(json_value(event_attributes, '$.account_number'))) as zuora_billing_account,
        cast(json_value(event_attributes, '$.id_subscrip') as int64) as id_subscrip,
        json_value(event_attributes, '$.website_id') as website_id,
        DATE_TRUNC(SAFE_CAST(json_value(event_attributes, '$.notice_date') as date), WEEK(FRIDAY)) as pricing_notice_date,
        SAFE_CAST(json_value(event_attributes, '$.pricing_effective_date') as date) as pricing_effective_date,
        cast(json_value(event_attributes, '$.pre_pricing_monthly_price') as float64) as pre_pricing_monthly_price,
        cast(json_value(event_attributes, '$.target_monthly_price') as float64) as target_monthly_price,
      FROM src
      WHERE event_name = "pricing_action_target"
      and json_value(event_attributes, '$.target') = "1"
    )
    QUALIFY 
    COUNT(DISTINCT id_subscrip) OVER(PARTITION BY zuora_billing_account) = 1
    and 
    COUNT(DISTINCT pricing_notice_date) OVER(PARTITION BY zuora_billing_account) = 1
  ),
  called as (     -- contacted the call center.
    select distinct
      event_timestamp as contact_call_time,
      case 
        when event_name = "contact_stop_save" then cast(json_value(event_attributes, '$.id_subscrip') as int64)
        else cast(json_value(event_attributes, '$.idsubscrip') as int64)
      end as id_subscrip,
      "Called" as contact_call
    FROM src
    where event_name in ("contact_stop_save", "stop_save")
  ),
  ol as (         -- online cancel flow at anon_id level
    select distinct
      event_timestamp as contact_ol_time,
      cast(json_value(event_attributes, '$.id_subscrip') as int64) as id_subscrip,
        -- json_value(event_attributes, '$.anon_id_d050') as anon_id, json_value(event_attributes, '$.website_id_d031') as website_id,
      "OL Cancel" as contact_ol,
    FROM src
    where event_name='snap_cancel_subscription_click_after_reason_selection'
    and json_value(event_attributes, '$.snap_flow_product') = 'digital'
  ),
  vol_perm as (   -- vol perm stops
    select * from (
      SELECT distinct 
        date(src.event_timestamp) as perm_stop_date,
        SAFE_CAST(json_value(src.event_attributes, '$.system_date') as date) as perm_stop_sys_date,
        cast(json_value(src.event_attributes, '$.id_subscrip') as int64) as id_subscrip,
        src.consumer_id,
        json_value(src.event_attributes, '$.website_id') as website_id,
        mdm.stop_code,
        mdm.perm_stop_voluntary as is_vol_perm
      FROM src
      join `gannett-enterprise-data.mdm_cz.subscriptions_stop_reasons` mdm
        on lower(trim(json_value(src.event_attributes, '$.coe_cnt_code'))) = lower(trim(mdm.stop_code)) 
      WHERE src.event_name = "stop_permanent" 
      and not REGEXP_CONTAINS(mdm.stop_descriptions, r'Chargeback|Dup')
        -- and mdm.stop_code not in (
        --   select distinct stop_code,
        --   from `gannett-enterprise-data.consumers_rfz.subscriptions_trans_start_stop`
        --   where is_perm_stop = 1
        --   and REGEXP_CONTAINS(stop_reason, r'Chargeback|Dup Start')
        -- )
    )
    QUALIFY 
    COUNT(DISTINCT perm_stop_sys_date) OVER(PARTITION BY id_subscrip) = 1
  ),
  new_start as (  -- new start
    select distinct
      date(event_timestamp) as event_date,
      SAFE_CAST(json_value(event_attributes, '$.system_date') as date) as system_date,
      consumer_id,
      json_value(event_attributes, '$.website_id') as website_id,
      cast(json_value(event_attributes, '$.idsubscrip') as int64) as id_subscrip,
      lower(trim(json_value(event_attributes, '$.gps_source_code'))) as gps_source_code,
      if(contains_substr(m.source_detail, 'win'), 1, 0) as is_winback
    FROM src
    left join `gannett-enterprise-data.mdm_cz.gps_source_mapping` m on
      lower(trim(json_value(event_attributes, '$.gps_source_code'))) = lower(trim(m.gps_source_code))
    WHERE event_name = "start_new"
  ),
  rejoin as (     -- rejoin: new starts within 90 days after vol perm stop (by consumer_id + website_id)
    select distinct
      ps.perm_stop_sys_date, ps.perm_stop_date, 
      ps.id_subscrip as perm_stop_subid, 
      count(distinct ns.id_subscrip) over (partition by ps.id_subscrip) as new_subid_counts,
      if(sum(ns.is_winback) over(partition by ps.id_subscrip)>0, 'repeat restart via winback', 'repeat restart via intro') as ever_winback_rejoin,
      ARRAY_AGG(ns.id_subscrip) OVER (PARTITION BY ps.id_subscrip ORDER BY ns.event_date
        rows between unbounded preceding and unbounded following
      ) AS new_start_subid_arr,
      ARRAY_AGG(ns.event_date) OVER ( PARTITION BY ps.id_subscrip ORDER BY ns.event_date
        rows between unbounded preceding and unbounded following
      ) AS new_start_date_arr,   
    from vol_perm ps 
    join new_start ns on
      ps.consumer_id = ns.consumer_id
      and ps.website_id = ns.website_id
      and ns.event_date between ps.perm_stop_date and date_add(ps.perm_stop_date, INTERVAL 90 day) 
    where ps.is_vol_perm is true
  ),
  combine as (
    select
      t.*,
      case
        when c.contact_call is null and o.contact_ol is null then "No Action yet"
        when c.contact_call is not null and o.contact_ol is not null then "Contacted both ways"
        else coalesce(c.contact_call, o.contact_ol)
      end as contact_channels,
      if(date_diff(c.contact_call_time, t.pricing_notice_date, day)<=90, 1, 0) as called_90d_of_notice, 
      if(date_diff(o.contact_ol_time, t.pricing_notice_date, day)<=90, 1, 0) as olContact_90d_of_notice, 
      count(distinct c.contact_call_time) over (partition by t.id_subscrip) as call_counts,   
      count(distinct o.contact_ol_time) over (partition by t.id_subscrip) as olContact_counts,
      min(c.contact_call_time) OVER (PARTITION BY t.id_subscrip) as min_contact_call_date,
      min(o.contact_ol_time) OVER (PARTITION BY t.id_subscrip) as min_contact_ol_date,
      s.perm_stop_sys_date, s.perm_stop_date, 
      s.is_vol_perm, stop_code,
      if(date_diff(s.perm_stop_date, t.pricing_notice_date, day) <= 90, 'Yes', 'No') perm_90d_of_notice,
      case
        when s.perm_stop_date is null then 0
        when s.is_vol_perm is true then 1
        else 2
      end as churn_code,
      if(r.new_subid_counts is null, 'No Rejoin', 'Rejoin') as has_rejoin,
      coalesce(r.ever_winback_rejoin, 'No Rejoin') as ever_winback_rejoin
    from priced t
    left join called c on     -- called after targeted
      t.id_subscrip = c.id_subscrip and c.contact_call_time >= t.pricing_notice_date
    left join ol o on         -- clicked cancel after targeted
      t.id_subscrip = o.id_subscrip and o.contact_ol_time >= t.pricing_notice_date
    left join vol_perm s on   -- create perm stopped after targeted
      t.id_subscrip = s.id_subscrip and s.perm_stop_sys_date >= t.pricing_notice_date
    left join rejoin r on     -- vol perm 90d of targeted and rejoin 90d of vol perm
      t.id_subscrip = r.perm_stop_subid
      and r.perm_stop_date between t.pricing_notice_date and date_add(t.pricing_notice_date, interval 90 day)
  ),
  features_1 as (
    select
      c.zuora_billing_account, c.id_subscrip, c.website_id,
      c.pricing_notice_date, c.pricing_effective_date, c.pre_pricing_monthly_price, c.target_monthly_price,
      case
        when c.contact_channels='Contacted both ways' then 
          if(
            c.min_contact_call_date >= c.min_contact_ol_date, 
            'Contacted both ways - Online first', 
            'Contacted both ways - Called-In first')
        else c.contact_channels
      end as contact_channels,
      case 
        when c.contact_channels="Contacted both ways" then least(c.min_contact_call_date, c.min_contact_ol_date)
        else coalesce(c.min_contact_call_date, c.min_contact_ol_date)
      end as earlist_contact_date,
      case
        when c.call_counts>1 then "2+ Called"
        when c.call_counts=1 then "1 Called"
      end as call_counts_tag, 
      case
        when c.olContact_counts>1 then "2+ OL Contact"
        when c.olContact_counts=1 then "1 OL Contact"
      end as ol_contact_counts_tag, 
      if(sum(c.called_90d_of_notice) over(partition by c.id_subscrip)>0, 1, 0) as called_90d_of_notice,
      if(sum(c.olContact_90d_of_notice) over(partition by c.id_subscrip)>0, 1, 0) as olContact_90d_of_notice,
      c.perm_stop_sys_date, c.perm_stop_date, 
      c.is_vol_perm, c.stop_code, c.perm_90d_of_notice,
      c.churn_code,
      c.has_rejoin,
      c.ever_winback_rejoin,
      case
        when c.contact_channels = 'No Action yet' and c.churn_code = 1 then 'Yes'
        when c.contact_channels != 'No Action yet' and c.churn_code = 2 then 'Yes'
        else 'No'
      end as conflict_tag
    from combine c
  ),
  feature_2 as (
    select distinct 
      zuora_billing_account, id_subscrip, website_id,
      pricing_notice_date, pricing_effective_date, 
      pre_pricing_monthly_price, target_monthly_price, 
      contact_channels, 
      case
        when contains_substr(contact_channels, "both ways") then concat(call_counts_tag, " | ", ol_contact_counts_tag)
        else coalesce(call_counts_tag, ol_contact_counts_tag, "No Action yet")
      end as contact_groups,
      case
        when ol_contact_counts_tag="2+ OL Contact" or call_counts_tag= "2+ Called" then '2+ Contacts'
        when ol_contact_counts_tag is not null or call_counts_tag is not null then '1 Contacts'
        else '0 Contact'
      end as multi_contact_points,
      earlist_contact_date,
      if(earlist_contact_date<pricing_effective_date, 'Contact Before Pricing', 'Contact On/After Pricing') as contact_timing,
      case 
        when olContact_90d_of_notice=1 or called_90d_of_notice=1 then 1
        else 0
      end as see_off_90d_of_notice,
      perm_90d_of_notice,
      perm_stop_sys_date, perm_stop_date, is_vol_perm, stop_code,
      churn_code,
      has_rejoin,
      ever_winback_rejoin,
      conflict_tag
    from features_1
  )
  select
    *,
    case
      when has_rejoin='No Rejoin' and contact_channels='Contacted both ways' then 'seek offer both channels'
      when has_rejoin='No Rejoin' and multi_contact_points = '2+ Contacts' then 'seek offer 2+'
      else "not repeate stop-saves"
    end as Repeat_StopSaves,
    case
      when has_rejoin='Rejoin' and see_off_90d_of_notice = 1 and perm_90d_of_notice = 'Yes' then ever_winback_rejoin
      else "not repeat restarts"
    end as Repeat_Restarts,
  from feature_2
  ;



