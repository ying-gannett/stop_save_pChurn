
create or replace table `gannett-datascience.test_results_zone.ss_test_result_usage_analysis`
as
with src as (
  select * from `gannett-datascience.test_results_zone.ss_test_result_p1_p2_combined`
),
kc as (   -- id_sub | anony_id_1,2,3 mappings
  select distinct 
    anonymous_id_1,
    anonymous_id_2,
    anonymous_id_3,
    CAST(subs AS INT64) AS id_subscrip, 
  from `gannett-enterprise-data.consumers_nonpii_cz.known_consumers`,
  UNNEST(SPLIT(idsubscrip, ',')) AS subs
),
unpivoted as (  -- unpivot anony_id
  select distinct
    inference_date,
    id_subscrip, 
    website_id,
    anonymous_id,
  from (
    select
      src.id_subscrip, src.website_id, src.inference_date,
      kc.* except(id_subscrip)
    from src
    join kc on
      src.id_subscrip = kc.id_subscrip
  )
  UNPIVOT (anonymous_id FOR anon IN (anonymous_id_1, anonymous_id_2, anonymous_id_3)) 
),
rfv as (
  select 
    * except(rfv_bin),
    case 
      when contains_substr(rfv_bin, 'SuperUser') then 'SuperUser'
      when contains_substr(rfv_bin, 'Enthusiast') then 'Enthusiast'
      when contains_substr(rfv_bin, 'Moderate') then 'Moderate'
      when contains_substr(rfv_bin, 'Indifferent') then 'Indifferent'
      when contains_substr(rfv_bin, 'AtRisk') then 'AtRisk'
      else 'Zombie'
    end as rfv_bin
  from (
    select
      unpivoted.* except(anonymous_id, website_id), 
      max(rfv.modelscore) as modelscore, STRING_AGG(distinct rfv.rfv_bin, ' | ') as rfv_bin
    from unpivoted
    join `gannett-enterprise-data.models_cz.rfv_by_site_code` rfv on
      rfv.anonymous_id = unpivoted.anonymous_id
      AND rfv.site_code = unpivoted.website_id
      and rfv.file_date = unpivoted.inference_date
    group by 1, 2
  )
),
ga as (   -- rank 1 platform in the past 90d
  select
    inference_date, id_subscrip, 
    STRING_AGG(favor_platform, ' | ') AS favor_platforms, 
    avg(freq) as session_per_day_in_90d
  from (
    select
      unpivoted.* except(anonymous_id, website_id), 
      g.platform as favor_platform,
      sum(session_cnt)/90 as freq   -- total sessions/ 90 days
    from unpivoted
    join `gannett-datascience.test_activation_zone.ss_test_ga4_platform` g on
      g.anonymous_id = unpivoted.anonymous_id
      AND g.site_code = unpivoted.website_id
      and g.event_date between date_sub(unpivoted.inference_date, INTERVAL 90 day) and unpivoted.inference_date
    group by unpivoted.inference_date, unpivoted.id_subscrip, g.platform
    qualify rank() over(partition by inference_date, id_subscrip order by freq desc) = 1
  )
  group by all
)
select 
  src.*, 
  m.market, m.tier, m.is_T1T2, m.is_selected_80Mkts,
  rfv.modelscore, rfv.rfv_bin,
  ga.favor_platforms, ga.session_per_day_in_90d
from src
join `gannett-datascience.test_activation_zone.stop_save_selected_80_mkts_with_subid` m on
  src.id_subscrip = m.id_subscrip
  and src.inference_date between m.effective_date and m.end_date
left join rfv on 
  src.id_subscrip = rfv.id_subscrip
  and src.inference_date = rfv.inference_date
left join ga on
  src.id_subscrip = ga.id_subscrip
  and src.inference_date = ga.inference_date





