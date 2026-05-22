--  this almost work. CNT: 
-- 1. 1 billing account -> N id_sub, so N monthly rate, how to handle?
-- 2. churned, but rate continue, how to handle?
-- 3. not churned, how to calculate cum revenue?

with sub_lnk as (   -- link billing_account and id_subscrip
  SELECT distinct 
    lower(trim(l.billing_account)) as billing_account, m.id_subscrip,
  FROM `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_main` m 
  join `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l on
    m.id_subscrip = l.circ_idsubscrip
  join `gannett-enterprise-data.mdm_cz.product` p on 
    m.mdm_product_id = p.product_id
  join `gannett-enterprise-data.mdm_cz.publication` pu on
    p.publication_id = pu.publication_id
  where pu.publication_name != 'USA TODAY Play' and l.billing_system = 'ZUORA'
),
t as (
  select
    billing_account, current_rate, email_date, 
    case 
      when __call_cancel_date is not null and __ol_cancel_date is not null 
      then least(__call_cancel_date, __ol_cancel_date)
      else coalesce(__call_cancel_date, __ol_cancel_date)
    end as __cancel_date,
    modeltype
  from `gannett-datascience.test_results_zone.ss_test_result_v2`
)

SELECT DISTINCT 
  t.*,
  sub_lnk.billing_account,
  srn.id_subscrip,
  srn.effective_date, srn.end_date,
  ROUND(GREATEST(CAST(rmc.monthly_price AS FLOAT64), 0), 2) AS month_cost
FROM t
join sub_lnk on
  t.billing_account = sub_lnk.billing_account
left JOIN `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_rate_new` srn
    ON sub_lnk.id_subscrip = srn.id_subscrip
    and srn.end_date >= t.email_date
left JOIN `gannett-enterprise-data.mdm_cz.rate_mapping_combined` rmc
    ON rmc.rate_key_value = srn.rate_key_value
    AND LOWER(rmc.rate_key_system) = LOWER(srn.rate_key_system)


