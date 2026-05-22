-- check if any billing account in stop_save_test_applied_Bart has 1+ id_subscrip, 
-- which would make it hard to link online cancel and payment data at the id_subscrip level

with m as (
  select count(*) as cnt
  from `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
  where modeltype = 'MIDPOINT' 
    and lower(trim(subscription)) in (
      SELECT distinct lower(trim(l.billing_account))
      from `gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest` l 
      where l.billing_system = 'ZUORA' and circ_status = 'Active' and circ_site != 'PLAY'
      group by 1 having count(distinct l.circ_idsubscrip) > 1
    )
),
p as (
select count(*) as cnt
from `gannett-datascience.test_results_zone.stop_save_test_applied_Bart`
where modeltype = 'PCHURN' 
  and lower(trim(subscription)) in (
    SELECT distinct lower(trim(billing_account))
    from `gannett-datascience.test_activation_zone.stop_save_test_Bart`
    group by 1 having count(distinct id_subscrip) > 1
  )
)
SELECT 
    CASE 
        WHEN (SELECT cnt FROM m) = 0 
         AND (SELECT cnt FROM p) = 0 
        THEN 'No violations' 
        ELSE 'Some violations' 
    END AS Result;
