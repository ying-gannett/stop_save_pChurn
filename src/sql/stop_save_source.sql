with mkts as (
  select distinct
    m.id_subscrip, m.effective_date, m.end_date,
    CASE
      WHEN CONTAINS_SUBSTR(pub.publication_name, 'Bergen') THEN 'Bergen'
      WHEN CONTAINS_SUBSTR(pub.publication_name, 'Detroit') THEN 'Detroit'
      WHEN CONTAINS_SUBSTR(pub.publication_name, 'Palm Beach') THEN 'Palm Beach'
      ELSE pub.publication_name
      END
    AS market,
    pub.publication_id, pub.publication_name
  from `gannett-enterprise-data.consumers_curated_zone_assets.subscriptions_main` m 
  join `gannett-enterprise-data.mdm_cz.product` prod on
    m.mdm_product_id = prod.product_id
  join `gannett-enterprise-data.mdm_cz.publication` pub on
    prod.publication_id = pub.publication_id
),
lk as (
  SELECT distinct circ_idsubscrip, billing_account
  FROM`gannett-enterprise-data.consumers_linkage_cz.subscription_link_latest`
  where billing_system = 'ZUORA'
)
SELECT distinct
  t.inference_date,
  lk.billing_account,
  t.id_subscrip, t.risk_tier,
  mkts.market, mkts.publication_name
FROM `gannett-enterprise-data.models_sz.pchurn_do_risk_tiers` t
join mkts on 
  t.id_subscrip = mkts.id_subscrip 
  and t.inference_date between mkts.effective_date and mkts.end_date
join lk on
  t.id_subscrip = lk.circ_idsubscrip
WHERE
t.inference_date = DATE_TRUNC(DATE('{run_date}'), WEEK(SUNDAY))
and mkts.market in (  -- list of markets that participate this project
  "Cambridge",
  "Sarasota",
  "Peoria",
  "York Dispatch",
  "Marion",
  "DoverNH",
  "East Brunswick",
  "Massillon",
  "Palm Beach",
  "WilmingtonNC",
  "Daytona Beach",
  "Topeka",
  "Newark",
  "York Daily Record",
  "MonroeMI",
  "Beaver",
  "Lancaster",
  "Canandaigua",
  "Worcester",
  "Bloomington",
  "Mansfield",
  "Staunton",
  "Port Huron",
  "Zanesville",
  "Stroudsburg",
  "Anderson",
  "Thibodaux",
  "ND Insider",
  "Yreka",
  "Salinas",
  "Spencer",
  "Monmouth",
  "Geneseo",
  "Des Moines",
  "Cincinnati",
  "Naples",
  "Bergen",
  "Palm Springs",
  "Greenville",
  "Green Bay",
  "Louisville",
  "Asheville",
  "Brevard",
  "JacksonMS",
  "Pensacola",
  "Evansville",
  "Appleton",
  "Detroit",
  "Phoenix",
  "Indianapolis",
  "Milwaukee",
  "Rochester",
  "Alliance",
  "Hattiesburg",
  "Lubbock",
  "St. Augustine",
  "Utica",
  "ColumbiaTN",
  "Providence",
  "Erie",
  "JacksonvilleFL",
  "Kitsap",
  "Gainesville",
  "Wooster",
  "Spartanburg",
  "Fall River",
  "Norwich",
  "Adrian",
  "Wausau",
  "Bridgewater",
  "Fort Myers",
  "Corpus Christi",
  "Nashville",
  "Westchester",
  "Asbury",
  "Lansing",
  "Treasure Coast",
  "Reno",
  "Memphis",
  "Sioux Falls",
  "BurlingtonVT"
)

