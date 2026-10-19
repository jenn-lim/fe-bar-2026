# Evidence 01 — Lakeflow ingestion (raw → bronze → silver)

Captured 2026-10-19 15:01 EDT from workspace `fevm-jlim-fe-bar-2026` by `scripts/capture_evidence.py` — live output, not edited.

## Job run — every task in the end-to-end DAG
```
job:    [dev jen_lim] [SilverLeaf Demo] Member Personalization Pipeline (id 486067273141675)
run:    https://fevm-jlim-fe-bar-2026.cloud.databricks.com/?o=7474645358176029#job/486067273141675/run/1086036015642743
result: SUCCESS

generate_data                SUCCESS    13:26:17 -> 13:27:14  (57s)
ingest_raw                   SUCCESS    13:27:14 -> 13:28:27  (73s)
feature_engineering          SUCCESS    13:28:27 -> 13:29:39  (71s)
train_and_score              SUCCESS    13:29:39 -> 13:30:30  (51s)
evaluate_recommendations     SUCCESS    13:30:30 -> 13:31:21  (51s)
genai_member_messages        SUCCESS    13:30:30 -> 13:33:23  (173s)
governance_and_genie         SUCCESS    13:33:23 -> 13:33:56  (33s)
sync_to_lakebase             SUCCESS    13:33:23 -> 13:37:04  (221s)
```

## Lakeflow Declarative Pipeline update (Auto Loader raw JSON -> bronze -> silver)
```
pipeline: [dev jen_lim] [SilverLeaf Demo] Raw -> Bronze -> Silver Ingest (id b992b754-9733-427c-b50b-96604d342a80)
update:   2af923f2-62e0-4fd7-bec1-a2a7a79716cf  state=COMPLETED  full_refresh=True
```

## Raw files landed in the Unity Catalog Volume (the raw dataset)
```sql
SELECT regexp_extract(_metadata.file_path, '/raw/([a-z_]+)/', 1) AS feed,
       count(DISTINCT _metadata.file_path) AS json_files, count(*) AS raw_records
FROM json.`/Volumes/jlim_fe_bar_2026_catalog/personalization/raw/*/`
GROUP BY 1 ORDER BY 1
```
```
feed          json_files  raw_records
------------  ----------  -----------
members       8           2000
order_items   8           27760
orders        8           12734
products      8           124
web_sessions  8           5917
```

## Row counts across the medallion (bronze == silver -> no rows dropped)
```sql
SELECT 'members' AS feed, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.bronze_members) AS bronze_rows, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.members) AS silver_rows UNION ALL SELECT 'products' AS feed, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.bronze_products) AS bronze_rows, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.products) AS silver_rows UNION ALL SELECT 'orders' AS feed, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.bronze_orders) AS bronze_rows, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.orders) AS silver_rows UNION ALL SELECT 'order_items' AS feed, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.bronze_order_items) AS bronze_rows, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.order_items) AS silver_rows UNION ALL SELECT 'web_sessions' AS feed, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.bronze_web_sessions) AS bronze_rows, (SELECT count(*) FROM jlim_fe_bar_2026_catalog.personalization.web_sessions) AS silver_rows
```
```
feed          bronze_rows  silver_rows
------------  -----------  -----------
members       2000         2000
products      124          124
orders        12734        12734
order_items   27760        27760
web_sessions  5917         5917
```

## Data-quality expectations — results from the pipeline event log
```sql
SELECT origin.flow_name AS flow, e.name AS expectation,
       sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM event_log('b992b754-9733-427c-b50b-96604d342a80')
LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, 2
```
```
flow                                                   expectation       passed  failed
-----------------------------------------------------  ----------------  ------  ------
jlim_fe_bar_2026_catalog.personalization.members       plausible_age     4000    0
jlim_fe_bar_2026_catalog.personalization.members       valid_member_id   4000    0
jlim_fe_bar_2026_catalog.personalization.order_items   nonneg_price      55520   0
jlim_fe_bar_2026_catalog.personalization.order_items   positive_qty      55520   0
jlim_fe_bar_2026_catalog.personalization.orders        valid_date        25468   0
jlim_fe_bar_2026_catalog.personalization.orders        valid_member_id   25468   0
jlim_fe_bar_2026_catalog.personalization.orders        valid_order_id    25468   0
jlim_fe_bar_2026_catalog.personalization.products      positive_price    248     0
jlim_fe_bar_2026_catalog.personalization.products      valid_product_id  248     0
jlim_fe_bar_2026_catalog.personalization.web_sessions  nonneg_views      11834   0
```

## Silver typing — arrays and dates survive the JSON round-trip
```sql
SELECT member_id, age, enrollment_date, health_conditions, otc_wallet_monthly FROM jlim_fe_bar_2026_catalog.personalization.members ORDER BY member_id LIMIT 3
```
```
member_id  age  enrollment_date  health_conditions                                          otc_wallet_monthly
---------  ---  ---------------  ---------------------------------------------------------  ------------------
M100000    73   2023-05-11       ["vision_loss","hearing_loss"]                             150
M100001    65   2023-08-14       ["hypertension"]                                           75
M100002    72   2023-09-21       ["hypertension","arthritis","osteoporosis","vision_loss"]  150
```
