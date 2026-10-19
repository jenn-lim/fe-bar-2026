# Evidence 02 — Unity Catalog governance

Captured 2026-10-19 15:05 EDT from workspace `fevm-jlim-fe-bar-2026` by `scripts/capture_evidence.py` — live output, not edited.

Catalog `jlim_fe_bar_2026_catalog`, schema `personalization`. All data synthetic (no PHI).

## Objects in the schema (medallion, ML, GenAI, serving, governed views)
```sql
SELECT table_name, table_type, comment FROM jlim_fe_bar_2026_catalog.information_schema.tables
WHERE table_schema = 'personalization'
  AND NOT startswith(table_name, '__materialization') AND NOT startswith(table_name, 'event_log_')
ORDER BY table_type, table_name
```
```
table_name                       table_type       comment
-------------------------------  ---------------  ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
category_recommendations_online  FOREIGN          Database table synced from another UC table
members_online                   FOREIGN          Database table synced from another UC table
recommendations_online           FOREIGN          Database table synced from another UC table
category_recommendations         MANAGED          NULL
guardrail_selftest               MANAGED          NULL
member_features                  MANAGED          NULL
member_messages                  MANAGED          Per-member home-page welcome banner: ai_query-generated and guardrail-checked, template fallback on any failed check (notebook 03c).
member_messages_checked          MANAGED          NULL
member_messages_raw              MANAGED          NULL
member_product_interactions      MANAGED          NULL
members_serving                  MANAGED          NULL
product_popularity               MANAGED          NULL
product_popularity_by_condition  MANAGED          NULL
recommendation_offline_eval      MANAGED          Temporal-holdout offline evaluation of home-page recommenders (written by notebook 03b).
recommendations                  MANAGED          Per-member home-page product recommendations. One row per (member_id, module, rank).
bronze_members                   STREAMING_TABLE  Raw members feed (JSON), ingested with Auto Loader.
bronze_order_items               STREAMING_TABLE  Raw order line-items feed (JSON), ingested with Auto Loader.
bronze_orders                    STREAMING_TABLE  Raw orders feed (JSON), ingested with Auto Loader.
bronze_products                  STREAMING_TABLE  Raw products catalog feed (JSON), ingested with Auto Loader.
bronze_web_sessions              STREAMING_TABLE  Raw web-sessions feed (JSON), ingested with Auto Loader.
members                          STREAMING_TABLE  Members: cleaned + typed (silver).
order_items                      STREAMING_TABLE  Order line-items: cleaned + typed (silver).
orders                           STREAMING_TABLE  Orders: cleaned + typed (silver).
products                         STREAMING_TABLE  Products catalog: cleaned + typed (silver).
web_sessions                     STREAMING_TABLE  Web sessions: cleaned + typed (silver).
members_analyst_v                VIEW             De-identified members for broad analytics: no names, age banded, health conditions masked unless silverleaf-care-team.
product_catalog                  VIEW             Shopper-facing product catalog (attributes + overall popularity only). The ONLY object the SilverLeaf Product Finder Genie Agent and the app can read in Unity Catalog — no member, order, or spend data.
```

## Classification tags — tables
```sql
SELECT table_name, tag_name, tag_value FROM jlim_fe_bar_2026_catalog.information_schema.table_tags
WHERE schema_name = 'personalization' AND startswith(tag_name, 'silverleaf_')
ORDER BY table_name, tag_name
```
```
table_name                   tag_name                tag_value
---------------------------  ----------------------  ---------------
category_recommendations     silverleaf_data_origin  synthetic
category_recommendations     silverleaf_domain       personalization
category_recommendations     silverleaf_layer        serving
member_features              silverleaf_contains     member_health
member_features              silverleaf_data_origin  synthetic
member_features              silverleaf_domain       member
member_features              silverleaf_layer        gold
member_messages              silverleaf_contains     genai_output
member_messages              silverleaf_data_origin  synthetic
member_messages              silverleaf_domain       personalization
member_messages              silverleaf_layer        serving
members                      silverleaf_contains     member_health
members                      silverleaf_data_origin  synthetic
members                      silverleaf_domain       member
members                      silverleaf_layer        silver
order_items                  silverleaf_data_origin  synthetic
order_items                  silverleaf_domain       commerce
order_items                  silverleaf_layer        silver
orders                       silverleaf_data_origin  synthetic
orders                       silverleaf_domain       commerce
orders                       silverleaf_layer        silver
products                     silverleaf_data_origin  synthetic
products                     silverleaf_domain       catalog
products                     silverleaf_layer        silver
recommendation_offline_eval  silverleaf_data_origin  synthetic
recommendation_offline_eval  silverleaf_domain       personalization
recommendation_offline_eval  silverleaf_layer        ml
recommendations              silverleaf_data_origin  synthetic
recommendations              silverleaf_domain       personalization
recommendations              silverleaf_layer        serving
web_sessions                 silverleaf_data_origin  synthetic
web_sessions                 silverleaf_domain       engagement
web_sessions                 silverleaf_layer        silver
```

## Classification tags — sensitive columns
```sql
SELECT table_name, column_name, tag_name, tag_value
FROM jlim_fe_bar_2026_catalog.information_schema.column_tags
WHERE schema_name = 'personalization' AND startswith(tag_name, 'silverleaf_')
ORDER BY table_name, column_name
```
```
table_name       column_name        tag_name                tag_value
---------------  -----------------  ----------------------  -------------
member_features  health_conditions  silverleaf_sensitivity  member_health
members          first_name         silverleaf_sensitivity  member_pii
members          health_conditions  silverleaf_sensitivity  member_health
members          last_name          silverleaf_sensitivity  member_pii
```

## Column-mask function (health conditions visible only to the care-team group)
```sql
SELECT routine_name, comment, routine_definition
FROM jlim_fe_bar_2026_catalog.information_schema.routines
WHERE routine_schema = 'personalization' AND routine_name = 'mask_health_conditions'
```
```
routine_name            comment                                                                                routine_definition
----------------------  -------------------------------------------------------------------------------------  ------------------------------------------------------------------------------------------
mask_health_conditions  Column mask: reveals member health conditions only to the silverleaf-care-team group.  CASE WHEN is_account_group_member('silverleaf-care-team') THEN conds ELSE array('***') END
```

## De-identified analyst view — names dropped, age banded, conditions masked
```sql
SELECT * FROM jlim_fe_bar_2026_catalog.personalization.members_analyst_v ORDER BY member_id LIMIT 5
```
```
member_id  age_band  state  plan_type               enrollment_date  otc_wallet_monthly  health_conditions
---------  --------  -----  ----------------------  ---------------  ------------------  -----------------
M100000    65-74     MI     C-SNP                   2023-05-11       150                 ["***"]
M100001    65-74     NC     Medicare Advantage HMO  2023-08-14       75                  ["***"]
M100002    65-74     AZ     C-SNP                   2023-09-21       150                 ["***"]
M100003    75-84     NY     D-SNP                   2023-12-26       100                 ["***"]
M100004    under 65  AZ     Medicare Advantage PPO  2023-01-06       100                 ["***"]
```

## Least privilege — every UC grant held by the app service principal (`e8c1826c-bf4c-44c4-81b2-915889f11934`)
```sql
SELECT 'CATALOG' AS level, catalog_name AS object, privilege_type
FROM system.information_schema.catalog_privileges
WHERE grantee = 'e8c1826c-bf4c-44c4-81b2-915889f11934' AND catalog_name = 'jlim_fe_bar_2026_catalog'
UNION ALL
SELECT 'SCHEMA', concat(catalog_name, '.', schema_name), privilege_type
FROM system.information_schema.schema_privileges
WHERE grantee = 'e8c1826c-bf4c-44c4-81b2-915889f11934' AND catalog_name = 'jlim_fe_bar_2026_catalog'
UNION ALL
SELECT 'TABLE', concat(table_catalog, '.', table_schema, '.', table_name), privilege_type
FROM system.information_schema.table_privileges
WHERE grantee = 'e8c1826c-bf4c-44c4-81b2-915889f11934' AND table_catalog = 'jlim_fe_bar_2026_catalog'
```
```
level    object                                                    privilege_type
-------  --------------------------------------------------------  --------------
CATALOG  jlim_fe_bar_2026_catalog                                  USE_CATALOG
SCHEMA   jlim_fe_bar_2026_catalog.personalization                  USE_SCHEMA
TABLE    jlim_fe_bar_2026_catalog.personalization.product_catalog  SELECT
```

The app SP can read `product_catalog` and nothing else in Unity Catalog; member data reaches the app only through Lakebase (see 03).

## Lineage — what the serving table was built from (system.access.table_lineage)
```sql
SELECT DISTINCT source_table_full_name, target_table_full_name
FROM system.access.table_lineage
WHERE target_table_catalog = 'jlim_fe_bar_2026_catalog'
  AND target_table_name IN ('recommendations', 'member_messages', 'members_serving', 'product_catalog')
  AND source_table_full_name IS NOT NULL
ORDER BY target_table_full_name, source_table_full_name
```
```
source_table_full_name                                                    target_table_full_name
------------------------------------------------------------------------  --------------------------------------------------------
jlim_fe_bar_2026_catalog.personalization.member_messages_checked          jlim_fe_bar_2026_catalog.personalization.member_messages
jlim_fe_bar_2026_catalog.personalization.member_messages                  jlim_fe_bar_2026_catalog.personalization.members_serving
jlim_fe_bar_2026_catalog.personalization.members                          jlim_fe_bar_2026_catalog.personalization.members_serving
jlim_fe_bar_2026_catalog.personalization.product_popularity               jlim_fe_bar_2026_catalog.personalization.product_catalog
jlim_fe_bar_2026_catalog.personalization.products                         jlim_fe_bar_2026_catalog.personalization.product_catalog
jlim_fe_bar_2026_catalog.personalization.members                          jlim_fe_bar_2026_catalog.personalization.recommendations
jlim_fe_bar_2026_catalog.personalization.order_items                      jlim_fe_bar_2026_catalog.personalization.recommendations
jlim_fe_bar_2026_catalog.personalization.product_popularity               jlim_fe_bar_2026_catalog.personalization.recommendations
jlim_fe_bar_2026_catalog.personalization.product_popularity_by_condition  jlim_fe_bar_2026_catalog.personalization.recommendations
jlim_fe_bar_2026_catalog.personalization.products                         jlim_fe_bar_2026_catalog.personalization.recommendations
```
