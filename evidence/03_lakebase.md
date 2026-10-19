# Evidence 03 — Lakebase operational serving

Captured 2026-10-19 15:01 EDT from workspace `fevm-jlim-fe-bar-2026` by `scripts/capture_evidence.py` — live output, not edited.

## Lakebase Autoscaling endpoint
```
endpoint: projects/member-personalization/branches/production/endpoints/primary
host:     ep-snowy-block-d2j04dro.database.us-east-1.cloud.databricks.com
state:    ACTIVE
autoscale_cu: 1.0 - 1.0
```

## Managed synced tables (Snapshot reverse ETL, Delta -> Postgres)
```
members_online                     SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE
recommendations_online             SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE
category_recommendations_online    SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE
```

## Row counts in Postgres (queried live with psycopg)
```
members_online                        2,000 rows
recommendations_online               34,799 rows
category_recommendations_online      14,000 rows
```

## Postgres grants held by the app service principal (`e8c1826c-bf4c-44c4-81b2-915889f11934`)
```
e8c1826c-bf4c-44c4-81b2-915889f11934  category_recommendations_online    SELECT
e8c1826c-bf4c-44c4-81b2-915889f11934  members_online                     SELECT
e8c1826c-bf4c-44c4-81b2-915889f11934  recommendations_online             SELECT
```

## The exact lookups the app runs on page load (member `M100000`)
```
SELECT member_id, first_name, plan_type, otc_wallet_monthly, welcome_message, message_source FROM personalization.members_online WHERE member_id = %s
-> ('M100000', 'Dorothy', 'C-SNP', 150, 'Welcome, Dorothy! Your $150 monthly allowance is ready to spend on great items like ValueCare Eye Drops and PureLife Reading Glasses — start shopping today!', 'genai')
```

```
SELECT module, rank, product_name, category, price, reason FROM personalization.recommendations_online WHERE member_id = %s ORDER BY module, rank
-> 16 rows in 30.0 ms
   ('Based on your conditions', 1, 'PureLife Reading Glasses', 'Personal Care', 16.84, 'Relevant to your vision_loss')
   ('Based on your conditions', 2, 'GoldStandard Reading Glasses', 'Personal Care', 15.82, 'Relevant to your vision_loss')
   ('Based on your conditions', 3, 'Premier Magnifier', 'Vision & Hearing', 19.55, 'Relevant to your vision_loss')
   ('Based on your conditions', 4, 'GoldStandard Magnifier', 'Vision & Hearing', 25.03, 'Relevant to your vision_loss')
   ('Based on your conditions', 5, 'ValueCare Reading Glasses', 'Personal Care', 16.81, 'Relevant to your vision_loss')
   ('Buy Again', 1, 'PureLife Eye Drops', 'Vision & Hearing', 12.4, 'Reordered 2 times')
   ('Recommended for you', 1, 'ValueCare Eye Drops', 'Vision & Hearing', 7.04, 'Members like you also bought this')
   ('Recommended for you', 2, 'PureLife Eye Drops', 'Vision & Hearing', 12.87, 'Members like you also bought this')
   ('Recommended for you', 3, 'ValueCare Hearing Aid Batteries', 'Vision & Hearing', 8.4, 'Members like you also bought this')
   ('Recommended for you', 4, 'ActiveSenior Low-Sodium Meals', 'Healthy Foods', 7.86, 'Members like you also bought this')
   ('Recommended for you', 5, 'EverWell Hearing Aid Batteries', 'Vision & Hearing', 13.85, 'Members like you also bought this')
   ('Recommended for you', 6, 'ValueCare Hearing Aid Batteries', 'Vision & Hearing', 8.01, 'Members like you also bought this')
   ('Recommended for you', 7, 'ActiveSenior Reading Glasses', 'Personal Care', 9.55, 'Members like you also bought this')
   ('Recommended for you', 8, 'GoldStandard Hearing Aid Batteries', 'Vision & Hearing', 8.51, 'Members like you also bought this')
   ('Recommended for you', 9, 'ActiveSenior Low-Sodium Meals', 'Healthy Foods', 11.45, 'Members like you also bought this')
   ('Recommended for you', 10, 'ValueCare Hearing Aid Batteries', 'Vision & Hearing', 13.35, 'Members like you also bought this')
```

## Index-backed point lookup (primary key from the synced table spec)
```
Index Scan using "__db_tmp_42d3771d-c141-4c95-89ea-441d38e9195d_pkey" on partition_16987 recommendations_online  (cost=0.29..52.34 rows=17 width=125)
  Index Cond: (member_id = 'x'::text)
```
