# Evidence 04 — ML + GenAI

Captured 2026-10-19 15:03 EDT from workspace `fevm-jlim-fe-bar-2026` by `scripts/capture_evidence.py` — live output, not edited.

## A · Offline evaluation — temporal holdout vs. the best-seller baseline (notebook 03b)
Recommenders rebuilt on purchases before the cutoff; scored on products each member bought for the first time after it.

```sql
SELECT recommender, round(hit_rate, 3) AS hit_rate_at_10, round(precision, 3) AS precision_at_10,
       round(recall, 3) AS recall_at_10, round(ndcg, 3) AS ndcg_at_10,
       concat(CAST(round(hit_rate_lift_vs_baseline * 100, 0) AS INT), '%') AS hit_rate_lift,
       concat(CAST(round(ndcg_lift_vs_baseline * 100, 0) AS INT), '%') AS ndcg_lift,
       members_evaluated, cutoff_date
FROM jlim_fe_bar_2026_catalog.personalization.recommendation_offline_eval ORDER BY ndcg DESC
```
```
recommender      hit_rate_at_10  precision_at_10  recall_at_10  ndcg_at_10  hit_rate_lift  ndcg_lift  members_evaluated  cutoff_date
---------------  --------------  ---------------  ------------  ----------  -------------  ---------  -----------------  -----------
hybrid           0.541           0.075            0.318         0.207       75%            114%       918                2026-04-01
condition_match  0.521           0.072            0.309         0.201       68%            109%       918                2026-04-01
cooccurrence_cf  0.41            0.05             0.218         0.143       32%            48%        918                2026-04-01
popularity       0.309           0.038            0.155         0.096       0%             0%         918                2026-04-01
```

## B · Production recommendations (notebook 03)
```sql
SELECT module, count(DISTINCT member_id) AS members, count(*) AS rows
FROM jlim_fe_bar_2026_catalog.personalization.recommendations GROUP BY module ORDER BY module
```
```
module                    members  rows
------------------------  -------  -----
Based on your conditions  1371     11049
Buy Again                 1406     3750
Recommended for you       2000     20000
```

```sql
SELECT reason, count(DISTINCT member_id) AS members FROM jlim_fe_bar_2026_catalog.personalization.recommendations
WHERE module = 'Recommended for you' GROUP BY reason ORDER BY reason
```
```
reason                             members
---------------------------------  -------
Members like you also bought this  1712
Popular with members like you      288
```

## C · GenAI welcome banner — ai_query() + guardrails (notebook 03c)
```sql
SELECT count(*) AS generated,
       round(avg(CAST(passed AS INT)), 3)             AS pass_rate,
       round(avg(CAST(no_medical_claim AS INT)), 3)   AS no_medical_claim,
       round(avg(CAST(no_condition_named AS INT)), 3) AS no_condition_named,
       round(avg(CAST(amount_is_real AS INT)), 3)     AS amount_is_real,
       round(avg(CAST(products_are_real AS INT)), 3)  AS products_are_real,
       round(avg(CAST(length_ok AS INT)), 3)          AS length_ok
FROM jlim_fe_bar_2026_catalog.personalization.member_messages_checked
```
```
generated  pass_rate  no_medical_claim  no_condition_named  amount_is_real  products_are_real  length_ok
---------  ---------  ----------------  ------------------  --------------  -----------------  ---------
2000       1.0        1.0               1.0                 1.0             1.0                1.0
```

Sample generations that passed (inputs -> model output):

```sql
SELECT member_id, first_name, otc_wallet_monthly AS otc, array_join(rec_names, ' | ') AS picks, msg
FROM jlim_fe_bar_2026_catalog.personalization.member_messages_checked WHERE passed ORDER BY member_id LIMIT 6
```
```
member_id  first_name  otc  picks                                                                                      msg
---------  ----------  ---  -----------------------------------------------------------------------------------------  ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
M100000    Dorothy     150  ValueCare Eye Drops | PureLife Reading Glasses | PureLife Eye Drops                        Welcome, Dorothy! Your $150 monthly allowance is ready to spend on great items like ValueCare Eye Drops and PureLife Reading Glasses — start shopping today!
M100001    Frank       75   ActiveSenior Low-Sodium Meals | PureLife Eye Drops | Premier Low-Sodium Meals              Welcome, Frank! You have $75 in monthly OTC benefits ready to spend — explore great options like ActiveSenior Low-Sodium Meals and PureLife Eye Drops to support your everyday wellness today.
M100002    Susan       150  ActiveSenior Low-Sodium Meals | ValueCare Magnifier | PureLife Vitamin D3                  Welcome, Susan! Your $150 monthly allowance is ready to use on great items like ActiveSenior Low-Sodium Meals and ValueCare Magnifier — start shopping today!
M100003    Dorothy     100  ActiveSenior Low-Sodium Meals | PureLife Pain Reliever                                     Welcome, Dorothy! Your $100 monthly allowance is ready to spend on great items like ActiveSenior Low-Sodium Meals and PureLife Pain Reliever — start shopping today!
M100004    Charles     100  Premier Low-Sodium Meals | ValueCare Glucose Test Strips | PureLife Pain Reliever          Welcome, Charles! Your $100 monthly allowance is ready to spend on great items like Premier Low-Sodium Meals and PureLife Pain Reliever — start shopping today!
M100005    William     75   Premier Low-Sodium Meals | ValueCare Whole Grain Snacks | ActiveSenior Electrolyte Drinks  Welcome, William! Your $75 monthly allowance is ready to use — explore great options like Premier Low-Sodium Meals and ActiveSenior Electrolyte Drinks to support your everyday wellness.
```

Guardrail self-test — planted bad messages pushed through the same checks every run (the job fails if any would be served):

```sql
SELECT case, expected_fail, msg, passed AS would_be_served
FROM jlim_fe_bar_2026_catalog.personalization.guardrail_selftest ORDER BY case
```
```
case              expected_fail       msg                                                                                                                                                                                                                                                                                                                                                                               would_be_served
----------------  ------------------  --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------  ---------------
condition_named   no_condition_named  Mary, since you have diabetes, your $50 allowance covers PureLife Glucose Tablets.                                                                                                                                                                                                                                                                                                false
cure_claim        no_medical_claim    Mary, ValueCare Fish Oil can cure heart problems — use your $50 today!                                                                                                                                                                                                                                                                                                            false
heals_claim       no_medical_claim    Mary, PureLife Glucose Tablets help your body heal faster — $50 to spend!                                                                                                                                                                                                                                                                                                         false
invented_amount   amount_is_real      Great news Mary — you have a $500 bonus credit for PureLife Glucose Tablets!                                                                                                                                                                                                                                                                                                      false
invented_product  products_are_real   Mary, try the new PureLife Miracle Sugar Blocker with your $50 allowance.                                                                                                                                                                                                                                                                                                         false
medical_claim     no_medical_claim    Hi Mary! PureLife Glucose Tablets will treat your blood sugar this month.                                                                                                                                                                                                                                                                                                         false
too_long          length_ok           Mary really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really really welcome back.  false
```

Generations the guardrails rejected (served the safe template instead):

```sql
SELECT member_id, msg, no_medical_claim, no_condition_named, amount_is_real, products_are_real, length_ok
FROM jlim_fe_bar_2026_catalog.personalization.member_messages_checked WHERE NOT passed ORDER BY member_id LIMIT 6
```
```
member_id  msg  no_medical_claim  no_condition_named  amount_is_real  products_are_real  length_ok
---------  ---  ----------------  ------------------  --------------  -----------------  ---------
```

## D · MLflow runs (experiment `/Shared/silverleaf-personalization`, id 3508673722664281)
```
run: member_messages_genai  (78144fcf6b5d4f308dc6a01d19bec411)  status=FINISHED
   params: max_words=45, llm_endpoint=databricks-claude-sonnet-4-6, temperature=0.4
   metrics:
     guardrail.amount_is_real                   1.0000
     guardrail.length_ok                        1.0000
     guardrail.no_condition_named               1.0000
     guardrail.no_medical_claim                 1.0000
     guardrail.pass_rate                        1.0000
     guardrail.products_are_real                1.0000
     guardrail.selftest_blocked                 7.0000
     messages_generated                         2000.0000

run: home_page_recs_offline_eval  (cfba4138002b45d492982891d26dd2bb)  status=FINISHED
   params: min_cooccurrence=2, members_evaluated=918, k=10, holdout_days=60, cutoff_date=2026-04-01, champion=hybrid
   metrics:
     condition_match.hit_rate                   0.5207
     condition_match.member_coverage            0.8856
     condition_match.ndcg                       0.2011
     condition_match.precision                  0.0722
     condition_match.recall                     0.3090
     cooccurrence_cf.hit_rate                   0.4096
     cooccurrence_cf.member_coverage            1.0000
     cooccurrence_cf.ndcg                       0.1427
     cooccurrence_cf.precision                  0.0501
     cooccurrence_cf.recall                     0.2177
     hybrid.hit_rate                            0.5414
     hybrid.member_coverage                     1.0000
     hybrid.ndcg                                0.2066
     hybrid.precision                           0.0745
     hybrid.recall                              0.3176
     popularity.hit_rate                        0.3094
     popularity.member_coverage                 1.0000
     popularity.ndcg                            0.0964
     popularity.precision                       0.0379
     popularity.recall                          0.1550

run: home_page_recs_hybrid  (c7df47309f3c4d7da201ce9f9d60393e)  status=FINISHED
   params: top_n=10, cf_method=hybrid, min_cooccurrence=2
   metrics:
     members_with_cf_recs                       1712.0000

run: home_page_recs_cooccurrence  (a96eff61707241bf898f72aeaa52238e)  status=FINISHED
   params: cf_method=cooccurrence, min_cooccurrence=2, top_n=10
   metrics:
     members_with_cf_recs                       1712.0000
```
