# Execution evidence — SilverLeaf Benefits member personalization

Text evidence that the build ran end to end, captured live from workspace
`fevm-jlim-fe-bar-2026` on 2026-10-19 by [`scripts/capture_evidence.py`](../scripts/capture_evidence.py).
Every block is real API / SQL / Postgres / log output: no screenshots, nothing hand-edited.
All data is synthetic (no real members, no PHI).

**The run:** one Lakeflow Job, 8 tasks, one clean pass. Job 486067273141675, run 1086036015642743,
result **SUCCESS** (full task timings in [01_lakeflow.md](01_lakeflow.md)).

## Evidence by required stage

| Stage | File | Key proof |
|---|---|---|
| **Lakeflow** ingest | [01_lakeflow.md](01_lakeflow.md) | All 8 job tasks SUCCESS in one run; pipeline update COMPLETED (full refresh); 5 raw JSON feeds in a UC Volume → bronze → silver with identical counts; every `EXPECT` rule's passed/failed counts from the pipeline event log |
| **Unity Catalog** govern | [02_unity_catalog.md](02_unity_catalog.md) | 33 table tags + 4 sensitive-column tags; `mask_health_conditions` definition; `members_analyst_v` returning masked conditions; app SP holds exactly 3 grants (`USE CATALOG`, `USE SCHEMA`, `SELECT product_catalog`); lineage from `system.access.table_lineage` |
| **Lakebase** serve | [03_lakebase.md](03_lakebase.md) | Endpoint ACTIVE; 3 synced tables `ONLINE_NO_PENDING_UPDATE`; live Postgres row counts; app SP `SELECT` grants; the exact page-load lookups with results (incl. GenAI banner); index-scan plan |
| **ML** | [04_ml_genai.md](04_ml_genai.md) §A, §B, §D | Temporal-holdout evaluation of 4 recommenders on 918 members; production engine **hybrid** (the champion): HitRate@10 **54.1% vs 30.9%** baseline (**+75%**), NDCG@10 **+114%**; MLflow runs with params + metrics |
| **GenAI** | [04_ml_genai.md](04_ml_genai.md) §C | `ai_query` banners for 2,000 members; per-check guardrail pass rates; sample inputs → outputs; **7/7 planted bad messages blocked** by the self-test gate |
| **Genie Agent** | [05_genie.md](05_genie.md) | Q → SQL → result → answer transcripts via the Conversation API; scoped to `product_catalog` only; spend and member-health questions refused with no SQL generated |
| **Databricks App** | [06_app.md](06_app.md) | App RUNNING as its own SP; live `databricks apps logs` lines showing Lakebase lookups per page load and Genie questions from inside the app |

## How the stages connect (same run)

```
raw/ JSON (Volume) ─Auto Loader→ bronze_* ─EXPECT→ members, products, orders, order_items, web_sessions
  → member_features, product_popularity, member_product_interactions             (02)
  → recommendations, category_recommendations   (hybrid engine)                  (03)
  → recommendation_offline_eval  (picked hybrid as champion)                     (03b)
  → member_messages  (ai_query → 5 guardrails → template fallback)               (03c)
  → members_online / recommendations_online / category_recommendations_online   (04, Lakebase)
  → tags, mask, members_analyst_v, product_catalog → Genie Agent                 (05)
  → silverleaf-home app reads Lakebase + calls Genie                             (app/)
```

## Reproduce

```bash
databricks bundle deploy -t dev --profile <p>
databricks bundle run home_page -t dev --profile <p>
databricks bundle run personalization_pipeline -t dev --profile <p>
uv run --with databricks-sdk --with "psycopg[binary]" \
  scripts/capture_evidence.py --profile <p> --catalog <catalog>
```
