# Lakeflow ingestion (raw → bronze → silver)

This build starts from a **raw dataset** and ingests it with **Lakeflow**, rather than
writing gold tables directly. The medallion front-end is:

```
notebook 01                 Lakeflow Declarative Pipeline (src/ingest_pipeline/)        notebooks 02–04
──────────────              ──────────────────────────────────────────────────        ────────────────
generate synthetic     →    raw JSON in a UC Volume                                →    member_features
data as raw JSON            /Volumes/{catalog}/{schema}/raw/<feed>                       recommendations
                            │                                                            category_recs
                            │  Auto Loader (STREAM read_files)                            → Lakebase → App
                            ▼
                            bronze_members / bronze_products / bronze_orders /
                            bronze_order_items / bronze_web_sessions
                            │  cast types + data-quality EXPECTATIONS
                            ▼
                            members / products / orders / order_items / web_sessions  (silver)
```

## What each piece does

- **`src/01_generate_data.py`** — lands the seeded synthetic feeds as newline-delimited
  JSON files in the managed Volume `{catalog}.{schema}.raw` (one folder per feed). This is
  the raw dataset; nothing is written as a table here.
- **`src/ingest_pipeline/bronze.sql`** — Auto Loader (`STREAM read_files(... format => 'json')`)
  incrementally ingests each feed into a `bronze_*` streaming table, preserving every raw
  column plus ingest lineage (`_source_file`, `_ingested_at`).
- **`src/ingest_pipeline/silver.sql`** — casts JSON strings back to real types (dates,
  timestamps, ints, doubles, booleans; arrays survive the round-trip) and attaches
  data-quality **expectations** (e.g. non-null keys, positive prices/quantities, plausible
  ages). Violations are logged to the pipeline event log every run.

## Why silver uses the plain entity names

The silver tables are published as `members`, `products`, `orders`, `order_items`,
`web_sessions` — **exactly the input contract** `02_feature_engineering.py` already reads.
So the feature/scoring notebooks are unchanged: silver *is* the gold-input contract.

## Running it

The bundle wires this as a job DAG: `generate_data` → `ingest_raw` (pipeline) →
`feature_engineering` → `train_and_score` → `evaluate_recommendations` + `genai_member_messages` →
`sync_to_lakebase` + `governance_and_genie`.

```bash
databricks bundle deploy -t dev --profile <profile>
databricks bundle run personalization_pipeline -t dev --profile <profile>
```

## Re-run note

Each feed folder is overwritten on regeneration, but Spark part-file names change, so
Auto Loader would treat regenerated files as *new*. The job's `ingest_raw` task therefore
sets `full_refresh: true`, so bronze/silver always reflect the latest generation. With a real
incremental feed, remove the flag and Auto Loader ingests only newly arrived files.

## Bring-your-own-data

Customers who already have gold tables can drop the `generate_data` + `ingest_raw` tasks
and point the `src_*` variables (notebook 02) at their own tables — the rest of the
pipeline is unchanged.
