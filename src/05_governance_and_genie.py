# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Govern + publish — Unity Catalog controls and the shopper-facing Genie Agent
# MAGIC
# MAGIC Everything the business touches is governed here, in code, on every run:
# MAGIC
# MAGIC | Control | What it does |
# MAGIC |---|---|
# MAGIC | **Classification tags** | Tables tagged `silverleaf_layer` / `_domain` / `_contains`; sensitive columns (names, health conditions) tagged `silverleaf_sensitivity=member_health` / `member_pii` |
# MAGIC | **Column mask** | `mask_health_conditions()` — analysts see `***` for health conditions unless they're in the care-team group |
# MAGIC | **Analyst view** | `members_analyst_v` — names dropped, conditions masked, age banded: safe for broad BI use |
# MAGIC | **Catalog view** | `product_catalog` — product attributes + popularity only; the ONLY thing the Genie Agent can see |
# MAGIC | **Least privilege** | The app's service principal gets `SELECT` on `product_catalog` and nothing else in UC |
# MAGIC | **Genie Agent** | `SilverLeaf Product Finder` created/updated from `genie/product_catalog_space.json` (versioned in git) |

# COMMAND ----------

# MAGIC %pip install -U "databricks-sdk>=0.110"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "personalization", "Schema")
dbutils.widgets.text("app_name", "silverleaf-home", "App name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse id for the Genie Agent")
dbutils.widgets.text("care_team_group", "silverleaf-care-team", "Group allowed to see health conditions")
dbutils.widgets.text("genie_space_json", "", "Path to genie/product_catalog_space.json (blank = bundle default)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
APP_NAME = dbutils.widgets.get("app_name").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
CARE_TEAM = dbutils.widgets.get("care_team_group").strip()
FQ = f"{CATALOG}.{SCHEMA}"
GENIE_TITLE = "SilverLeaf Product Finder"

import json, os
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
spark.sql(f"USE {FQ}")

APP_SP = w.apps.get(name=APP_NAME).service_principal_client_id
print("app service principal:", APP_SP)

# COMMAND ----------

# MAGIC %md ## 1 · Classification tags (tables + sensitive columns)

# COMMAND ----------

TABLE_TAGS = {
    "members": {"layer": "silver", "domain": "member", "contains": "member_health"},
    "orders": {"layer": "silver", "domain": "commerce"},
    "order_items": {"layer": "silver", "domain": "commerce"},
    "products": {"layer": "silver", "domain": "catalog"},
    "web_sessions": {"layer": "silver", "domain": "engagement"},
    "member_features": {"layer": "gold", "domain": "member", "contains": "member_health"},
    "recommendations": {"layer": "serving", "domain": "personalization"},
    "category_recommendations": {"layer": "serving", "domain": "personalization"},
    "member_messages": {"layer": "serving", "domain": "personalization", "contains": "genai_output"},
    "recommendation_offline_eval": {"layer": "ml", "domain": "personalization"},
}
COLUMN_TAGS = {
    ("members", "health_conditions"): "member_health",
    ("members", "first_name"): "member_pii",
    ("members", "last_name"): "member_pii",
    ("member_features", "health_conditions"): "member_health",
}

# Tag keys are namespaced (silverleaf_*): in a shared metastore, bare keys like `layer` or
# `domain` are often governed tags owned by other teams, with their own allowed values.
NS = "silverleaf"

def tag_sql(kind, name, tags):
    pairs = ", ".join(f"'{NS}_{k}' = '{v}'" for k, v in tags.items())
    return f"ALTER {kind} {name} SET TAGS ({pairs})"

applied = []
for tbl, tags in TABLE_TAGS.items():
    tags = {**tags, "data_origin": "synthetic"}
    # silver tables are pipeline-owned streaming tables; others are managed tables
    for kind in ("TABLE", "STREAMING TABLE"):
        try:
            spark.sql(tag_sql(kind, f"{FQ}.{tbl}", tags)); applied.append(tbl); break
        except Exception as e:
            last = e
    else:
        raise RuntimeError(f"could not tag {tbl}: {last}")
for (tbl, col), level in COLUMN_TAGS.items():
    for kind in ("TABLE", "STREAMING TABLE"):
        try:
            spark.sql(f"ALTER {kind} {FQ}.{tbl} ALTER COLUMN {col} SET TAGS ('{NS}_sensitivity' = '{level}')")
            applied.append(f"{tbl}.{col}"); break
        except Exception as e:
            last = e
    else:
        raise RuntimeError(f"could not tag {tbl}.{col}: {last}")
print(f"tagged {len(applied)} objects")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Column mask + de-identified analyst view
# MAGIC
# MAGIC `mask_health_conditions` returns the real array only for members of the care-team group;
# MAGIC everyone else gets `['***']`. The analyst view applies it, drops names and bands age —
# MAGIC so BI users can analyse benefit usage by segment without ever seeing who has what condition.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.mask_health_conditions(conds ARRAY<STRING>)
RETURNS ARRAY<STRING>
COMMENT 'Column mask: reveals member health conditions only to the {CARE_TEAM} group.'
RETURN CASE WHEN is_account_group_member('{CARE_TEAM}') THEN conds ELSE array('***') END
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.members_analyst_v
COMMENT 'De-identified members for broad analytics: no names, age banded, health conditions masked unless {CARE_TEAM}.'
AS SELECT member_id,
          CASE WHEN age < 65 THEN 'under 65' WHEN age < 75 THEN '65-74'
               WHEN age < 85 THEN '75-84' ELSE '85+' END       AS age_band,
          state, plan_type, enrollment_date, otc_wallet_monthly,
          {FQ}.mask_health_conditions(health_conditions)     AS health_conditions
   FROM {FQ}.members
""")
sample = spark.sql(f"SELECT * FROM {FQ}.members_analyst_v ORDER BY member_id LIMIT 3").toPandas()
print("members_analyst_v as seen by the job's identity:\n", sample.to_string(index=False))

# COMMAND ----------

# MAGIC %md ## 3 · The Genie Agent's only data source: `product_catalog`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {FQ}.product_catalog
COMMENT 'Shopper-facing product catalog (attributes + overall popularity only). The ONLY object the SilverLeaf Product Finder Genie Agent and the app can read in Unity Catalog — no member, order, or spend data.'
AS SELECT p.product_id, p.product_name, p.category, p.subcategory, p.vendor, p.price,
          p.condition_tags, p.is_consumable, coalesce(pp.units_sold, 0) AS units_sold
   FROM {FQ}.products p LEFT JOIN {FQ}.product_popularity pp USING (product_id)
""")
COLUMN_COMMENTS = {
    "product_name": "Shopper-facing product name (brand + item).",
    "category": "Top-level store category, e.g. Diabetic Care, Vision & Hearing.",
    "subcategory": "Item type within the category, e.g. Glucose Test Strips.",
    "price": "Unit price in USD, payable from the member's OTC allowance.",
    "condition_tags": "ARRAY of health conditions the product is relevant to (diabetes, hypertension, arthritis, heart_disease, osteoporosis, vision_loss, hearing_loss, mobility_impaired).",
    "is_consumable": "True if the product is used up and reordered (strips, vitamins, batteries).",
    "units_sold": "Total units sold across all members — use for 'popular' / 'best-selling'.",
}
for col, cmt in COLUMN_COMMENTS.items():
    escaped = cmt.replace("'", "\\'")
    spark.sql(f"COMMENT ON COLUMN {FQ}.product_catalog.{col} IS '{escaped}'")

# COMMAND ----------

# MAGIC %md ## 4 · Least-privilege grants for the app's service principal

# COMMAND ----------

for stmt in (
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{APP_SP}`",
    f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `{APP_SP}`",
    f"GRANT SELECT ON TABLE {FQ}.product_catalog TO `{APP_SP}`",
):
    spark.sql(stmt); print(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Create / update the Genie Agent from the versioned config
# MAGIC The space definition (instructions, example SQL, sample questions) lives in git at
# MAGIC `genie/product_catalog_space.json`. We point it at this run's catalog, then create the space
# MAGIC (first run) or update it in place (every run after) — so the agent is reproducible and reviewable.

# COMMAND ----------

_path = dbutils.widgets.get("genie_space_json").strip()
if not _path:
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _path = "/Workspace" + os.path.dirname(os.path.dirname(nb)) + "/genie/product_catalog_space.json"
try:
    with open(_path) as f:
        space = json.load(f)
except OSError:
    # Some workspaces can't read workspace files through the /Workspace FUSE mount from
    # serverless; fall back to the Workspace API.
    from databricks.sdk.service.workspace import ExportFormat
    with w.workspace.download(_path.removeprefix("/Workspace"), format=ExportFormat.AUTO) as f:
        space = json.load(f)
serialized = json.dumps(space).replace("member_personalization.personalization", FQ)

if not WAREHOUSE_ID:
    WAREHOUSE_ID = next(wh.id for wh in w.warehouses.list())
existing = [s for s in (w.genie.list_spaces().spaces or []) if s.title == GENIE_TITLE]
if existing:
    space_id = existing[0].space_id
    w.genie.update_space(space_id=space_id, serialized_space=serialized, warehouse_id=WAREHOUSE_ID)
    print("updated Genie Agent", space_id)
else:
    space_id = w.genie.create_space(
        warehouse_id=WAREHOUSE_ID, serialized_space=serialized, title=GENIE_TITLE,
        description="Shopper-facing product finder for the SilverLeaf Benefits OTC store. "
                    "Scoped to the product catalog only.").space_id
    print("created Genie Agent", space_id)

# The app's SP may run the agent (CAN_RUN) — and can use the warehouse to execute its SQL.
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
w.permissions.update("genie", space_id, access_control_list=[
    AccessControlRequest(service_principal_name=APP_SP, permission_level=PermissionLevel.CAN_RUN)])
w.permissions.update("warehouses", WAREHOUSE_ID, access_control_list=[
    AccessControlRequest(service_principal_name=APP_SP, permission_level=PermissionLevel.CAN_USE)])
print(f"granted CAN_RUN on Genie Agent {space_id} and CAN_USE on warehouse {WAREHOUSE_ID} to {APP_SP}")
dbutils.jobs.taskValues.set("genie_space_id", space_id)

# COMMAND ----------

# MAGIC %md ## 6 · Governance summary (what's enforced right now)

# COMMAND ----------

display(spark.sql(f"""
  SELECT table_name, tag_name, tag_value FROM {CATALOG}.information_schema.table_tags
  WHERE schema_name = '{SCHEMA}' AND startswith(tag_name, '{NS}_') ORDER BY table_name, tag_name
"""))
display(spark.sql(f"""
  SELECT table_name, column_name, tag_name, tag_value FROM {CATALOG}.information_schema.column_tags
  WHERE schema_name = '{SCHEMA}' AND startswith(tag_name, '{NS}_') ORDER BY table_name, column_name
"""))
display(spark.sql(f"""
  SELECT grantee, table_name, privilege_type FROM {CATALOG}.information_schema.table_privileges
  WHERE table_schema = '{SCHEMA}' AND grantee = '{APP_SP}'
"""))
