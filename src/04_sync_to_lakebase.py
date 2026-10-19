# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Serve via Lakebase Autoscaling — managed Synced Tables (Reverse ETL)
# MAGIC
# MAGIC Publishes the batch-generated serving tables into **Lakebase Autoscaling** using
# MAGIC Databricks' **managed synced tables** (reverse ETL): a managed Lakeflow pipeline keeps a
# MAGIC Postgres copy of each Unity Catalog table in sync. The home-page app reads those Postgres
# MAGIC tables with millisecond key-value lookups.
# MAGIC
# MAGIC We use **Snapshot** mode (full refresh each run) — it fits the daily-batch design and
# MAGIC needs no Change Data Feed. (Triggered/Continuous are available if you want incremental
# MAGIC sync; those require CDF on the source.)
# MAGIC
# MAGIC Synced tables land in Postgres under the **schema = the UC schema name**, e.g.
# MAGIC `personalization.recommendations_online`. The app reads from there.

# COMMAND ----------

# MAGIC %pip install -U "databricks-sdk>=0.110" "psycopg[binary]>=3.1"
# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "personalization")
dbutils.widgets.text("endpoint",
    "projects/YOUR-LAKEBASE-PROJECT/branches/production/endpoints/primary")
dbutils.widgets.text("branch", "projects/YOUR-LAKEBASE-PROJECT/branches/production")
dbutils.widgets.text("app_name", "silverleaf-home")
dbutils.widgets.text("app_sp_role", "")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT = dbutils.widgets.get("endpoint")
BRANCH = dbutils.widgets.get("branch")
APP_NAME = dbutils.widgets.get("app_name").strip()
APP_SP_ROLE = dbutils.widgets.get("app_sp_role").strip()
PG_DB = "databricks_postgres"

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    SyncedTable, SyncedTableSyncedTableSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SchedPolicy,
)
from databricks.sdk.errors import NotFound

w = WorkspaceClient()

# Resolve the app's service-principal id (its Postgres role) — explicit override, else
# auto-discover from the deployed app. If neither resolves, we sync but skip the grant.
if not APP_SP_ROLE and APP_NAME:
    try:
        APP_SP_ROLE = w.apps.get(name=APP_NAME).service_principal_client_id or ""
        print(f"Discovered app SP for '{APP_NAME}': {APP_SP_ROLE}")
    except Exception as e:
        print(f"(could not look up app '{APP_NAME}' — will skip GRANT this run: {e})")
GRANT_TO_APP = bool(APP_SP_ROLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build an app-ready `members_serving` table
# MAGIC Synced tables copy a Delta table **as-is**, so we first materialize the exact member
# MAGIC columns the app renders (including `conditions` as a string and the GenAI welcome banner from
# MAGIC notebook 03c), filtered to members who have recommendations. `recommendations` and `category_recommendations` are already
# MAGIC app-ready, so they sync directly.

# COMMAND ----------

spark.sql(f"USE {CATALOG}.{SCHEMA}")
spark.sql(f"""
    CREATE OR REPLACE TABLE members_serving AS
    SELECT m.member_id, m.first_name, m.last_name, m.age, m.state, m.plan_type,
           m.otc_wallet_monthly, CONCAT_WS(', ', m.health_conditions) AS conditions,
           msg.welcome_message, msg.message_source
    FROM members m
    LEFT JOIN member_messages msg USING (member_id)
    WHERE m.member_id IN (SELECT DISTINCT member_id FROM recommendations)
""")
print("members_serving rebuilt:", spark.table("members_serving").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create / refresh the managed synced tables (Snapshot)
# MAGIC One synced table per serving table. We delete-then-create so each batch run does a clean
# MAGIC full snapshot of the latest data. `create_database_objects_if_missing=True` creates the
# MAGIC Postgres schema/table automatically.

# COMMAND ----------

# (source Delta table, synced-table name, primary key columns)
SYNCS = [
    ("recommendations",          "recommendations_online",          ["member_id", "module", "rank"]),
    ("category_recommendations", "category_recommendations_online", ["member_id", "rank"]),
    ("members_serving",          "members_online",                  ["member_id"]),
]

def ensure_synced(source_tbl: str, synced_tbl: str, pks: list):
    """Create a Snapshot synced table, replacing any existing one (full refresh each run)."""
    synced_id = f"{CATALOG}.{SCHEMA}.{synced_tbl}"
    full_name = f"synced_tables/{synced_id}"
    try:
        w.postgres.get_synced_table(name=full_name)
        w.postgres.delete_synced_table(name=full_name).wait()
        print(f"  deleted existing {synced_tbl}")
    except NotFound:
        pass
    w.postgres.create_synced_table(
        synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
            source_table_full_name=f"{CATALOG}.{SCHEMA}.{source_tbl}",
            branch=BRANCH,
            primary_key_columns=pks,
            scheduling_policy=SchedPolicy.SNAPSHOT,
            postgres_database=PG_DB,
            create_database_objects_if_missing=True,
        )),
        synced_table_id=synced_id,
    ).wait()
    # create().wait() can return before the snapshot has populated the Postgres table, so
    # poll until the table is ONLINE with no pending update before we rely on it.
    for _ in range(120):
        st = w.postgres.get_synced_table(name=full_name)
        ds = st.status.detailed_state.value if st.status and st.status.detailed_state else ""
        if "ONLINE_NO_PENDING_UPDATE" in ds:
            break
        if "FAILED" in ds:
            raise RuntimeError(f"{synced_tbl} sync failed: {getattr(st.status, 'message', '')}")
        time.sleep(5)
    print(f"  synced {source_tbl} -> {PG_DB}.{SCHEMA}.{synced_tbl}  ({ds})")

for source_tbl, synced_tbl, pks in SYNCS:
    ensure_synced(source_tbl, synced_tbl, pks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the app's service-principal role read access
# MAGIC The synced Postgres tables live in the `{schema}` Postgres schema. The app authenticates
# MAGIC as its service principal, so grant that role USAGE + SELECT.

# COMMAND ----------

if GRANT_TO_APP:
    import psycopg
    host = w.postgres.get_endpoint(name=ENDPOINT).status.hosts.host
    user = w.current_user.me().user_name
    token = w.postgres.generate_database_credential(endpoint=ENDPOINT).token
    conn_str = f"host={host} dbname={PG_DB} user={user} password={token} sslmode=require"
    with psycopg.connect(conn_str, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'GRANT USAGE ON SCHEMA {SCHEMA} TO "{APP_SP_ROLE}";')
        for _, synced_tbl, _ in SYNCS:
            cur.execute(f'GRANT SELECT ON {SCHEMA}.{synced_tbl} TO "{APP_SP_ROLE}";')
    print(f"Granted USAGE + SELECT on {SCHEMA}.* synced tables to {APP_SP_ROLE}")
else:
    print("No app SP resolved — GRANT skipped (re-run after the app is deployed, "
          "or pass app_sp_role).")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

for _, synced_tbl, _ in SYNCS:
    st = w.postgres.get_synced_table(name=f"synced_tables/{CATALOG}.{SCHEMA}.{synced_tbl}")
    state = st.status.detailed_state.value if st.status and st.status.detailed_state else "?"
    print(f"  {SCHEMA}.{synced_tbl:32s} {state}")
print("\nServing via managed Lakebase synced tables (Snapshot reverse ETL).")
