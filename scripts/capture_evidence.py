"""Capture TEXT execution evidence for every stage of the build into evidence/*.md.

Everything written here is real API / SQL / Postgres / log output pulled live from the
workspace after a pipeline run — no screenshots, nothing hand-edited.

    uv run --with databricks-sdk --with "psycopg[binary]" \
        scripts/capture_evidence.py --profile <profile> --catalog <catalog>

Re-run any time; each file is overwritten with fresh output.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import time

import psycopg
from databricks.sdk import WorkspaceClient

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True)
ap.add_argument("--catalog", required=True)
ap.add_argument("--schema", default="personalization")
ap.add_argument("--app-name", default="silverleaf-home")
ap.add_argument("--lakebase-endpoint",
                default="projects/member-personalization/branches/production/endpoints/primary")
ap.add_argument("--mlflow-experiment", default="/Shared/silverleaf-personalization")
ap.add_argument("--only", default="", help="comma list of stages to (re)capture, e.g. 05,06")
args = ap.parse_args()

w = WorkspaceClient(profile=args.profile)
FQ = f"{args.catalog}.{args.schema}"
OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "evidence")
HOST = w.config.host.rstrip("/")
WORKSPACE = HOST.split("//")[1].split(".")[0]
NOW = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
WAREHOUSE = next(wh.id for wh in w.warehouses.list())
ONLY = set(filter(None, args.only.split(",")))


# ---------------------------------------------------------------- helpers
def sql(stmt: str):
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt,
                                                wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"{r.status.error.message}\n{stmt}")
    cols = [c.name for c in r.manifest.schema.columns]
    return cols, (r.result.data_array or []) if r.result else []


def table(stmt: str) -> str:
    """Run SQL and render a fixed-width text table (plus the SQL that produced it)."""
    cols, rows = sql(stmt)
    widths = [max(len(c), *(len(str(v)) for v in (row[i] for row in rows))) if rows else len(c)
              for i, c in enumerate(cols)]
    line = lambda vals: "  ".join(str(v if v is not None else "NULL").ljust(wd)
                                  for v, wd in zip(vals, widths)).rstrip()
    body = "\n".join([line(cols), line(["-" * wd for wd in widths])] + [line(r) for r in rows])
    return f"```sql\n{stmt.strip()}\n```\n```\n{body}\n```\n"


def block(text: str) -> str:
    return f"```\n{text.rstrip()}\n```\n"


def write(name: str, title: str, parts: list):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        f.write(f"# {title}\n\nCaptured {NOW} from workspace `{WORKSPACE}` by "
                f"`scripts/capture_evidence.py` — live output, not edited.\n\n")
        f.write("\n".join(parts))
    print("wrote", path)


def want(stage: str) -> bool:
    return not ONLY or stage in ONLY


job = next(j for j in w.jobs.list()
           if "[SilverLeaf Demo] Member Personalization Pipeline" in (j.settings.name or ""))
run = next(iter(w.jobs.list_runs(job_id=job.job_id, completed_only=True, limit=1)))
run = w.jobs.get_run(run.run_id)


# ---------------------------------------------------------------- 01 Lakeflow
if want("01"):
    fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000).astimezone().strftime("%H:%M:%S")
    tasks = "\n".join(
        f"{t.task_key:28s} {t.state.result_state.value if t.state.result_state else t.state.life_cycle_state.value:10s}"
        f" {fmt(t.start_time)} -> {fmt(t.end_time)}  ({(t.end_time - t.start_time) / 1000:,.0f}s)"
        for t in sorted(run.tasks, key=lambda t: t.start_time))
    pipe = next(p for p in w.pipelines.list_pipelines() if "[SilverLeaf Demo] Raw" in (p.name or ""))
    upd = w.pipelines.list_updates(pipeline_id=pipe.pipeline_id, max_results=1).updates[0]
    parts = [
        "## Job run — every task in the end-to-end DAG",
        block(f"job:    {job.settings.name} (id {job.job_id})\nrun:    {run.run_page_url}\n"
              f"result: {run.state.result_state.value}\n\n{tasks}"),
        "## Lakeflow Declarative Pipeline update (Auto Loader raw JSON -> bronze -> silver)",
        block(f"pipeline: {pipe.name} (id {pipe.pipeline_id})\n"
              f"update:   {upd.update_id}  state={upd.state.value}  full_refresh={upd.full_refresh}"),
        "## Raw files landed in the Unity Catalog Volume (the raw dataset)",
        table(f"""SELECT regexp_extract(_metadata.file_path, '/raw/([a-z_]+)/', 1) AS feed,
       count(DISTINCT _metadata.file_path) AS json_files, count(*) AS raw_records
FROM json.`/Volumes/{args.catalog}/{args.schema}/raw/*/`
GROUP BY 1 ORDER BY 1"""),
        "## Row counts across the medallion (bronze == silver -> no rows dropped)",
        table(" UNION ALL ".join(
            f"SELECT '{t}' AS feed, (SELECT count(*) FROM {FQ}.bronze_{t}) AS bronze_rows, "
            f"(SELECT count(*) FROM {FQ}.{t}) AS silver_rows"
            for t in ("members", "products", "orders", "order_items", "web_sessions"))),
        "## Data-quality expectations — results from the pipeline event log",
        table(f"""SELECT origin.flow_name AS flow, e.name AS expectation,
       sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
FROM event_log('{pipe.pipeline_id}')
LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
  'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) x AS e
WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality.expectations IS NOT NULL
GROUP BY 1, 2 ORDER BY 1, 2"""),
        "## Silver typing — arrays and dates survive the JSON round-trip",
        table(f"SELECT member_id, age, enrollment_date, health_conditions, otc_wallet_monthly "
              f"FROM {FQ}.members ORDER BY member_id LIMIT 3"),
    ]
    write("01_lakeflow.md", "Evidence 01 — Lakeflow ingestion (raw → bronze → silver)", parts)


# ---------------------------------------------------------------- 02 Unity Catalog
if want("02"):
    app_sp = w.apps.get(args.app_name).service_principal_client_id
    parts = [
        f"Catalog `{args.catalog}`, schema `{args.schema}`. All data synthetic (no PHI).\n",
        "## Objects in the schema (medallion, ML, GenAI, serving, governed views)",
        table(f"""SELECT table_name, table_type, comment FROM {args.catalog}.information_schema.tables
WHERE table_schema = '{args.schema}'
  AND NOT startswith(table_name, '__materialization') AND NOT startswith(table_name, 'event_log_')
ORDER BY table_type, table_name"""),
        "## Classification tags — tables",
        table(f"""SELECT table_name, tag_name, tag_value FROM {args.catalog}.information_schema.table_tags
WHERE schema_name = '{args.schema}' AND startswith(tag_name, 'silverleaf_')
ORDER BY table_name, tag_name"""),
        "## Classification tags — sensitive columns",
        table(f"""SELECT table_name, column_name, tag_name, tag_value
FROM {args.catalog}.information_schema.column_tags
WHERE schema_name = '{args.schema}' AND startswith(tag_name, 'silverleaf_')
ORDER BY table_name, column_name"""),
        "## Column-mask function (health conditions visible only to the care-team group)",
        table(f"""SELECT routine_name, comment, routine_definition
FROM {args.catalog}.information_schema.routines
WHERE routine_schema = '{args.schema}' AND routine_name = 'mask_health_conditions'"""),
        "## De-identified analyst view — names dropped, age banded, conditions masked",
        table(f"SELECT * FROM {FQ}.members_analyst_v ORDER BY member_id LIMIT 5"),
        f"## Least privilege — every UC grant held by the app service principal (`{app_sp}`)",
        table(f"""SELECT 'CATALOG' AS level, catalog_name AS object, privilege_type
FROM system.information_schema.catalog_privileges
WHERE grantee = '{app_sp}' AND catalog_name = '{args.catalog}'
UNION ALL
SELECT 'SCHEMA', concat(catalog_name, '.', schema_name), privilege_type
FROM system.information_schema.schema_privileges
WHERE grantee = '{app_sp}' AND catalog_name = '{args.catalog}'
UNION ALL
SELECT 'TABLE', concat(table_catalog, '.', table_schema, '.', table_name), privilege_type
FROM system.information_schema.table_privileges
WHERE grantee = '{app_sp}' AND table_catalog = '{args.catalog}'"""),
        "The app SP can read `product_catalog` and nothing else in Unity Catalog; member data "
        "reaches the app only through Lakebase (see 03).\n",
        "## Lineage — what the serving table was built from (system.access.table_lineage)",
        table(f"""SELECT DISTINCT source_table_full_name, target_table_full_name
FROM system.access.table_lineage
WHERE target_table_catalog = '{args.catalog}'
  AND target_table_name IN ('recommendations', 'member_messages', 'members_serving', 'product_catalog')
  AND source_table_full_name IS NOT NULL
ORDER BY target_table_full_name, source_table_full_name"""),
    ]
    write("02_unity_catalog.md", "Evidence 02 — Unity Catalog governance", parts)


# ---------------------------------------------------------------- 03 Lakebase
if want("03"):
    ep = w.postgres.get_endpoint(name=args.lakebase_endpoint)
    host = ep.status.hosts.host
    states = []
    for t in ("members_online", "recommendations_online", "category_recommendations_online"):
        st = w.postgres.get_synced_table(name=f"synced_tables/{FQ}.{t}")
        states.append(f"{t:34s} {st.status.detailed_state.value}")
    token = w.postgres.generate_database_credential(endpoint=args.lakebase_endpoint).token
    user = w.current_user.me().user_name
    app_sp = w.apps.get(args.app_name).service_principal_client_id
    out = []
    with psycopg.connect(f"host={host} dbname=databricks_postgres user={user} password={token} "
                         f"sslmode=require") as conn, conn.cursor() as cur:
        for t in ("members_online", "recommendations_online", "category_recommendations_online"):
            cur.execute(f"SELECT count(*) FROM {args.schema}.{t}")
            out.append(f"{t:34s} {cur.fetchone()[0]:>8,} rows")
        cur.execute("""SELECT grantee, table_name, privilege_type FROM information_schema.role_table_grants
                       WHERE grantee = %s ORDER BY table_name""", (app_sp,))
        grants = "\n".join(f"{g[0]}  {g[1]:34s} {g[2]}" for g in cur.fetchall())
        cur.execute(f"SELECT member_id FROM {args.schema}.members_online ORDER BY member_id LIMIT 1")
        mid = cur.fetchone()[0]
        q_member = (f"SELECT member_id, first_name, plan_type, otc_wallet_monthly, welcome_message, "
                    f"message_source FROM {args.schema}.members_online WHERE member_id = %s")
        cur.execute(q_member, (mid,))
        member_row = cur.fetchone()
        q_recs = (f"SELECT module, rank, product_name, category, price, reason "
                  f"FROM {args.schema}.recommendations_online WHERE member_id = %s ORDER BY module, rank")
        t0 = time.perf_counter()
        cur.execute(q_recs, (mid,))
        recs = cur.fetchall()
        ms = (time.perf_counter() - t0) * 1000
        cur.execute(f"EXPLAIN SELECT * FROM {args.schema}.recommendations_online WHERE member_id = 'x'")
        plan = "\n".join(r[0] for r in cur.fetchall())
    parts = [
        "## Lakebase Autoscaling endpoint",
        block(f"endpoint: {args.lakebase_endpoint}\nhost:     {host}\nstate:    {ep.status.current_state.value}\n"
              f"autoscale_cu: {ep.status.autoscaling_limit_min_cu} - {ep.status.autoscaling_limit_max_cu}"),
        "## Managed synced tables (Snapshot reverse ETL, Delta -> Postgres)",
        block("\n".join(states)),
        "## Row counts in Postgres (queried live with psycopg)",
        block("\n".join(out)),
        f"## Postgres grants held by the app service principal (`{app_sp}`)",
        block(grants),
        f"## The exact lookups the app runs on page load (member `{mid}`)",
        block(f"{q_member}\n-> {member_row}"),
        block(f"{q_recs}\n-> {len(recs)} rows in {ms:.1f} ms\n" + "\n".join(f"   {r}" for r in recs)),
        "## Index-backed point lookup (primary key from the synced table spec)",
        block(plan),
    ]
    write("03_lakebase.md", "Evidence 03 — Lakebase operational serving", parts)


# ---------------------------------------------------------------- 04 ML / GenAI
if want("04"):
    exp = w.experiments.get_by_name(args.mlflow_experiment).experiment
    runs = w.experiments.search_runs(experiment_ids=[exp.experiment_id], max_results=10,
                                     order_by=["attributes.start_time DESC"])
    seen, run_txt = set(), []
    for r in runs:
        name = r.info.run_name
        if name in seen:
            continue
        seen.add(name)
        params = ", ".join(f"{p.key}={p.value}" for p in (r.data.params or []))
        metrics = "\n".join(f"     {m.key:42s} {m.value:.4f}" for m in sorted(r.data.metrics or [], key=lambda m: m.key))
        run_txt.append(f"run: {name}  ({r.info.run_id})  status={r.info.status.value}\n"
                       f"   params: {params}\n   metrics:\n{metrics}")
    parts = [
        "## A · Offline evaluation — temporal holdout vs. the best-seller baseline (notebook 03b)",
        "Recommenders rebuilt on purchases before the cutoff; scored on products each member bought "
        "for the first time after it.\n",
        table(f"""SELECT recommender, round(hit_rate, 3) AS hit_rate_at_10, round(precision, 3) AS precision_at_10,
       round(recall, 3) AS recall_at_10, round(ndcg, 3) AS ndcg_at_10,
       concat(CAST(round(hit_rate_lift_vs_baseline * 100, 0) AS INT), '%') AS hit_rate_lift,
       concat(CAST(round(ndcg_lift_vs_baseline * 100, 0) AS INT), '%') AS ndcg_lift,
       members_evaluated, cutoff_date
FROM {FQ}.recommendation_offline_eval ORDER BY ndcg DESC"""),
        "## B · Production recommendations (notebook 03)",
        table(f"""SELECT module, count(DISTINCT member_id) AS members, count(*) AS rows
FROM {FQ}.recommendations GROUP BY module ORDER BY module"""),
        table(f"""SELECT reason, count(DISTINCT member_id) AS members FROM {FQ}.recommendations
WHERE module = 'Recommended for you' GROUP BY reason ORDER BY reason"""),
        "## C · GenAI welcome banner — ai_query() + guardrails (notebook 03c)",
        table(f"""SELECT count(*) AS generated,
       round(avg(CAST(passed AS INT)), 3)             AS pass_rate,
       round(avg(CAST(no_medical_claim AS INT)), 3)   AS no_medical_claim,
       round(avg(CAST(no_condition_named AS INT)), 3) AS no_condition_named,
       round(avg(CAST(amount_is_real AS INT)), 3)     AS amount_is_real,
       round(avg(CAST(products_are_real AS INT)), 3)  AS products_are_real,
       round(avg(CAST(length_ok AS INT)), 3)          AS length_ok
FROM {FQ}.member_messages_checked"""),
        "Sample generations that passed (inputs -> model output):\n",
        table(f"""SELECT member_id, first_name, otc_wallet_monthly AS otc, array_join(rec_names, ' | ') AS picks, msg
FROM {FQ}.member_messages_checked WHERE passed ORDER BY member_id LIMIT 6"""),
        "Guardrail self-test — planted bad messages pushed through the same checks every run "
        "(the job fails if any would be served):\n",
        table(f"""SELECT case, expected_fail, msg, passed AS would_be_served
FROM {FQ}.guardrail_selftest ORDER BY case"""),
        "Generations the guardrails rejected (served the safe template instead):\n",
        table(f"""SELECT member_id, msg, no_medical_claim, no_condition_named, amount_is_real, products_are_real, length_ok
FROM {FQ}.member_messages_checked WHERE NOT passed ORDER BY member_id LIMIT 6"""),
        f"## D · MLflow runs (experiment `{args.mlflow_experiment}`, id {exp.experiment_id})",
        block("\n\n".join(run_txt)),
    ]
    write("04_ml_genai.md", "Evidence 04 — ML + GenAI", parts)


# ---------------------------------------------------------------- 05 Genie
if want("05"):
    space = next(s for s in (w.genie.list_spaces().spaces or []) if s.title == "SilverLeaf Product Finder")
    exported = w.genie.get_space(space.space_id, include_serialized_space=True)
    tables = [t["identifier"] for t in json.loads(exported.serialized_space)["data_sources"]["tables"]]
    parts = [f"Genie Agent `{space.title}` ({space.space_id}). Data sources: {tables} — "
             "nothing else. Questions asked live through the Genie Conversation API:\n"]
    for q in ("What do you have for diabetes?",
              "I need something for arthritis pain under $15",
              "What's the cheapest blood pressure monitor?",
              "How much have I spent this year?",
              "Which members have diabetes?"):
        msg = w.genie.start_conversation_and_wait(space.space_id, q)
        answer, gsql, res = "", None, ""
        for att in msg.attachments or []:
            if att.text and att.text.content:
                answer = att.text.content
            if att.query:
                gsql = att.query.query
                r = w.genie.get_message_attachment_query_result(
                    msg.space_id, msg.conversation_id, msg.id, att.attachment_id).statement_response
                cols = [c.name for c in r.manifest.schema.columns]
                rows = r.result.data_array or []
                res = " | ".join(cols) + "\n" + "\n".join(" | ".join(map(str, x)) for x in rows[:8])
                res += f"\n({len(rows)} rows)"
        parts.append(f"---\n\n### Q: {q}\n\n**Genie's answer:**\n\n{answer or '(no text)'}\n")
        parts.append(f"**SQL Genie generated:**\n```sql\n{gsql}\n```\n**Result:**\n{block(res)}" if gsql
                     else "**No SQL generated — outside the catalog scope (guardrail held).**\n")
    write("05_genie.md", "Evidence 05 — Genie Agent (shopper-facing product finder)", parts)


# ---------------------------------------------------------------- 06 App
if want("06"):
    app = w.apps.get(args.app_name)
    dep = app.active_deployment
    logs = subprocess.run(["databricks", "apps", "logs", args.app_name, "--tail-lines", "400",
                           "--profile", args.profile], capture_output=True, text=True).stdout
    keep = [l for l in logs.splitlines() if "[lakebase]" in l or "[genie]" in l]
    parts = [
        "## App status",
        block(f"name:      {app.name}\nurl:       {app.url}\ncompute:   {app.compute_status.state.value}\n"
              f"app:       {app.app_status.state.value} — {app.app_status.message}\n"
              f"service principal: {app.service_principal_client_id}\n"
              f"deployment: {dep.deployment_id}  {dep.status.state.value} — {dep.status.message}"),
        "## Live request log (`databricks apps logs`) — the app serving real member page loads",
        "Each `[lakebase]` line is one home-page render: a Lakebase lookup by member_id. Each "
        "`[genie]` line is an \"Ask SilverLeaf\" question answered by the Genie Agent from inside the app.\n",
        block("\n".join(keep[-40:]) or "(no request lines yet — open the app and click through a few members)"),
        "## Where each part of the page comes from",
        block("Welcome banner            members_online.welcome_message   (GenAI, 03c -> Lakebase)\n"
              "Shop Your Categories      category_recommendations_online  (03 -> Lakebase)\n"
              "Recommended / Buy Again / recommendations_online           (03 -> Lakebase)\n"
              "  Picked for Your Health\n"
              "Ask SilverLeaf            Genie Agent over product_catalog (05, UC view)"),
    ]
    write("06_app.md", "Evidence 06 — Databricks App (SilverLeaf member portal)", parts)
