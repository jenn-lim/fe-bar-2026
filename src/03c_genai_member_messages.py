# Databricks notebook source
# MAGIC %md
# MAGIC # 03c · GenAI — a personalized, guard-railed welcome message for every member
# MAGIC
# MAGIC The shelves tell a member *what*; this tells them *why*, in one warm sentence at the top of
# MAGIC the home page — e.g. *"Robert, you still have $50 of OTC allowance this month — your usual
# MAGIC test strips and a low-sodium meal pick are ready below."*
# MAGIC
# MAGIC **How:** `ai_query()` calls a Databricks-hosted foundation model in **batch** (the endpoint
# MAGIC must support batch inference — Claude Sonnet 4.6 by default), straight from SQL, over the member's profile + their top recommendations. No endpoint to manage, governed
# MAGIC by Unity Catalog, runs on the same nightly schedule as everything else.
# MAGIC
# MAGIC **Guardrails (this is a Medicare population):** every generated message is checked before it
# MAGIC can reach a member. A message is **rejected** — and replaced with a safe template — if it
# MAGIC makes a medical claim (cure / treat / diagnose / prescribe…), names a product that wasn't
# MAGIC actually recommended, quotes a dollar amount other than the member's real allowance, or runs
# MAGIC too long. The pass rate is logged to MLflow so drift in the model's behaviour is visible.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "personalization", "Schema")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6", "Foundation model endpoint")
dbutils.widgets.text("mlflow_experiment", "", "MLflow experiment path (blank = default)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM = dbutils.widgets.get("llm_endpoint").strip()
MAX_WORDS = 45

import mlflow
mlflow.autolog(disable=True)
_exp = dbutils.widgets.get("mlflow_experiment").strip()
if _exp:
    mlflow.set_experiment(_exp)
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 1 · Build each member's prompt context (profile + top 3 recommendations)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW message_context AS
WITH top_recs AS (
  SELECT member_id,
         collect_list(named_struct('module', module, 'rank', rank, 'name', product_name)) AS recs
  FROM recommendations
  WHERE rank <= 2 AND module IN ('Buy Again', 'Based on your conditions', 'Recommended for you')
  GROUP BY member_id
)
SELECT m.member_id, m.first_name, m.otc_wallet_monthly,
       concat_ws(', ', m.health_conditions) AS conditions,
       slice(array_distinct(transform(array_sort(t.recs, (l, r) -> CASE WHEN l.rank < r.rank THEN -1
                                                           WHEN l.rank > r.rank THEN 1 ELSE 0 END),
                                      x -> x.name)), 1, 3) AS rec_names
FROM members m JOIN top_recs t USING (member_id)
""")
print("members with message context:", spark.table("message_context").count())

# COMMAND ----------

# MAGIC %md ## 2 · Generate in batch with `ai_query()`

# COMMAND ----------

PROMPT = (
    "You write the one-sentence welcome banner on a Medicare OTC benefits store home page. "
    "Write ONE friendly sentence (max 35 words) to the member, using their first name. "
    "You may mention their monthly OTC allowance amount and AT MOST two of the listed products, "
    "using the exact product names given. Do NOT give medical advice, do NOT claim any product "
    "treats, cures, prevents or diagnoses anything, do NOT mention their health conditions by name, "
    "and do NOT invent products, prices, or discounts. Output only the sentence, as plain text "
    "(no markdown, no quotes)."
)

spark.sql(f"""
CREATE OR REPLACE TABLE member_messages_raw AS
SELECT member_id, first_name, otc_wallet_monthly, rec_names,
       ai_query(
         '{LLM}',
         concat('{PROMPT}',
                '\\n\\nMember first name: ', first_name,
                '\\nMonthly OTC allowance: $', CAST(otc_wallet_monthly AS STRING),
                '\\nProducts picked for them: ', array_join(rec_names, '; ')),
         modelParameters => named_struct('max_tokens', 120, 'temperature', 0.4),
         failOnError => false
       ) AS gen
FROM message_context
""")
raw = spark.table("member_messages_raw")
print("generated:", raw.count(), " errors:", raw.filter("gen.errorMessage IS NOT NULL").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Guardrail checks — only messages that pass every check are served
# MAGIC
# MAGIC | Check | Rejects |
# MAGIC |---|---|
# MAGIC | `no_medical_claim` | whole words: treat*, cure(s/d), prevent*, diagnos*, prescri*, heal(s/ed/ing), therap*, disease(s) — not "health" |
# MAGIC | `no_condition_named` | the member's own condition names (privacy — banner is visible on screen) |
# MAGIC | `amount_is_real` | any `$` amount other than the member's actual monthly allowance |
# MAGIC | `products_are_real` | product-looking names (brand prefixes) that weren't in their recommendations |
# MAGIC | `length_ok` | more than 45 words, or empty / errored generations |

# COMMAND ----------

BRANDS = ["Premier", "ValueCare", "GoldStandard", "EverWell", "PureLife", "ActiveSenior"]
brand_alt = "|".join(BRANDS)

def guardrails(source: str) -> str:
    """SQL applying every guardrail check to `source` (needs msg, conditions, otc_wallet_monthly, rec_names)."""
    return f"""
    WITH chk AS (
      SELECT *,
        msg IS NOT NULL AND size(split(msg, '\\\\s+')) <= {MAX_WORDS}                AS length_ok,
        NOT coalesce(msg RLIKE '(?i)\\\\b(treat\\\\w*|cure[sd]?|prevent\\\\w*|diagnos\\\\w*|prescri\\\\w*|heal(s|ed|ing)?|therap\\\\w*|diseases?)\\\\b', false)
                                                                                  AS no_medical_claim,
        NOT coalesce(exists(split(conditions, ', '),
                            c -> c <> '' AND lower(msg) LIKE concat('%', replace(c, '_', ' '), '%')), false)
                                                                                  AS no_condition_named,
        forall(regexp_extract_all(coalesce(msg, ''), '\\\\$\\\\s?([0-9]+(?:\\\\.[0-9]{{2}})?)', 1),
               a -> CAST(a AS DOUBLE) = CAST(otc_wallet_monthly AS DOUBLE))       AS amount_is_real,
        forall(regexp_extract_all(coalesce(msg, ''),
                                  '((?:{brand_alt}) [A-Z][A-Za-z0-9-]*(?: (?:&|[A-Z][A-Za-z0-9-]*))*)', 1),
               p -> exists(rec_names, n -> lower(n) = lower(p)))                  AS products_are_real
      FROM {source}
    )
    SELECT *, (length_ok AND no_medical_claim AND no_condition_named
               AND amount_is_real AND products_are_real) AS passed
    FROM chk"""

spark.sql("""
CREATE OR REPLACE TEMP VIEW generated AS
SELECT r.*, c.conditions,
       trim(regexp_replace(regexp_replace(gen.result, '[*_`]', ''), '^"|"$', '')) AS msg
FROM member_messages_raw r JOIN message_context c USING (member_id)
""")
spark.sql(f"CREATE OR REPLACE TABLE member_messages_checked AS {guardrails('generated')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Guardrail self-test — prove the checks catch bad output
# MAGIC Real generations almost always pass, which says nothing about whether the checks *work*.
# MAGIC So every run also pushes a fixed set of planted bad messages through the same checks, and
# MAGIC **the job fails** if any of them would have been served.

# COMMAND ----------

RED_TEAM = [
    # (case, msg, expected failing check)
    ("medical_claim", "Hi Mary! PureLife Glucose Tablets will treat your blood sugar this month.", "no_medical_claim"),
    ("cure_claim", "Mary, ValueCare Fish Oil can cure heart problems — use your $50 today!", "no_medical_claim"),
    ("heals_claim", "Mary, PureLife Glucose Tablets help your body heal faster — $50 to spend!", "no_medical_claim"),
    ("condition_named", "Mary, since you have diabetes, your $50 allowance covers PureLife Glucose Tablets.", "no_condition_named"),
    ("invented_amount", "Great news Mary — you have a $500 bonus credit for PureLife Glucose Tablets!", "amount_is_real"),
    ("invented_product", "Mary, try the new PureLife Miracle Sugar Blocker with your $50 allowance.", "products_are_real"),
    ("too_long", "Mary " + "really " * 50 + "welcome back.", "length_ok"),
]
red = spark.createDataFrame(
    [(c, m, e, "Mary", 50, "diabetes, heart_disease", ["PureLife Glucose Tablets", "ValueCare Fish Oil"])
     for c, m, e in RED_TEAM],
    "case string, msg string, expected_fail string, first_name string, otc_wallet_monthly int, "
    "conditions string, rec_names array<string>")
red.createOrReplaceTempView("red_team")
spark.sql(f"CREATE OR REPLACE TABLE guardrail_selftest AS {guardrails('red_team')}")
selftest = spark.sql("""
  SELECT case, expected_fail, passed AS would_be_served,
         NOT (CASE expected_fail WHEN 'no_medical_claim' THEN no_medical_claim
                                 WHEN 'no_condition_named' THEN no_condition_named
                                 WHEN 'amount_is_real' THEN amount_is_real
                                 WHEN 'products_are_real' THEN products_are_real
                                 WHEN 'length_ok' THEN length_ok END) AS caught_by_expected_check
  FROM guardrail_selftest ORDER BY case
""")
display(selftest)
leaked = selftest.filter("would_be_served OR NOT caught_by_expected_check").count()
assert leaked == 0, f"{leaked} planted bad message(s) slipped past the guardrails"
print(f"guardrail self-test: {len(RED_TEAM)}/{len(RED_TEAM)} planted bad messages blocked")

# COMMAND ----------

checks = spark.sql("""
  SELECT count(*) AS n,
         avg(CAST(passed AS INT))             AS pass_rate,
         avg(CAST(length_ok AS INT))          AS length_ok,
         avg(CAST(no_medical_claim AS INT))   AS no_medical_claim,
         avg(CAST(no_condition_named AS INT)) AS no_condition_named,
         avg(CAST(amount_is_real AS INT))     AS amount_is_real,
         avg(CAST(products_are_real AS INT))  AS products_are_real
  FROM member_messages_checked
""").first().asDict()
for k, v in checks.items():
    print(f"  {k:20s} {v:.3f}" if k != "n" else f"  messages checked     {v:,}")

# COMMAND ----------

# MAGIC %md ## 4 · Publish `member_messages` (LLM text if it passed, safe template otherwise)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE member_messages AS
SELECT member_id,
       CASE WHEN passed THEN msg
            ELSE concat('Welcome back, ', first_name, '! You have $',
                        CAST(otc_wallet_monthly AS STRING),
                        ' in OTC allowance this month — your picks are ready below.')
       END AS welcome_message,
       CASE WHEN passed THEN 'genai' ELSE 'template_fallback' END AS message_source,
       current_timestamp() AS generated_at
FROM member_messages_checked
""")
spark.sql("COMMENT ON TABLE member_messages IS 'Per-member home-page welcome banner: "
          "ai_query-generated and guardrail-checked, template fallback on any failed check (notebook 03c).'")

with mlflow.start_run(run_name="member_messages_genai"):
    mlflow.log_params({"llm_endpoint": LLM, "max_words": MAX_WORDS, "temperature": 0.4})
    mlflow.log_metrics({f"guardrail.{k}": float(v) for k, v in checks.items() if k != "n"})
    mlflow.log_metric("messages_generated", float(checks["n"]))
    mlflow.log_metric("guardrail.selftest_blocked", float(len(RED_TEAM) - leaked))
    mlflow.log_text(PROMPT, "prompt.txt")

display(spark.sql("""
  SELECT message_source, count(*) AS members FROM member_messages GROUP BY message_source
"""))
display(spark.sql("""
  SELECT member_id, msg, length_ok, no_medical_claim, no_condition_named, amount_is_real,
         products_are_real, passed
  FROM member_messages_checked ORDER BY member_id LIMIT 8
"""))
