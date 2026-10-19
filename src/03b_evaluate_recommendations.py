# Databricks notebook source
# MAGIC %md
# MAGIC # 03b · Offline Evaluation — do the recommendations actually beat "show best-sellers"?
# MAGIC
# MAGIC Notebook 03 produces recommendations; this notebook **proves they're worth serving**.
# MAGIC Today every member sees the same best-seller home page, so that is the baseline to beat.
# MAGIC
# MAGIC ### Method — temporal holdout (no peeking at the future)
# MAGIC 1. Pick a **cutoff** = the last `HOLDOUT_DAYS` of order history.
# MAGIC 2. Rebuild every recommender using **only purchases before the cutoff**.
# MAGIC 3. For each member, the "right answers" are the products they bought **for the first
# MAGIC    time after the cutoff** (new-to-member discovery — exactly what the home page is for;
# MAGIC    reorders are handled separately by Buy It Again).
# MAGIC 4. Score each recommender's top-10 against those right answers.
# MAGIC
# MAGIC | Metric | Plain English |
# MAGIC |---|---|
# MAGIC | **HitRate@10** | % of members for whom at least one of the 10 shown products was later bought |
# MAGIC | **Precision@10** | Of the 10 products shown, how many were later bought |
# MAGIC | **Recall@10** | Of the products a member later discovered, how many we had shown |
# MAGIC | **NDCG@10** | Like recall, but rewards putting the right product near the top of the shelf |
# MAGIC
# MAGIC Results are logged to **MLflow** and written to `recommendation_offline_eval` (Unity Catalog),
# MAGIC so the model-selection decision is auditable.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "personalization", "Schema")
dbutils.widgets.text("holdout_days", "60", "Holdout window (days)")
dbutils.widgets.text("mlflow_experiment", "", "MLflow experiment path (blank = default)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
HOLDOUT_DAYS = int(dbutils.widgets.get("holdout_days"))
K = 10
MIN_COOCCURRENCE = 2          # same setting notebook 03 uses in production

import math
import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

mlflow.autolog(disable=True)
_exp = dbutils.widgets.get("mlflow_experiment").strip()
if _exp:
    mlflow.set_experiment(_exp)

spark.sql(f"USE {CATALOG}.{SCHEMA}")
items = spark.table("order_items")
products = spark.table("products")
members = spark.table("members")

# COMMAND ----------

# MAGIC %md ## 1 · Temporal split

# COMMAND ----------

max_date = items.agg(F.max("order_date")).first()[0]
cutoff = spark.sql(f"SELECT date_sub(DATE'{max_date}', {HOLDOUT_DAYS}) AS d").first()["d"]

train = items.filter(F.col("order_date") < F.lit(cutoff))
test = items.filter(F.col("order_date") >= F.lit(cutoff))

train_owned = train.select("member_id", "product_id").distinct()
# Ground truth: products a member bought for the FIRST time after the cutoff.
truth = (test.select("member_id", "product_id").distinct()
         .join(train_owned, ["member_id", "product_id"], "left_anti"))
# Evaluate members who have history to learn from AND something new to discover.
eval_members = truth.select("member_id").distinct().join(
    train_owned.select("member_id").distinct(), "member_id")
truth = truth.join(eval_members, "member_id")

n_eval = eval_members.count()
print(f"cutoff={cutoff}  (history ends {max_date}, holdout {HOLDOUT_DAYS} days)")
print(f"train order lines: {train.count():,}   test order lines: {test.count():,}")
print(f"members evaluated: {n_eval:,}   new-product purchases to predict: {truth.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · The candidate recommenders (all rebuilt from pre-cutoff data only)
# MAGIC
# MAGIC | Recommender | What it does |
# MAGIC |---|---|
# MAGIC | `popularity` **(baseline)** | Same best-sellers for everyone (minus what they own) — today's home page |
# MAGIC | `condition_match` | Best-sellers tagged for the member's health conditions |
# MAGIC | `cooccurrence_cf` | "Members who bought what you bought also bought…" — production engine in notebook 03 |
# MAGIC | `hybrid` | CF score + a health-condition boost + a small popularity tie-breaker |

# COMMAND ----------

def top_k(scored, score_col="score"):
    """Keep each member's top-K unowned products, ranked 1..K."""
    w = Window.partitionBy("member_id").orderBy(F.desc(score_col), F.asc("product_id"))
    return (scored.join(train_owned, ["member_id", "product_id"], "left_anti")
            .join(eval_members, "member_id")
            .withColumn("rank", F.row_number().over(w))
            .filter(F.col("rank") <= K)
            .select("member_id", "product_id", "rank"))

# Popularity from training data only.
pop = train.groupBy("product_id").agg(F.sum("quantity").alias("units"))
max_units = pop.agg(F.max("units")).first()[0]
pop = pop.withColumn("pop_norm", F.col("units") / F.lit(max_units))

# --- baseline: global best-sellers ---
popularity = top_k(eval_members.crossJoin(pop.select("product_id", F.col("units").alias("score"))))

# --- condition-matched best-sellers ---
member_cond = members.select("member_id", F.explode("health_conditions").alias("condition"))
prod_cond = products.select("product_id", F.explode("condition_tags").alias("condition"))
cond_match_pairs = (member_cond.join(prod_cond, "condition")
                    .select("member_id", "product_id").distinct())
condition_match = top_k(cond_match_pairs.join(pop, "product_id")
                        .select("member_id", "product_id", F.col("units").alias("score")))

# --- item-to-item co-occurrence CF (same logic as notebook 03, on train only) ---
a = train_owned.withColumnRenamed("product_id", "p1")
b = train_owned.withColumnRenamed("product_id", "p2")
pairs = (a.join(b, "member_id").filter(F.col("p1") < F.col("p2"))
         .groupBy("p1", "p2").agg(F.count("*").alias("cooc"))
         .filter(F.col("cooc") >= MIN_COOCCURRENCE))
related = (pairs.select(F.col("p1").alias("anchor"), F.col("p2").alias("product_id"), "cooc")
           .unionByName(pairs.select(F.col("p2").alias("anchor"), F.col("p1").alias("product_id"), "cooc")))
cf_scores = (train_owned.withColumnRenamed("product_id", "anchor")
             .join(related, "anchor")
             .groupBy("member_id", "product_id").agg(F.sum("cooc").alias("cf")))
cooccurrence_cf = top_k(cf_scores.withColumnRenamed("cf", "score"))

# --- hybrid: normalized CF + condition boost + popularity tie-breaker ---
cf_max = cf_scores.groupBy("member_id").agg(F.max("cf").alias("cf_max"))
hybrid_scored = (
    cf_scores.join(cf_max, "member_id")
    .withColumn("cf_norm", F.col("cf") / F.col("cf_max"))
    .select("member_id", "product_id", "cf_norm")
    .join(cond_match_pairs.withColumn("cond", F.lit(1.0)), ["member_id", "product_id"], "full_outer")
    .join(pop.select("product_id", "pop_norm"), "product_id", "left")
    .fillna({"cf_norm": 0.0, "cond": 0.0, "pop_norm": 0.0})
    .withColumn("score", F.col("cf_norm") + 0.5 * F.col("cond") + 0.1 * F.col("pop_norm"))
)
hybrid = top_k(hybrid_scored)

CANDIDATES = {
    "popularity": popularity,
    "condition_match": condition_match,
    "cooccurrence_cf": cooccurrence_cf,
    "hybrid": hybrid,
}

# COMMAND ----------

# MAGIC %md ## 3 · Score every recommender against what members actually bought next

# COMMAND ----------

# Ideal DCG depends on how many right answers each member has (capped at K).
n_truth = truth.groupBy("member_id").agg(F.count("*").alias("n_truth"))
idcg_lookup = {n: sum(1.0 / math.log2(i + 2) for i in range(min(n, K))) for n in range(1, K + 1)}
idcg_expr = F.create_map(*[x for n, v in idcg_lookup.items() for x in (F.lit(n), F.lit(v))])

def evaluate(name, recs):
    hits = recs.join(truth.withColumn("hit", F.lit(1)), ["member_id", "product_id"], "left") \
               .fillna({"hit": 0})
    per_member = (
        hits.groupBy("member_id")
        .agg(F.sum("hit").alias("hits"),
             F.sum(F.col("hit") / F.log2(F.col("rank") + 1)).alias("dcg"))
        .join(n_truth, "member_id", "right")             # members with no recs score 0
        .fillna({"hits": 0, "dcg": 0.0})
        .withColumn("idcg", idcg_expr[F.least(F.col("n_truth"), F.lit(K))])
    )
    m = per_member.agg(
        F.avg((F.col("hits") > 0).cast("double")).alias("hit_rate"),
        F.avg(F.col("hits") / K).alias("precision"),
        F.avg(F.col("hits") / F.col("n_truth")).alias("recall"),
        F.avg(F.col("dcg") / F.col("idcg")).alias("ndcg"),
    ).first().asDict()
    coverage = recs.select("member_id").distinct().count() / n_eval
    return {"recommender": name, **{k: float(v) for k, v in m.items()}, "member_coverage": coverage}

results = [evaluate(name, recs) for name, recs in CANDIDATES.items()]
base = next(r for r in results if r["recommender"] == "popularity")
for r in results:
    r["hit_rate_lift_vs_baseline"] = r["hit_rate"] / base["hit_rate"] - 1 if base["hit_rate"] else None
    r["ndcg_lift_vs_baseline"] = r["ndcg"] / base["ndcg"] - 1 if base["ndcg"] else None

eval_df = (spark.createDataFrame(results)
           .withColumn("k", F.lit(K))
           .withColumn("cutoff_date", F.lit(cutoff))
           .withColumn("members_evaluated", F.lit(n_eval))
           .withColumn("evaluated_at", F.current_timestamp()))
eval_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("recommendation_offline_eval")
spark.sql("COMMENT ON TABLE recommendation_offline_eval IS "
          "'Temporal-holdout offline evaluation of home-page recommenders (written by notebook 03b).'")

print(f"{'recommender':18s} {'HitRate@10':>10s} {'Prec@10':>8s} {'Recall@10':>9s} {'NDCG@10':>8s} {'HR lift':>8s}")
for r in sorted(results, key=lambda r: -r["ndcg"]):
    lift = f"{r['hit_rate_lift_vs_baseline']:+.0%}" if r["hit_rate_lift_vs_baseline"] is not None else "n/a"
    print(f"{r['recommender']:18s} {r['hit_rate']:10.3f} {r['precision']:8.3f} "
          f"{r['recall']:9.3f} {r['ndcg']:8.3f} {lift:>8s}")

# COMMAND ----------

# MAGIC %md ## 4 · Log to MLflow and pick the champion

# COMMAND ----------

champion = max(results, key=lambda r: r["ndcg"])
with mlflow.start_run(run_name="home_page_recs_offline_eval"):
    mlflow.log_params({"k": K, "holdout_days": HOLDOUT_DAYS, "cutoff_date": str(cutoff),
                       "min_cooccurrence": MIN_COOCCURRENCE, "members_evaluated": n_eval,
                       "champion": champion["recommender"]})
    for r in results:
        for metric in ("hit_rate", "precision", "recall", "ndcg", "member_coverage"):
            mlflow.log_metric(f"{r['recommender']}.{metric}", r[metric])
    mlflow.log_table(data=eval_df.drop("evaluated_at").toPandas(), artifact_file="offline_eval.json")

print(f"Champion by NDCG@10: {champion['recommender']}  "
      f"(HitRate@10 {champion['hit_rate']:.1%} vs baseline {base['hit_rate']:.1%})")
display(eval_df.orderBy(F.desc("ndcg")))
