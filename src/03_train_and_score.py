# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Build Recommendations  →  `recommendations` + `category_recommendations`
# MAGIC
# MAGIC ### How these recommendations work (no ML background needed)
# MAGIC By default there is **no heavy "model training"** here and nothing is a black box — the
# MAGIC home page is built from four simple, explainable ideas, each just counting and ranking.
# MAGIC (One section, "Recommended for You", can *optionally* use a trained **ALS** model
# MAGIC instead — see the `cf_method` parameter below.) The four ideas:
# MAGIC
# MAGIC | Home-page section | How it's built |
# MAGIC |---|---|
# MAGIC | **🛍️ Shop Your Categories** | Rank the product categories for each member by how much they buy there + their health conditions. |
# MAGIC | **✨ Recommended for You** | "Members who bought what you bought also bought…" (item-to-item collaborative filtering). |
# MAGIC | **🔁 Buy It Again** | Consumable products the member has reordered before. |
# MAGIC | **🩺 Picked for Your Health** | Top products tagged for the member's health conditions that they haven't bought yet. |
# MAGIC
# MAGIC #### What is "item-to-item collaborative filtering"? (the only non-obvious one)
# MAGIC 1. Look at every pair of products and count **how many members bought both**. If many
# MAGIC    people who buy glucose strips also buy lancets, those two are "related".
# MAGIC 2. To recommend for one member, take everything **they** have bought, look up each
# MAGIC    item's related products, and **add up the relatedness scores**.
# MAGIC 3. Show the highest-scoring products they don't already own.
# MAGIC
# MAGIC It needs **no training and no tuning** — just counting co-purchases, and it runs on
# MAGIC serverless. A trained-model alternative (**ALS matrix factorization**) is also available
# MAGIC via the `cf_method` parameter (best on classic compute — see the "Recommended for You"
# MAGIC section below for why).
# MAGIC
# MAGIC We log a couple of run metrics to **MLflow** so you have a record of each run, but the
# MAGIC logic is fully transparent in the code below.

# COMMAND ----------

# ---- CONFIGURATION ----
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "personalization", "Schema")
# Same "bring your own data" overrides as notebook 02 (blank = use {catalog}.{schema}).
dbutils.widgets.text("src_members", "", "Source: members")
dbutils.widgets.text("src_products", "", "Source: products")
dbutils.widgets.text("src_order_items", "", "Source: order_items")
# Engine for "Recommended for You": "cooccurrence" (default, serverless) or "als" (classic compute).
dbutils.widgets.dropdown("cf_method", "hybrid", ["hybrid", "cooccurrence", "als"],
                         "Recommended-for-You engine")
# Optional: a fixed MLflow experiment path (blank = the notebook's default experiment).
dbutils.widgets.text("mlflow_experiment", "", "MLflow experiment path (blank = default)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
CF_METHOD = dbutils.widgets.get("cf_method")

# How many items/categories to recommend per section.
TOP_N = 10
# A product pair must be co-purchased by at least this many members to count as "related".
# Higher = more conservative recommendations. (Used by the co-occurrence engine only.)
MIN_COOCCURRENCE = 2

import mlflow
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Databricks notebooks enable MLflow autologging by default, which auto-serializes a fitted
# Spark model. On serverless that serialization trips the Spark Connect ML model-size guard,
# so we disable it and log only what we choose. (No effect on the co-occurrence engine.)
mlflow.autolog(disable=True)

# Send runs to an explicit experiment if one was provided (else the notebook default).
_exp = dbutils.widgets.get("mlflow_experiment").strip()
if _exp:
    mlflow.set_experiment(_exp)


def source(entity: str, override: str) -> str:
    """Resolve a source table name (override FQN, else {CATALOG}.{SCHEMA}.{entity})."""
    override = (override or "").strip()
    return override if override else f"{CATALOG}.{SCHEMA}.{entity}"


SRC_MEMBERS = source("members", dbutils.widgets.get("src_members"))
SRC_PRODUCTS = source("products", dbutils.widgets.get("src_products"))
SRC_ORDER_ITEMS = source("order_items", dbutils.widgets.get("src_order_items"))

spark.sql(f"USE {CATALOG}.{SCHEMA}")   # feature tables from notebook 02 live here

products = spark.table(SRC_PRODUCTS)
items = spark.table(SRC_ORDER_ITEMS)
members = spark.table(SRC_MEMBERS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✨ "Recommended for You" — collaborative filtering (two engines)
# MAGIC
# MAGIC This section supports **three interchangeable engines**, selected by the `cf_method`
# MAGIC parameter. Both produce the **same output columns**, so everything downstream is identical.
# MAGIC
# MAGIC | `cf_method` | What it is | Trained model? | Compute |
# MAGIC |---|---|---|---|
# MAGIC | `hybrid` *(default)* | Co-occurrence + health-condition boost + popularity — the 03b evaluation champion | No | Serverless ✅ |
# MAGIC | `cooccurrence` | "Members who bought X also bought Y" — count co-purchases and rank | No | Serverless ✅ |
# MAGIC | `als` | Spark MLlib **ALS** matrix factorization — learns latent member/product "taste" factors | **Yes** | Classic compute (see note) |
# MAGIC
# MAGIC > **ALS is supported by `pyspark.ml` on serverless and trains there** — but serverless
# MAGIC > enforces a Spark Connect ML **model-size guard** (256 MB on this environment), and the
# MAGIC > fitted ALS model is measured above it (300–900 MB, nondeterministically), so ALS — and
# MAGIC > especially persisting the model below — is **reliable on classic compute** (no such
# MAGIC > guard) or wherever the limit can be raised. The code handles two other serverless
# MAGIC > specifics regardless: (1) MLflow **autologging is disabled** so the model isn't
# MAGIC > auto-serialized into the size-limited model cache; (2) recommendations are scored from
# MAGIC > the model's **factors directly** (explode → join → sum), not `recommendForAllUsers()`,
# MAGIC > which uses higher-order functions Unity Catalog rejects on serverless.
# MAGIC >
# MAGIC > **Bottom line:** keep the default `hybrid` (or `cooccurrence`) on serverless; use `als` on a classic
# MAGIC > job/all-purpose cluster (Databricks ML runtime).
# MAGIC
# MAGIC Both engines start from the same `owned` table (each member's distinct purchases) and
# MAGIC produce a `scored` DataFrame of `(member_id, product_id, cf_score)`.

# COMMAND ----------

# Shared by both engines (and by the cold-start / condition sections below):
# each member's distinct purchased products.
owned = items.select("member_id", "product_id").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engine A — item-to-item co-occurrence (default, no training)
# MAGIC Count how many members bought each *pair* of products, then for each member sum the
# MAGIC relatedness of everything they own and keep the products they don't have yet.

# COMMAND ----------

def cf_via_cooccurrence():
    """Recommend via item-to-item co-occurrence counting.

    No model is trained — this is pure counting and runs on serverless. Returns a
    ``(scored_df, reason_text)`` tuple where ``scored_df`` has columns
    ``member_id, product_id, cf_score`` (already-owned products removed).
    """
    a = owned.withColumnRenamed("product_id", "p1")
    b = owned.withColumnRenamed("product_id", "p2")
    pairs = (
        a.join(b, "member_id")
        .filter(F.col("p1") < F.col("p2"))                 # each unordered pair once
        .groupBy("p1", "p2").agg(F.count("*").alias("cooc"))
        .filter(F.col("cooc") >= MIN_COOCCURRENCE)
    )
    related = (                                            # symmetric edge list
        pairs.select(F.col("p1").alias("anchor"), F.col("p2").alias("related"), "cooc")
        .unionByName(pairs.select(F.col("p2").alias("anchor"), F.col("p1").alias("related"), "cooc"))
    )
    scored = (
        owned.join(related, owned.product_id == related.anchor)
        .groupBy("member_id", "related").agg(F.sum("cooc").alias("cf_score"))
        .withColumnRenamed("related", "product_id")
        .join(owned, ["member_id", "product_id"], "left_anti")
    )
    return scored, "Members like you also bought this"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engine A+ — hybrid (default; the offline-evaluation champion)
# MAGIC Notebook `03b` scores every engine on a temporal holdout. Co-occurrence alone beats the
# MAGIC best-seller page, but blending it with health-condition relevance wins clearly — so the
# MAGIC hybrid is what we serve:
# MAGIC
# MAGIC ```
# MAGIC score = co-occurrence score / member's max        # what similar members bought (0-1)
# MAGIC       + 0.5 if tagged for one of their conditions # health relevance
# MAGIC       + 0.1 x global popularity (0-1)             # tie-breaker
# MAGIC ```
# MAGIC Same weights as the `hybrid` candidate in 03b, so what we evaluate is what we ship.

# COMMAND ----------

def cf_via_hybrid():
    """Co-occurrence CF blended with condition relevance and popularity (03b champion).

    Returns ``(scored_df, reason)`` with columns ``member_id, product_id, cf_score``.
    """
    cooc, _ = cf_via_cooccurrence()
    cf_max = cooc.groupBy("member_id").agg(F.max("cf_score").alias("cf_max"))
    cond_pairs = (
        members.select("member_id", F.explode("health_conditions").alias("condition"))
        .join(products.select("product_id", F.explode("condition_tags").alias("condition")), "condition")
        .select("member_id", "product_id").distinct()
        .join(owned, ["member_id", "product_id"], "left_anti")
        .join(owned.select("member_id").distinct(), "member_id")   # members with history only
        .withColumn("cond", F.lit(1.0))
    )
    pop = spark.table("product_popularity")
    max_units = pop.agg(F.max("units_sold")).first()[0] or 1
    scored = (
        cooc.join(cf_max, "member_id")
        .select("member_id", "product_id", (F.col("cf_score") / F.col("cf_max")).alias("cf_norm"))
        .join(cond_pairs, ["member_id", "product_id"], "full_outer")
        .join(pop.select("product_id", (F.col("units_sold") / F.lit(max_units)).alias("pop_norm")),
              "product_id", "left")
        .fillna({"cf_norm": 0.0, "cond": 0.0, "pop_norm": 0.0})
        .withColumn("cf_score", F.col("cf_norm") + 0.5 * F.col("cond") + 0.1 * F.col("pop_norm"))
        .select("member_id", "product_id", "cf_score")
    )
    return scored, "Members like you also bought this"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engine B — ALS matrix factorization (optional trained model, serverless)
# MAGIC ALS *learns* a short "taste vector" for every member and every product from the implicit
# MAGIC purchase signal in `member_product_interactions` (built in notebook 02), then scores every
# MAGIC product for every member by comparing vectors. Unlike co-occurrence, it can surface a
# MAGIC good-fit product even when there's no direct co-purchase link. We log its parameters to
# MAGIC MLflow (the model artifact itself isn't auto-logged — see the autolog note above).

# COMMAND ----------

def cf_via_als():
    """Recommend via Spark MLlib ALS matrix factorization (a TRAINED model).

    Runs on serverless. Trains ALS on the implicit purchase signal and returns
    ``(scored_df, reason)`` with the same ``member_id, product_id, cf_score`` schema as the
    co-occurrence engine. Two serverless specifics are handled: MLflow autologging is disabled
    (so the model isn't auto-serialized into the size-limited Spark Connect ML cache), and
    scoring is done from the factor matrices rather than ``recommendForAllUsers`` (which uses
    UC-unsupported higher-order functions).
    """
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.recommendation import ALS

    # Serverless (Spark Connect ML) caps the in-session model size; ALS factor blocks can
    # exceed the default. Raise the limit so the fitted model fits in the session cache.
    try:
        spark.conf.set("spark.connect.session.connectML.mlCache.memoryControl.maxModelSize", "4g")
    except Exception as e:
        print(f"(could not set maxModelSize: {e})")

    interactions = spark.table("member_product_interactions")   # member_id, product_id, rating

    # ALS needs integer ids — index the string ids and keep maps to translate back.
    m_indexer = StringIndexer(inputCol="member_id", outputCol="m_idx", handleInvalid="keep").fit(interactions)
    p_indexer = StringIndexer(inputCol="product_id", outputCol="p_idx", handleInvalid="keep").fit(interactions)
    idx = (p_indexer.transform(m_indexer.transform(interactions))
           .withColumn("m_idx", F.col("m_idx").cast("int"))
           .withColumn("p_idx", F.col("p_idx").cast("int")))
    m_map = idx.select("member_id", "m_idx").distinct()
    p_map = idx.select("product_id", "p_idx").distinct()

    als = ALS(
        userCol="m_idx", itemCol="p_idx", ratingCol="rating",
        implicitPrefs=True,              # purchases are implicit positive signals, not 1-5 ratings
        rank=24,                         # size of each member/product taste vector
        maxIter=12, regParam=0.08, alpha=20.0,
        coldStartStrategy="drop", nonnegative=True, seed=42,
    )
    model = als.fit(idx)                                          # <-- the training step
    mlflow.log_params({"als_rank": 24, "als_maxIter": 12, "als_regParam": 0.08, "als_alpha": 20.0})

    # Grab the learned factor matrices once (rename the ALS "id" to our index column so joins
    # are by name — avoids cross-DataFrame column references that Spark Connect can't resolve).
    uf_raw = model.userFactors.withColumnRenamed("id", "m_idx")    # m_idx, features:array
    itf_raw = model.itemFactors.withColumnRenamed("id", "p_idx")   # p_idx, features:array

    # Persist the trained model as its learned factor tables (keyed by the real ids). We don't
    # use mlflow.spark.log_model() because serializing the Spark model trips the serverless
    # Spark Connect ML model-size guard; the factor matrices ARE the model's learned content,
    # and as Delta tables they're reproducible, inspectable, and reusable for scoring later.
    (uf_raw.join(m_map, "m_idx").select("member_id", "features")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("als_user_factors"))
    (itf_raw.join(p_map, "p_idx").select("product_id", "features")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("als_item_factors"))
    mlflow.log_param("als_factor_tables", f"{CATALOG}.{SCHEMA}.als_user_factors, als_item_factors")
    print("Persisted ALS factors to als_user_factors / als_item_factors")

    # Score every member×product from the learned factors. We compute the dot product of the
    # user and item "taste vectors" ourselves — explode each factor array to (dimension, value),
    # join on dimension, multiply, and sum — instead of model.recommendForAllUsers(). That
    # built-in uses Spark higher-order functions, which Unity Catalog on serverless rejects;
    # posexplode + join + sum uses only standard operations and runs fine on serverless.
    uf = uf_raw.selectExpr("m_idx", "posexplode(features) as (pos, uval)")
    itf = itf_raw.selectExpr("p_idx", "posexplode(features) as (pos, ival)")
    scores = (
        uf.join(itf, "pos")
        .groupBy("m_idx", "p_idx")
        .agg(F.sum(F.col("uval") * F.col("ival")).alias("cf_score"))
    )
    scored = (
        scores.join(m_map, "m_idx").join(p_map, "p_idx")
        .select("member_id", "product_id", "cf_score")
        .join(owned, ["member_id", "product_id"], "left_anti")
    )
    return scored, "Recommended for you (ALS)"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the selected engine and build `cf_recs`
# MAGIC Whichever engine runs, we take its scores, keep each member's top `TOP_N`, attach product
# MAGIC details, and tag the rows as the "Recommended for you" module. Run info goes to MLflow.

# COMMAND ----------

with mlflow.start_run(run_name=f"home_page_recs_{CF_METHOD}"):
    mlflow.log_params({"cf_method": CF_METHOD, "top_n": TOP_N,
                       "min_cooccurrence": MIN_COOCCURRENCE})
    if CF_METHOD == "als":
        scored, cf_reason = cf_via_als()
    elif CF_METHOD == "hybrid":
        scored, cf_reason = cf_via_hybrid()
    else:
        scored, cf_reason = cf_via_cooccurrence()

    cf_recs = (
        scored
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("member_id").orderBy(F.desc("cf_score"))))
        .filter(F.col("rank") <= TOP_N)
        .join(products, "product_id")
        .select("member_id", "product_id", "product_name", "category", "price", "rank",
                F.lit("Recommended for you").alias("module"),
                F.lit(cf_reason).alias("reason"))
    )
    n_covered = cf_recs.select("member_id").distinct().count()
    mlflow.log_metric("members_with_cf_recs", n_covered)
    print(f"[cf_method={CF_METHOD}] Recommended-for-You generated for {n_covered:,} members")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 "Buy It Again" — reordered consumables
# MAGIC
# MAGIC The simplest, highest-converting section: consumable products (vitamins, test strips,
# MAGIC etc.) the member has bought **2+ times**, ranked by how often and how recently. No model —
# MAGIC just their own reorder history.

# COMMAND ----------

buy_again = (
    items.join(products.filter("is_consumable = true").select("product_id"), "product_id")
    .groupBy("member_id", "product_id")
    .agg(F.count("*").alias("times_bought"),
         F.max("order_date").alias("last_bought"))
    .filter(F.col("times_bought") >= 2)
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("member_id").orderBy(F.desc("times_bought"), F.desc("last_bought"))))
    .filter(F.col("rank") <= TOP_N)
    .join(products, "product_id")
    .select("member_id", "product_id", "product_name", "category", "price", "rank",
            F.lit("Buy Again").alias("module"),
            F.concat(F.lit("Reordered "), F.col("times_bought"), F.lit(" times")).alias("reason"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🩺 "Picked for Your Health" — condition-matched products
# MAGIC
# MAGIC For each member, take the products tagged for their health conditions (e.g. diabetes →
# MAGIC glucose tablets), drop anything they already buy or already see on Recommended for You,
# MAGIC and keep the most popular ones. This
# MAGIC is how health relevance shows up on the home page.

# COMMAND ----------

cond_pop = spark.table("product_popularity_by_condition")
member_conditions = members.select("member_id", F.explode_outer("health_conditions").alias("condition"))

cond_recs = (
    member_conditions
    .join(cond_pop, "condition")
    .join(owned, ["member_id", "product_id"], "left_anti")
    # keep the shelves distinct: skip anything already on this member's Recommended-for-You
    .join(cf_recs.select("member_id", "product_id"), ["member_id", "product_id"], "left_anti")
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("member_id").orderBy(F.desc("units_sold"))))
    .filter(F.col("rank") <= TOP_N)
    .select("member_id", "product_id", "product_name", "category", "price", "rank",
            F.lit("Based on your conditions").alias("module"),
            F.concat(F.lit("Relevant to your "), F.col("condition")).alias("reason"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛍️ "Shop Your Categories" — personalized category ordering
# MAGIC
# MAGIC The business wants to personalize **categories**, not just items — i.e. reorder the
# MAGIC "Shop Popular Categories" carousel per member. We score every member×category by:
# MAGIC
# MAGIC ```
# MAGIC score = (units bought in that category)        # what they actually shop
# MAGIC       + 8  if the category matches a condition  # health relevance boost
# MAGIC       + a small global-popularity term          # tie-breaker / cold-start
# MAGIC ```
# MAGIC
# MAGIC Every member gets **all** categories ranked, so the app can order the whole carousel.

# COMMAND ----------

prod_cat = products.select("product_id", "category")
all_categories = products.select("category").distinct()

cat_affinity = (
    items.join(prod_cat, "product_id")
    .groupBy("member_id", "category").agg(F.sum("quantity").alias("units"))
)
cond_to_cat = (products.withColumn("condition", F.explode("condition_tags"))
               .select("condition", "category").distinct())
member_cond_cat = (member_conditions.join(cond_to_cat, "condition")
                   .select("member_id", "category").distinct()
                   .withColumn("cond_boost", F.lit(8.0)))
cat_pop = (items.join(prod_cat, "product_id").groupBy("category")
           .agg(F.sum("quantity").alias("g_units")))
max_g = cat_pop.agg(F.max("g_units")).first()[0] or 1

category_recommendations = (
    members.select("member_id").crossJoin(all_categories)
    .join(cat_affinity, ["member_id", "category"], "left")
    .join(member_cond_cat, ["member_id", "category"], "left")
    .join(cat_pop, "category", "left")
    .fillna({"units": 0, "cond_boost": 0.0, "g_units": 0})
    .withColumn("score", F.col("units") + F.col("cond_boost") + 3.0 * F.col("g_units") / F.lit(max_g))
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("member_id").orderBy(F.desc("score"), F.asc("category"))))
    .withColumn("reason", F.when(F.col("units") > 0, F.lit("You shop here often"))
                .when(F.col("cond_boost") > 0, F.lit("Recommended for your health"))
                .otherwise(F.lit("Popular with members like you")))
    .select("member_id", "rank", "category", "reason", F.round("score", 2).alias("score"))
    .withColumn("generated_at", F.current_timestamp())
)
(category_recommendations
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("category_recommendations"))
print(f"category_recommendations: {spark.table('category_recommendations').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cold-start safety net
# MAGIC
# MAGIC Brand-new members have no purchase history, so collaborative filtering produces nothing
# MAGIC for them. We fill their "Recommended for You" section with the overall **best-sellers**
# MAGIC so the home page is never empty. (Their category + condition sections still work from
# MAGIC their profile.)

# COMMAND ----------

covered = cf_recs.select("member_id").distinct()
cold = members.select("member_id").join(covered, "member_id", "left_anti")

top_sellers = (
    spark.table("product_popularity").orderBy(F.desc("units_sold")).limit(TOP_N)
    .select("product_id", "product_name", "category", "price")
    .withColumn("seed_rank", F.row_number().over(Window.orderBy(F.lit(1))))
)
cold_recs = (
    cold.crossJoin(top_sellers)
    .select("member_id", "product_id", "product_name", "category", "price",
            F.col("seed_rank").alias("rank"),
            F.lit("Recommended for you").alias("module"),
            F.lit("Popular with members like you").alias("reason"))
)
print(f"cold-start members getting fallback recs: {cold.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assemble the final `recommendations` table
# MAGIC
# MAGIC Stack the four product sections into one tidy table — one row per
# MAGIC `(member_id, module, rank)`. This single table (plus `category_recommendations`) is the
# MAGIC **only thing the app reads**. Notebook `04` copies it into Lakebase for fast serving.

# COMMAND ----------

recommendations = (
    cf_recs
    .unionByName(cold_recs)
    .unionByName(buy_again)
    .unionByName(cond_recs)
    .withColumn("generated_at", F.current_timestamp())
)
(recommendations
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("recommendations"))
spark.sql("COMMENT ON TABLE recommendations IS "
          "'Per-member home-page product recommendations. One row per (member_id, module, rank).'")

print("recommendations table written.")
display(spark.sql("""
  SELECT module, COUNT(DISTINCT member_id) AS members, COUNT(*) AS rows
  FROM recommendations GROUP BY module ORDER BY module
"""))

# COMMAND ----------

# MAGIC %md ### Spot-check one member (all sections)

# COMMAND ----------

display(spark.sql(f"""
  SELECT m.first_name, m.age, m.health_conditions, r.module, r.rank,
         r.product_name, r.category, r.price, r.reason
  FROM recommendations r JOIN {SRC_MEMBERS} m USING (member_id)
  WHERE r.member_id = (SELECT member_id FROM recommendations
                       GROUP BY member_id HAVING COUNT(DISTINCT module) = 3 LIMIT 1)
  ORDER BY r.module, r.rank
"""))
