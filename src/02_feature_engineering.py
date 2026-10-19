# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Feature Engineering
# MAGIC
# MAGIC ### What this notebook does (plain English)
# MAGIC A recommendation model can't read raw orders and web clicks directly — it needs
# MAGIC each member summarized as a tidy set of **numbers**. That summarizing step is called
# MAGIC **feature engineering**. Think of it as building each member's "stat line" from their
# MAGIC history: how recently they shopped, how often, how much, what they bought, and which
# MAGIC health categories matter to them.
# MAGIC
# MAGIC This notebook reads your source tables and produces a few clean tables that the next
# MAGIC notebook (`03`) turns into recommendations:
# MAGIC
# MAGIC | Output table | What it is |
# MAGIC |---|---|
# MAGIC | `member_features` | One row per member: **RFM** + profile + a "new member?" flag |
# MAGIC | `member_product_interactions` | Who-bought-what, used to find members with similar taste |
# MAGIC | `product_popularity` | Overall best-sellers (used for brand-new members) |
# MAGIC | `product_popularity_by_condition` | Best-sellers for each health condition |
# MAGIC
# MAGIC Nothing here is a "black box" — every column is a simple count, sum, or date math
# MAGIC that you can read straight from the code.
# MAGIC
# MAGIC ---
# MAGIC ### INPUT CONTRACT — what your gold tables must provide
# MAGIC To use your **own** data (instead of the synthetic data from notebook `01`), point
# MAGIC the `src_*` variables at your tables. Each must contain at least these columns:
# MAGIC
# MAGIC | Source | Required columns |
# MAGIC |---|---|
# MAGIC | **members** | `member_id`, `enrollment_date` (date), `health_conditions` (array<string>), `age`, `state`, `plan_type`, `otc_wallet_monthly`, `first_name`, `last_name` |
# MAGIC | **products** | `product_id`, `product_name`, `category`, `price`, `condition_tags` (array<string>), `is_consumable` (bool), `popularity_weight` (double, optional) |
# MAGIC | **orders** | `order_id`, `member_id`, `order_date` (date) |
# MAGIC | **order_items** | `order_id`, `member_id`, `product_id`, `quantity` (int), `unit_price` (double), `order_date` (date) |
# MAGIC | **web_sessions** | `member_id`, `category_viewed`, `page_views` (int) |
# MAGIC
# MAGIC If your column names differ, the simplest approach is to create a **view** that renames
# MAGIC them to match, then point the `src_*` variable at that view.

# COMMAND ----------

# ---- CONFIGURATION ----
dbutils.widgets.text("catalog", "main", "Catalog (for outputs)")
dbutils.widgets.text("schema", "personalization", "Schema (for outputs)")
# Leave these blank to use the tables in {catalog}.{schema}; set to a full
# "catalog.schema.table" name to use your own gold tables.
dbutils.widgets.text("src_members", "", "Source: members")
dbutils.widgets.text("src_products", "", "Source: products")
dbutils.widgets.text("src_orders", "", "Source: orders")
dbutils.widgets.text("src_order_items", "", "Source: order_items")
dbutils.widgets.text("src_sessions", "", "Source: web_sessions")
# As-of date for recency/tenure math. Blank => today's date (correct for production);
# set a fixed YYYY-MM-DD only for reproducible runs against a frozen snapshot.
dbutils.widgets.text("ref_date", "", "As-of date YYYY-MM-DD (blank = today)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Reference date column used throughout: a fixed date if provided, else current_date().
_ref = dbutils.widgets.get("ref_date").strip()
REF_COL = F.to_date(F.lit(_ref)) if _ref else F.current_date()


def source(entity: str, override: str) -> str:
    """Resolve a source table name.

    Returns the override (a full ``catalog.schema.table`` name) if provided,
    otherwise the default table ``{CATALOG}.{SCHEMA}.{entity}`` created by
    notebook 01. This is what makes the pipeline "bring your own data".
    """
    override = (override or "").strip()
    return override if override else f"{CATALOG}.{SCHEMA}.{entity}"


SRC_MEMBERS = source("members", dbutils.widgets.get("src_members"))
SRC_PRODUCTS = source("products", dbutils.widgets.get("src_products"))
SRC_ORDERS = source("orders", dbutils.widgets.get("src_orders"))
SRC_ORDER_ITEMS = source("order_items", dbutils.widgets.get("src_order_items"))
SRC_SESSIONS = source("web_sessions", dbutils.widgets.get("src_sessions"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")   # outputs are written here
print("Reading from:")
for t in [SRC_MEMBERS, SRC_PRODUCTS, SRC_ORDERS, SRC_ORDER_ITEMS, SRC_SESSIONS]:
    print(f"  - {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member features: RFM + profile
# MAGIC
# MAGIC **RFM** is a classic, easy-to-explain way to summarize a customer:
# MAGIC - **Recency** — how many days since their last order (lower = more engaged)
# MAGIC - **Frequency** — how many orders they've placed
# MAGIC - **Monetary** — how much they've spent
# MAGIC
# MAGIC We also carry the member's profile (age, plan, conditions, OTC wallet) and a
# MAGIC **`is_cold_start`** flag that is `true` for members with no purchase history — those
# MAGIC members can't be personalized from past behavior, so notebook `03` gives them a
# MAGIC sensible popularity-based home page instead.

# COMMAND ----------

orders = spark.table(SRC_ORDERS)
items = spark.table(SRC_ORDER_ITEMS)
members = spark.table(SRC_MEMBERS)
sessions = spark.table(SRC_SESSIONS)

# Recency (days since last order) + Frequency (number of orders)
rfm = (
    orders.groupBy("member_id")
    .agg(
        F.datediff(REF_COL, F.max("order_date")).alias("recency_days"),
        F.countDistinct("order_id").alias("frequency_orders"),
    )
)

# Monetary (total spend) + breadth of products bought
monetary = (
    items.withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    .groupBy("member_id")
    .agg(
        F.round(F.sum("line_total"), 2).alias("monetary_total"),
        F.countDistinct("product_id").alias("distinct_products"),
        F.sum("quantity").alias("total_units"),
    )
)

# Behavioral signal: the category the member browses most (from web sessions)
top_browsed = (
    sessions.groupBy("member_id", "category_viewed")
    .agg(F.sum("page_views").alias("views"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("member_id").orderBy(F.desc("views"))))
    .filter(F.col("rn") == 1)
    .select("member_id", F.col("category_viewed").alias("top_browsed_category"))
)

member_features = (
    members
    .withColumn("tenure_days", F.datediff(REF_COL, F.col("enrollment_date")))
    .join(rfm, "member_id", "left")
    .join(monetary, "member_id", "left")
    .join(top_browsed, "member_id", "left")
    # members with no orders get zeros and a very high recency
    .fillna({"recency_days": 9999, "frequency_orders": 0, "monetary_total": 0.0,
             "distinct_products": 0, "total_units": 0})
    .withColumn("is_cold_start", F.col("frequency_orders") == 0)
)
member_features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("member_features")
n_cold = spark.table("member_features").filter("is_cold_start").count()
print(f"member_features: {spark.table('member_features').count():,} rows "
      f"({n_cold:,} are new / cold-start members)")
display(spark.table("member_features").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member × product interactions
# MAGIC
# MAGIC This is the table that powers "members like you also bought…". Each row says *how
# MAGIC strongly* a member is associated with a product. We don't have star ratings, so we
# MAGIC infer a **strength score** from behavior (this is called *implicit feedback*):
# MAGIC
# MAGIC ```
# MAGIC rating = (# times purchased) + log(1 + total quantity) + recency weight
# MAGIC ```
# MAGIC
# MAGIC - buying something **more often** or in **larger quantity** → higher score
# MAGIC - **recent** purchases count a bit more than old ones
# MAGIC
# MAGIC The exact formula isn't sacred — it just needs to rank "how much does this member
# MAGIC like this product" sensibly. Notebook `03` uses it to find look-alike buying patterns.

# COMMAND ----------

interactions = (
    items
    # recency weight: 1.0 for a purchase today, fading toward 0.5 over ~2 years
    .withColumn("recency_w", F.greatest(F.lit(0.5),
                F.lit(1.0) - F.datediff(REF_COL, F.col("order_date")) / F.lit(730.0)))
    .groupBy("member_id", "product_id")
    .agg(
        F.count("*").alias("purchase_events"),
        F.sum("quantity").alias("total_qty"),
        F.round(F.sum("recency_w"), 3).alias("weighted_score"),
    )
    .withColumn("rating", F.col("purchase_events") + F.log1p("total_qty") + F.col("weighted_score"))
)
interactions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("member_product_interactions")
print(f"member_product_interactions: {spark.table('member_product_interactions').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product popularity (the safety net for new members)
# MAGIC
# MAGIC A brand-new member has no history to learn from. For them we fall back to **what's
# MAGIC popular** — overall, and specifically among members with the same health condition.
# MAGIC These two tables are simple "units sold" counts.

# COMMAND ----------

products = spark.table(SRC_PRODUCTS)

# Overall best-sellers
global_pop = (
    items.groupBy("product_id").agg(F.sum("quantity").alias("units_sold"))
    .join(products, "product_id")
)
global_pop.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("product_popularity")

# Best-sellers broken out by the health condition each product is tagged for
cond_pop = (
    global_pop
    .withColumn("condition", F.explode_outer("condition_tags"))
    .filter(F.col("condition").isNotNull())
    .select("condition", "product_id", "product_name", "category", "price",
            "units_sold", "popularity_weight")
)
cond_pop.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("product_popularity_by_condition")

print("Feature engineering complete. Output tables:")
for t in ["member_features", "member_product_interactions", "product_popularity",
          "product_popularity_by_condition"]:
    print(f"  {t:35s} {spark.table(t).count():>8,} rows")
