# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Synthetic Data Generation  *(OPTIONAL)*
# MAGIC
# MAGIC **You only need this notebook for the demo.** If you already have a gold layer,
# MAGIC set the job variable `generate_synthetic_data="false"` and this notebook skips
# MAGIC itself — point the pipeline at your own tables instead (see notebook `02`, which
# MAGIC documents the exact columns each input table must provide).
# MAGIC
# MAGIC When enabled, it generates a realistic, **SilverLeaf-Benefits-themed** dataset.
# MAGIC Everything is synthetic — no real members, no PHI.
# MAGIC
# MAGIC **Raw files landed** as newline-delimited JSON in the UC Volume
# MAGIC `{catalog}.{schema}.raw` (one folder per feed). This is the **raw dataset** — the
# MAGIC Lakeflow ingest pipeline picks these up with **Auto Loader** and refines them
# MAGIC bronze → silver (see `src/ingest_pipeline/`):
# MAGIC - `raw/members` — ~2,000 members, age-skewed elderly, health conditions, OTC benefit wallet
# MAGIC - `raw/products` — OTC / health-benefit catalog items, condition-tagged
# MAGIC - `raw/orders` + `raw/order_items` — purchase history with realistic RFM + condition-driven signal
# MAGIC - `raw/web_sessions` — browse/click behavior
# MAGIC
# MAGIC Signals are **planted** so recommendations are visibly sensible (diabetic members
# MAGIC buy glucose supplies, consumables get repeat-purchased → "Buy Again", etc.).
# MAGIC The generator is **seeded** (`SEED = 42`) so every run produces the same data.

# COMMAND ----------

# ---- CONFIGURATION (set as job parameters / bundle variables) ----
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "personalization", "Schema")
dbutils.widgets.dropdown("generate_synthetic_data", "true", ["true", "false"],
                         "Generate synthetic data?")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
GENERATE = dbutils.widgets.get("generate_synthetic_data").lower() == "true"

# COMMAND ----------

# If the customer is bringing their own gold data, skip generation entirely.
if not GENERATE:
    dbutils.notebook.exit(
        "generate_synthetic_data=false — skipping synthetic data generation. "
        "Configure the src_* variables in notebooks 02/03 to use your gold tables."
    )

# COMMAND ----------

import random
import datetime as dt
import pandas as pd

SEED = 42                       # fixed seed => reproducible synthetic data
random.seed(SEED)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"


def land(pdf, name):
    """Land a pandas DataFrame as newline-delimited JSON files in the raw Volume.

    This is the *raw dataset* the Lakeflow ingest pipeline picks up with Auto Loader
    (bronze → silver). Each feed overwrites its own `raw/<name>` folder, so re-running
    this notebook refreshes the raw data — re-run the ingest pipeline with a
    **full refresh** afterwards so Auto Loader reprocesses the regenerated files
    instead of appending them as new.
    """
    (spark.createDataFrame(pdf)
        .write.mode("overwrite")
        .json(f"{RAW}/{name}"))
    print(f"landed {len(pdf):>7,} rows -> {RAW}/{name}")


print(f"Landing synthetic raw files under {RAW}")

TODAY = dt.date(2026, 6, 1)     # "today" anchor for recency/tenure calculations

# COMMAND ----------

# MAGIC %md ## Catalog of products (OTC / health-benefit items)

# COMMAND ----------

# (category, subcategory, condition_tags, is_consumable, price_range)
CATALOG_SPEC = [
    # Diabetic Care
    ("Diabetic Care", "Glucose Test Strips", ["diabetes"], True, (18, 45)),
    ("Diabetic Care", "Glucose Tablets", ["diabetes"], True, (6, 14)),
    ("Diabetic Care", "Lancets", ["diabetes"], True, (5, 12)),
    ("Diabetic Care", "Diabetic Socks", ["diabetes"], False, (10, 22)),
    ("Diabetic Care", "Sugar-Free Snacks", ["diabetes"], True, (4, 11)),
    # Vitamins & Supplements
    ("Vitamins & Supplements", "Multivitamin", [], True, (8, 20)),
    ("Vitamins & Supplements", "Vitamin D3", ["osteoporosis"], True, (7, 16)),
    ("Vitamins & Supplements", "Calcium", ["osteoporosis"], True, (6, 15)),
    ("Vitamins & Supplements", "Fish Oil", ["heart_disease"], True, (9, 22)),
    ("Vitamins & Supplements", "CoQ10", ["heart_disease"], True, (12, 28)),
    # OTC Medications
    ("OTC Medications", "Pain Reliever", ["arthritis"], True, (5, 14)),
    ("OTC Medications", "Antacid", [], True, (4, 10)),
    ("OTC Medications", "Allergy Relief", [], True, (7, 18)),
    ("OTC Medications", "Cough & Cold", [], True, (6, 15)),
    ("OTC Medications", "Blood Pressure Monitor", ["hypertension"], False, (28, 65)),
    # Mobility & Daily Living
    ("Mobility & Daily Living", "Cane", ["mobility_impaired"], False, (18, 40)),
    ("Mobility & Daily Living", "Walker", ["mobility_impaired"], False, (45, 120)),
    ("Mobility & Daily Living", "Grab Bar", ["mobility_impaired"], False, (15, 35)),
    ("Mobility & Daily Living", "Compression Stockings", ["heart_disease"], False, (12, 30)),
    ("Mobility & Daily Living", "Pill Organizer", [], False, (6, 16)),
    # Personal Care
    ("Personal Care", "Incontinence Pads", [], True, (10, 25)),
    ("Personal Care", "Skin Moisturizer", [], True, (5, 14)),
    ("Personal Care", "Denture Care", [], True, (4, 11)),
    ("Personal Care", "Reading Glasses", ["vision_loss"], False, (8, 18)),
    # Vision & Hearing
    ("Vision & Hearing", "Hearing Aid Batteries", ["hearing_loss"], True, (6, 15)),
    ("Vision & Hearing", "Magnifier", ["vision_loss"], False, (10, 26)),
    ("Vision & Hearing", "Eye Drops", ["vision_loss"], True, (5, 13)),
    # Healthy Foods
    ("Healthy Foods", "Low-Sodium Meals", ["hypertension"], True, (5, 12)),
    ("Healthy Foods", "Protein Shakes", [], True, (8, 20)),
    ("Healthy Foods", "Whole Grain Snacks", ["diabetes"], True, (4, 10)),
    ("Healthy Foods", "Electrolyte Drinks", [], True, (6, 14)),
]

VENDORS = ["MediSupply Co", "HealthFirst Brands", "CareWell", "VitaLine", "DailyAid", "WellnessMart"]
BRANDS = ["Premier", "ValueCare", "GoldStandard", "EverWell", "PureLife", "ActiveSenior"]

products = []
pid = 1000
for (cat, sub, tags, consumable, (lo, hi)) in CATALOG_SPEC:
    # 2-4 branded variants per subcategory -> ~90 SKUs base; expand for variety
    for _ in range(random.randint(3, 5)):
        pid += 1
        brand = random.choice(BRANDS)
        price = round(random.uniform(lo, hi), 2)
        # popularity weight: consumables + common conditions sell more
        pop = random.uniform(0.5, 1.0) + (0.6 if consumable else 0.0)
        products.append({
            "product_id": f"P{pid}",
            "product_name": f"{brand} {sub}",
            "category": cat,
            "subcategory": sub,
            "vendor": random.choice(VENDORS),
            "price": price,
            "condition_tags": tags,
            "is_consumable": consumable,
            "popularity_weight": round(pop, 3),
        })

products_pdf = pd.DataFrame(products)
land(products_pdf, "products")
display(spark.read.json(f"{RAW}/products").limit(10))

# COMMAND ----------

# MAGIC %md ## Members (~2,000) — elderly skew, health conditions, OTC wallet

# COMMAND ----------

CONDITIONS = ["diabetes", "hypertension", "arthritis", "heart_disease",
              "osteoporosis", "vision_loss", "hearing_loss", "mobility_impaired"]
COND_PREVALENCE = [0.30, 0.45, 0.35, 0.20, 0.18, 0.22, 0.20, 0.15]
PLANS = ["Medicare Advantage HMO", "Medicare Advantage PPO", "D-SNP", "C-SNP"]
STATES = ["FL", "TX", "CA", "NY", "PA", "OH", "AZ", "NC", "GA", "MI"]
FIRST = ["Robert", "Mary", "James", "Patricia", "John", "Jennifer", "Michael", "Linda",
         "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Margaret",
         "Thomas", "Dorothy", "Charles", "Helen", "Frank", "Betty", "George", "Ruth"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]

N_MEMBERS = 2000
members = []
for i in range(N_MEMBERS):
    mid = f"M{100000 + i}"
    age = int(min(95, max(50, random.gauss(72, 9))))
    conds = [c for c, p in zip(CONDITIONS, COND_PREVALENCE) if random.random() < p]
    # enrollment 1 month - 4 years ago
    enroll = TODAY - dt.timedelta(days=random.randint(30, 1460))
    members.append({
        "member_id": mid,
        "first_name": random.choice(FIRST),
        "last_name": random.choice(LAST),
        "age": age,
        "gender": random.choice(["F", "M"]),
        "state": random.choice(STATES),
        "plan_type": random.choice(PLANS),
        "enrollment_date": enroll,
        "health_conditions": conds,
        "otc_wallet_monthly": random.choice([25, 50, 75, 100, 150]),
    })

members_pdf = pd.DataFrame(members)
land(members_pdf, "members")
display(spark.read.json(f"{RAW}/members").limit(10))

# COMMAND ----------

# MAGIC %md ## Orders & order items — condition-driven, with repeat-purchase (Buy Again) signal

# COMMAND ----------

prod_list = products_pdf.to_dict("records")
# index products by condition tag for signal injection
by_condition = {}
for p in prod_list:
    for t in p["condition_tags"]:
        by_condition.setdefault(t, []).append(p)
consumables = [p for p in prod_list if p["is_consumable"]]
all_products = prod_list

def weighted_choice(pool):
    weights = [p["popularity_weight"] for p in pool]
    return random.choices(pool, weights=weights, k=1)[0]

orders = []
order_items = []
oid = 500000
# ~15% of members are cold-start (no orders) to exercise the fallback
for m in members:
    if random.random() < 0.15:
        continue
    # activity tier -> number of orders over the last ~12 months
    tier = random.choices(["heavy", "regular", "light"], weights=[0.2, 0.5, 0.3])[0]
    n_orders = {"heavy": random.randint(10, 24), "regular": random.randint(4, 9), "light": random.randint(1, 3)}[tier]
    # each member has a small set of "staple" consumables they reorder
    staples = random.sample(consumables, k=random.randint(1, 3))
    # condition-relevant products this member gravitates to
    cond_pool = []
    for c in m["health_conditions"]:
        cond_pool += by_condition.get(c, [])
    for _ in range(n_orders):
        oid += 1
        days_ago = random.randint(1, 365)
        order_date = TODAY - dt.timedelta(days=days_ago)
        orders.append({
            "order_id": f"O{oid}",
            "member_id": m["member_id"],
            "order_date": order_date,
            "channel": random.choice(["web", "web", "web", "mobile", "phone"]),
        })
        n_items = random.randint(1, 4)
        chosen = set()
        for _ in range(n_items):
            r = random.random()
            if r < 0.45 and staples:           # reorder a staple -> Buy Again signal
                p = random.choice(staples)
            elif r < 0.75 and cond_pool:        # condition-driven purchase
                p = weighted_choice(cond_pool)
            else:                               # general popularity
                p = weighted_choice(all_products)
            if p["product_id"] in chosen:
                continue
            chosen.add(p["product_id"])
            qty = random.randint(1, 3)
            order_items.append({
                "order_id": f"O{oid}",
                "member_id": m["member_id"],
                "product_id": p["product_id"],
                "quantity": qty,
                "unit_price": p["price"],
                "order_date": order_date,
            })

orders_pdf = pd.DataFrame(orders)
items_pdf = pd.DataFrame(order_items)
land(orders_pdf, "orders")
land(items_pdf, "order_items")

# COMMAND ----------

# MAGIC %md ## Web sessions — browse/click behavior (lighter signal)

# COMMAND ----------

sessions = []
sid = 9000000
members_with_activity = items_pdf["member_id"].unique().tolist()
for mid in members_with_activity:
    for _ in range(random.randint(1, 6)):
        sid += 1
        days_ago = random.randint(0, 120)
        ts = dt.datetime.combine(TODAY - dt.timedelta(days=days_ago),
                                 dt.time(random.randint(7, 21), random.randint(0, 59)))
        cat = random.choice([p["category"] for p in all_products])
        sessions.append({
            "session_id": f"S{sid}",
            "member_id": mid,
            "session_ts": ts,
            "category_viewed": cat,
            "page_views": random.randint(1, 12),
            "device": random.choice(["desktop", "mobile", "tablet"]),
        })

sessions_pdf = pd.DataFrame(sessions)
land(sessions_pdf, "web_sessions")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print("Raw files landed in the Volume:")
for e in ["members", "products", "orders", "order_items", "web_sessions"]:
    print(f"  {e:15s} {spark.read.json(f'{RAW}/{e}').count():>8,} rows  ({RAW}/{e})")
print("\nRaw data generation complete. Next: the Lakeflow ingest pipeline "
      "(bronze -> silver) picks these up with Auto Loader.")
