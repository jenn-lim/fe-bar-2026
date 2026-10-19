"""
SilverLeaf Benefits — Personalized Member Home Page (demo)

A member benefits-store home page with personalization layered in. Reads the
batch-generated `recommendations` table and renders the member's home page:
Recommended for you, Buy Again, and Based on your conditions, plus the member's
benefit-wallet balances. Nothing is computed live — the app only looks up the
precomputed serving table, exactly like the production design.
"""
import os
import time
import streamlit as st
import pandas as pd
import psycopg
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="SilverLeaf Benefits · My Benefits Store",
                   page_icon="🍃", layout="wide", initial_sidebar_state="expanded")

# Served from Lakebase Autoscaling (managed Postgres) — millisecond key-value lookups.
# Tables are managed synced tables (reverse ETL), in the Postgres schema named after the UC schema.
LAKEBASE_ENDPOINT = os.getenv("LAKEBASE_ENDPOINT",
    "projects/YOUR-LAKEBASE-PROJECT/branches/production/endpoints/primary")
PG_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "personalization")

MODULE_META = {
    "Recommended for you": ("Recommended for You", "✨"),
    "Buy Again": ("Buy It Again", "🔁"),
    "Based on your conditions": ("Picked for Your Health", "🩺"),
}
MODULE_ORDER = ["Recommended for you", "Buy Again", "Based on your conditions"]
CATEGORY_STYLE = {
    "Diabetic Care": ("🩸", "#e8f0fe"), "Vitamins & Supplements": ("💊", "#e9f7ef"),
    "OTC Medications": ("🌡️", "#fdecea"), "Mobility & Daily Living": ("🦯", "#f3eafe"),
    "Personal Care": ("🧴", "#fff4e5"), "Vision & Hearing": ("👓", "#e7f5fb"),
    "Healthy Foods": ("🥗", "#eef7e6"),
}

# ---------- data access (Lakebase Autoscaling / Postgres) ----------
@st.cache_resource
def _conn():
    # The app's service principal authenticates to Lakebase via OAuth. Its Postgres
    # role == the SP application id (DATABRICKS_CLIENT_ID), matching the role created
    # for it on the branch. The OAuth token is only needed at connect time.
    w = WorkspaceClient()
    host = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host
    user = os.environ["DATABRICKS_CLIENT_ID"]
    token = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT).token
    return psycopg.connect(
        f"host={host} dbname=databricks_postgres user={user} "
        f"password={token} sslmode=require",
        autocommit=True,
    )

@st.cache_data(ttl=300)
def query(q: str, params: tuple = ()) -> pd.DataFrame:
    try:
        conn = _conn()
        with conn.cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except psycopg.OperationalError:
        # token expired or connection dropped after scale-to-zero — rebuild and retry
        _conn.clear()
        with _conn().cursor() as cur:
            cur.execute(q, params)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)

@st.cache_data(ttl=300)
def load_members() -> pd.DataFrame:
    return query(f"""
        SELECT member_id, first_name, last_name, age, state, plan_type,
               otc_wallet_monthly, conditions, welcome_message, message_source
        FROM {PG_SCHEMA}.members_online ORDER BY last_name, first_name
    """)

@st.cache_data(ttl=300)
def load_recs(member_id: str) -> pd.DataFrame:
    t0 = time.perf_counter()
    df = query(f"""
        SELECT module, rank, product_name, category, price, reason
        FROM {PG_SCHEMA}.recommendations_online WHERE member_id = %s ORDER BY module, rank
    """, (member_id,))
    # One structured line per home-page render — `databricks apps logs` is the audit trail.
    print(f"[lakebase] member={member_id} recs={len(df)} "
          f"modules={sorted(df.module.unique().tolist())} "
          f"latency_ms={(time.perf_counter() - t0) * 1000:.1f}", flush=True)
    return df

@st.cache_data(ttl=300)
def load_categories(member_id: str) -> pd.DataFrame:
    return query(f"""
        SELECT rank, category, reason
        FROM {PG_SCHEMA}.category_recommendations_online WHERE member_id = %s ORDER BY rank
    """, (member_id,))

# ---------- Genie-powered product finder (catalog-scoped) ----------
# The app's service principal calls a Genie space (CAN_RUN) scoped to the product
# catalog view ONLY — so it can find products but cannot reach member/spend/account
# data (those tables aren't in the space). This mirrors a smart portal search box.
# Notebook 05 creates the space; set GENIE_SPACE_ID to pin it, else it's found by title.
GENIE_SPACE_TITLE = "SilverLeaf Product Finder"

@st.cache_resource
def genie_space_id() -> str:
    pinned = os.getenv("GENIE_SPACE_ID", "").strip()
    if pinned:
        return pinned
    spaces = WorkspaceClient().genie.list_spaces().spaces or []
    return next(s.space_id for s in spaces if s.title == GENIE_SPACE_TITLE)

@st.cache_data(ttl=300, show_spinner=False)
def genie_search(question: str):
    """Ask the catalog-scoped Genie space; return (answer_text, sql, result_df)."""
    w = WorkspaceClient()
    msg = w.genie.start_conversation_and_wait(genie_space_id(), question)
    answer, sql, df = None, None, None
    for att in (getattr(msg, "attachments", None) or []):
        if getattr(att, "text", None) and att.text and att.text.content:
            answer = att.text.content
        if getattr(att, "query", None) and att.query:
            sql = att.query.query
            try:
                res = w.genie.get_message_attachment_query_result(
                    msg.space_id, msg.conversation_id, msg.id, att.attachment_id)
                sr = res.statement_response
                cols = [c.name for c in sr.manifest.schema.columns]
                df = pd.DataFrame(sr.result.data_array or [], columns=cols)
            except Exception:
                df = None
    print(f"[genie] q={question!r} sql={'yes' if sql else 'no'} "
          f"rows={0 if df is None else len(df)} answer={(answer or '')[:120]!r}", flush=True)
    return answer, sql, df


def render_product_results(df: pd.DataFrame):
    """Render Genie's product rows as cards when possible, else a table."""
    lc = {c.lower(): c for c in df.columns}
    name_c, cat_c, price_c = lc.get("product_name"), lc.get("category"), lc.get("price")
    if not name_c:
        st.dataframe(df, use_container_width=True, hide_index=True)
        return
    rows = df.head(10)
    cards = st.columns(min(5, max(1, len(rows))))
    for i, (_, r) in enumerate(rows.iterrows()):
        cat = str(r[cat_c]) if cat_c else ""
        emoji, bg = CATEGORY_STYLE.get(cat, ("📦", "#eef1f6"))
        try:
            price = f"${float(r[price_c]):.2f}" if price_c else ""
        except (TypeError, ValueError):
            price = str(r[price_c]) if price_c else ""
        cards[i % len(cards)].markdown(f"""
        <div class="pcard">
          <div class="img" style="background:{bg};">{emoji}</div>
          <div class="body">
            <div class="cat">{cat}</div>
            <div class="name">{r[name_c]}</div>
            <div class="reason"></div>
            <div class="foot"><span class="price">{price}</span>
                 <span class="add">Add to cart</span></div>
          </div>
        </div>""", unsafe_allow_html=True)

# ---------- SilverLeaf-themed styling ----------
st.markdown("""
<style>
:root{
  --sl-green:#1f7a4d; --sl-green2:#155e3b; --sl-leaf:#2f9e6a;
  --sl-gold:#e0a300; --sl-orange:#e8672e; --sl-mint:#19a05a; --ink:#1f2937;
}
#MainMenu, header[data-testid="stHeader"], footer {visibility:hidden; height:0;}
.block-container {padding:0 2.5rem 2rem 2.5rem; max-width:1500px;}
/* top white header */
.sl-top{display:flex; align-items:center; gap:22px; padding:14px 4px 12px 4px;}
.sl-logo{font-size:22px; font-weight:800; color:#5b6470;}
.sl-logo b{background:var(--sl-green); color:#fff; padding:3px 10px; border-radius:7px; margin-right:2px;}
.sl-search{flex:1; display:flex;}
.sl-search input{flex:1; border:1px solid #d6dbe3; border-right:none;
  border-radius:24px 0 0 24px; padding:10px 18px; font-size:14px; color:#888;}
.sl-search .btn{background:var(--sl-gold); color:#1f2937; font-weight:700;
  border-radius:0 24px 24px 0; padding:10px 22px; font-size:14px;}
.sl-pill{background:#eafaf0; color:var(--sl-mint); font-weight:700; border-radius:18px;
  padding:5px 12px; font-size:13px;}
.sl-acct{color:#3b455a; font-size:13px; line-height:1.2;}
.sl-cart{color:var(--sl-green); font-weight:700;}
/* green nav */
.sl-nav{background:var(--sl-green); border-radius:8px; padding:11px 20px; display:flex;
  gap:26px; align-items:center; margin-bottom:18px;}
.sl-nav a{color:#d7efe1; font-size:14px; font-weight:600; text-decoration:none;}
.sl-nav a.active{color:#fff; border-bottom:3px solid var(--sl-gold); padding-bottom:7px;}
/* hero */
.sl-hero{font-size:24px; font-weight:800; color:var(--ink); margin:6px 0 2px 0;}
.sl-hero .accent{color:var(--sl-green2);}
.sl-sub{color:#6b7280; font-size:14px; margin-bottom:8px;}
.sl-secthead{display:flex; justify-content:space-between; align-items:baseline; margin:18px 0 10px;}
.sl-secthead h3{font-size:19px; font-weight:800; color:var(--ink); margin:0;}
.sl-secthead .see{color:var(--sl-green2); font-size:13px; font-weight:700;}
.sl-banner{background:#f2faf5; border:1px solid #cfe9da; border-left:5px solid var(--sl-leaf);
  border-radius:10px; padding:12px 16px; font-size:15px; color:var(--ink); margin:8px 0 6px;}
/* product card */
.pcard{border:1px solid #e8ebf0; border-radius:14px; overflow:hidden; background:#fff;
  box-shadow:0 1px 4px rgba(20,60,40,.06); height:230px; display:flex; flex-direction:column;}
.pcard .img{height:92px; display:flex; align-items:center; justify-content:center; font-size:40px;}
.pcard .body{padding:11px 13px; display:flex; flex-direction:column; flex:1;}
.pcard .cat{font-size:11px; color:var(--sl-green2); font-weight:700; text-transform:uppercase; letter-spacing:.3px;}
.pcard .name{font-size:14px; font-weight:700; color:var(--ink); margin:4px 0; line-height:1.25;}
.pcard .reason{font-size:11px; color:#9aa1ad; margin-top:auto;}
.pcard .foot{display:flex; justify-content:space-between; align-items:center; margin-top:6px;}
.pcard .price{font-size:17px; font-weight:800; color:var(--sl-mint);}
.pcard .add{background:var(--sl-gold); color:#1f2937; font-weight:700; font-size:11px;
  border-radius:14px; padding:5px 11px;}
/* category tile (personalized carousel) */
.ctile{border:1px solid #e8ebf0; border-radius:14px; overflow:hidden; background:#fff;
  box-shadow:0 1px 4px rgba(20,60,40,.06); text-align:center;}
.ctile .top{height:74px; display:flex; align-items:center; justify-content:center; font-size:34px;}
.ctile .lab{font-size:13px; font-weight:700; color:var(--ink); padding:8px 6px 3px;}
.ctile .why{font-size:10px; color:#9aa1ad; padding:0 6px 9px;}
/* benefits band */
.sl-band{background:linear-gradient(120deg,var(--sl-green) 0%,var(--sl-green2) 100%);
  border-radius:14px; padding:20px 24px; margin-top:26px; color:#fff;}
.sl-band h3{margin:0; font-size:20px; font-weight:800;}
.sl-band p{margin:2px 0 14px; color:#c9e7d7; font-size:13px;}
.bcard{background:#fff; border-radius:12px; padding:14px 16px;}
.bcard .t{color:var(--sl-green); font-weight:800; font-size:15px;}
.bcard .bal{color:#111; font-weight:800; font-size:20px; margin-top:6px;}
.bcard .l{color:#8a93a3; font-size:11px;}
</style>
""", unsafe_allow_html=True)

# ---------- sidebar: demo control (member picker) ----------
members = load_members()
st.sidebar.markdown("### 🧪 Demo control")
labels = {f"{r.first_name} {r.last_name} · {r.member_id}": r.member_id
          for r in members.itertuples()}
choice = st.sidebar.selectbox("View the home page as:", list(labels.keys()))
mid = labels[choice]
m = members[members.member_id == mid].iloc[0]
st.sidebar.markdown(f"**Age:** {m.age}  ·  **{m.state}**  \n**Plan:** {m.plan_type}")
if m.conditions:
    st.sidebar.markdown("**Conditions:** " + " ".join(f"`{c}`" for c in m.conditions.split(", ")))
st.sidebar.caption("Recommendations are precomputed in batch and read from the "
                   "`recommendations` table by member_id. Nothing runs in real time — "
                   "and all data is synthetic (no PHI).")

# ---------- header ----------
otc = float(m.otc_wallet_monthly)
total_bal = otc + 75 + 500 + 60  # OTC + Food + Hearing + Dental (mock parity)
st.markdown(f"""
<div class="sl-top">
  <div class="sl-logo">🍃 <b>Silver</b>Leaf</div>
  <div class="sl-search">
    <input value="Search for OTC, Grocery, and more" disabled>
    <span class="btn">🔍 Search</span>
  </div>
  <span class="sl-pill">💳 Balances ${total_bal:,.0f}</span>
  <span class="sl-acct">Hi, {m.first_name}<br>Account ⌄</span>
  <span class="sl-cart">🛒 Cart</span>
</div>
<div class="sl-nav">
  <a class="active">Home</a><a>Shop ⌄</a><a>Grocery Delivery</a><a>My Card</a>
  <a>My Benefits ⌄</a><a>Fresh Meals</a><a>My Nutrition</a><a>Rewards</a><a>Health Assistant</a>
</div>
<div class="sl-hero">Welcome back, <span class="accent">{m.first_name}</span> 👋</div>
<div class="sl-sub">Your benefits store, personalized to your health and purchase history.</div>
""", unsafe_allow_html=True)

# GenAI welcome banner — generated nightly by ai_query (notebook 03c) and guardrail-checked;
# members whose message failed a check get the safe template instead.
if isinstance(m.welcome_message, str) and m.welcome_message:
    st.markdown(f'<div class="sl-banner">✨ {m.welcome_message}</div>', unsafe_allow_html=True)

# ---------- Ask SilverLeaf (Genie-powered product finder) ----------
st.markdown('<div class="sl-secthead"><h3>🔎 Ask SilverLeaf</h3>'
            '<span class="see">AI product finder</span></div>', unsafe_allow_html=True)
gq = st.text_input("Ask for products", key="genie_q", label_visibility="collapsed",
                   placeholder="Ask for products — e.g. “what do you have for diabetes under $20?”")
if gq:
    with st.spinner("Finding products…"):
        try:
            answer, sql, gdf = genie_search(gq)
        except Exception as e:
            answer, sql, gdf = f"Sorry, I couldn't search right now ({e}).", None, None
    if answer:
        st.markdown(answer)
    if gdf is not None and not gdf.empty:
        render_product_results(gdf)
    if sql:
        with st.expander("🔍 See the query Genie ran"):
            st.code(sql, language="sql")
    st.write("")

# ---------- personalized category carousel ----------
cats = load_categories(mid)
if not cats.empty:
    st.markdown("""<div class="sl-secthead"><h3>🛍️ Shop Your Categories</h3>
                <span class="see">Ordered for you ›</span></div>""", unsafe_allow_html=True)
    ncat = min(6, len(cats))
    ccols = st.columns(ncat)
    for col, r in zip(ccols, cats.head(ncat).itertuples()):
        emoji, bg = CATEGORY_STYLE.get(r.category, ("📦", "#eef1f6"))
        col.markdown(f"""
        <div class="ctile"><div class="top" style="background:{bg};">{emoji}</div>
          <div class="lab">{r.category}</div><div class="why">{r.reason}</div></div>""",
                     unsafe_allow_html=True)
    st.write("")

# ---------- personalized product shelves ----------
recs = load_recs(mid)

def render_shelf(mod_key: str, n_cols: int = 5):
    title, icon = MODULE_META[mod_key]
    sub = recs[recs.module == mod_key].sort_values("rank").head(n_cols)
    if sub.empty:
        return
    st.markdown(f"""<div class="sl-secthead"><h3>{icon} {title}</h3>
                <span class="see">See all ›</span></div>""", unsafe_allow_html=True)
    cols = st.columns(n_cols)
    for col, r in zip(cols, sub.itertuples()):
        emoji, bg = CATEGORY_STYLE.get(r.category, ("📦", "#eef1f6"))
        col.markdown(f"""
        <div class="pcard">
          <div class="img" style="background:{bg};">{emoji}</div>
          <div class="body">
            <div class="cat">{r.category}</div>
            <div class="name">{r.product_name}</div>
            <div class="reason">{r.reason}</div>
            <div class="foot"><span class="price">${r.price:.2f}</span>
                 <span class="add">Add to cart</span></div>
          </div>
        </div>""", unsafe_allow_html=True)

for mod in MODULE_ORDER:
    render_shelf(mod)

# ---------- benefits balance band ----------
st.markdown(f"""
<div class="sl-band">
  <h3>Explore Your Health Benefits</h3>
  <p>Discover and maximize all your plan benefits</p>
</div>""", unsafe_allow_html=True)
b1, b2, b3, b4 = st.columns(4)
for col, (title, bal, label) in zip(
    [b1, b2, b3, b4],
    [("OTC & Wellness", f"${otc:,.0f}", "Monthly allowance"),
     ("Food & Grocery", "$75.00", "Remaining balance"),
     ("Hearing", "$500.00", "Per ear"),
     ("Dental", "Exam + Services", "Covered")]):
    col.markdown(f"""<div class="bcard"><div class="t">{title} ›</div>
                 <div class="bal">{bal}</div><div class="l">{label}</div></div>""",
                 unsafe_allow_html=True)

with st.expander("ℹ️ How this works (for the Databricks team)"):
    st.markdown("""
    This page is rendered entirely from **precomputed** tables in Lakebase:

    - **🛍️ Shop Your Categories** — categories ranked per member by purchase affinity +
      health-condition relevance (a personalized "Shop Popular Categories" carousel)
    - **✨ Recommended for You** — item-to-item collaborative filtering
      ("members like you also bought"), with a popularity fallback for cold-start members
    - **🔁 Buy It Again** — consumables the member has reordered (RFM-driven)
    - **🩺 Picked for Your Health** — products matched to the member's health conditions
    - **✨ Welcome banner** — written nightly by a foundation model via `ai_query()`, then
      guardrail-checked (no medical claims, no invented products or prices)
    - **🔎 Ask SilverLeaf** — a Genie Agent scoped to the product catalog only

    Both **categories and items** are personalized — the two surfaces the business asked for.

    A scheduled batch job refreshes the table; the app only does a key-value lookup by
    `member_id`. No real-time inference — the whole loop stays inside Unity Catalog.
    All data is synthetic; no real members and no PHI.
    """)
