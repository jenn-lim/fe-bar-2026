# SilverLeaf Benefits — Demo Talk Track

**Scenario:** SilverLeaf Benefits administers the supplemental OTC benefit for Medicare Advantage
health plans. Members get a monthly allowance ($25–$150) to spend in an online store. Today every
member sees the same best-seller home page.

**Audiences**

| Role | Cares about | Lead with |
|---|---|---|
| **Executive sponsor** — VP Member Experience / Chief Growth Officer | Benefit utilization, member satisfaction and retention, Star ratings, cost to serve | The outcome and the proof it works |
| **Domain owner** — Director of Benefits Product / Store merchandising | What members see, control over the experience, compliance in a Medicare population | The member page and the guardrails |
| **Technical stakeholder** — Director of Data Engineering / Platform architect | Integration, governance, operability, cost, lock-in | One platform, one job, governed end to end |

**Length:** 20 minutes (8 business, 10 technical, 2 close). All data synthetic.

---

## 1 · Business open (3 min)

> "Every member of your store sees the same home page today. A diabetic member on a $50 allowance
> scrolls past walkers and hearing-aid batteries to find test strips — or doesn't, and the allowance
> goes unspent. That's a benefit your plans are paying for that members never feel.
>
> We built a home page that's different for every member: ordered for their health conditions,
> their purchase history and their allowance — and we tested it against what you show today
> before anyone saw it."

**The proof (slide: "Does it beat today's page?")**

> "We hid the last 60 days of purchases and asked each approach to predict what members would
> discover next. Today's best-seller page got a product right for **31%** of members. The
> personalized page gets it right for **54%** — about **three-quarters more** members see something
> they go on to buy, and the right product shows up much nearer the top of the shelf."

*If asked "is that real data?"* — "It's synthetic data with realistic purchase patterns, so treat the
absolute numbers as directional. The method is the point: it's the same holdout test we'd run on
your order history in week one of a pilot, before any member sees a change."

## 2 · The member page (5 min) — *domain owner*

Open the app. Pick a member with diabetes and a $50 allowance.

1. **Welcome banner** — "Written for this member overnight by a foundation model. It knows their
   name, their allowance and their picks."
   - **Guardrails:** "In a Medicare population you can't have a model saying a product *treats*
     anything. Every banner passes five checks: no medical claims, no health condition named on
     screen, no invented products or prices, and a length limit. If any check fails, the member gets
     a plain template instead. We also run planted bad messages through the checks on every run —
     if one gets through, the job stops."
2. **Shop Your Categories** — "Category order is personal too — Diabetic Care first for her."
3. **Recommended for You / Buy It Again / Picked for Your Health** — "Each shelf says *why*:
   'Reordered 4 times', 'Relevant to your arthritis'. Your merchandising team can read every rule."
4. **Ask SilverLeaf** — type *"something for arthritis pain under $15"*. "That's a Genie Agent. It
   only sees the product catalog." Then type *"how much have I spent this year?"* — "It refuses,
   because member and order data aren't in its scope. That's by construction, not by prompt."

Switch to a brand-new member: "No history? They still get condition-matched picks and best-sellers
— never an empty page."

## 3 · Technical walkthrough (10 min) — *technical stakeholder*

**One job, one platform.** Open the job run graph.

> "This is the entire system: one Lakeflow Job, eight tasks, every stage reading the previous stage's
> Unity Catalog tables. Nothing is copied by hand, and nothing runs outside Databricks."

| Stage | Show | Say |
|---|---|---|
| **Lakeflow** | Pipeline graph, expectations tab | "Raw JSON lands in a governed Volume. Auto Loader ingests it to bronze; silver casts types and enforces data-quality expectations — every passed/failed record count is in the event log." |
| **Unity Catalog** | Catalog Explorer → `members` → tags; `members_analyst_v` | "Sensitive columns are tagged. Analysts query a de-identified view where health conditions are masked unless they're in the care-team group. The app's service principal can read one view in UC — the product catalog — and nothing else." |
| **ML** | MLflow experiment → `home_page_recs_offline_eval` | "Four recommenders, one temporal holdout, logged metrics. The hybrid won, so the hybrid is what's in production — the evaluation chose the model." |
| **GenAI** | `03c` notebook, `member_messages_checked` | "`ai_query` in batch from SQL — no endpoint to run. Guardrail results per message are a table you can audit." |
| **Lakebase** | Synced tables, Postgres query | "Results are reverse-ETL'd into Lakebase Postgres. A page load is three primary-key lookups — single-digit milliseconds. No feature server, no cache to manage." |
| **Genie Agent** | Space config in git | "The agent's instructions and example SQL are versioned in the repo and applied by the job, so changes are reviewable like code." |
| **App** | `app.py`, app logs | "The app runs as its own service principal, reads Lakebase and calls Genie. Every page load and question is logged." |

**Likely questions**

- *"Why not real-time?"* — "Allowances and purchase history change daily, not by the second. Nightly
  batch is cheaper and simpler; Lakebase gives real-time *serving*. If you want same-session
  updates, switch the synced tables to triggered mode — same code."
- *"What does it cost?"* — "Serverless everywhere: you pay for the nightly job minutes, a Lakebase
  endpoint that scales to zero, and per-token `ai_query` calls. No always-on clusters."
- *"How do we use our own data?"* — "Set `generate_synthetic_data=false` and point five variables at
  your tables. The input contract is documented in notebook 02."
- *"PHI?"* — "Health conditions stay in Unity Catalog, masked for analysts, never sent to Genie,
  excluded from the LLM prompt, and a guardrail rejects any banner that names one."
- *"Lock-in?"* — "Everything is Delta, Postgres and SQL. The bundle redeploys the whole thing to
  another workspace with one command."

## 4 · Close (2 min)

> "Here's what I'd propose: a four-week pilot on one plan's members. Week one, we run this exact
> holdout test on your real order history — before any member sees anything. Weeks two to four,
> an A/B test: half of members get the personalized page. We measure allowance utilization,
> orders per member and repeat visits. If the lift holds, you have the number for your plan
> sponsors and a production pipeline that's already built."

**Ask:** a data-sharing agreement for 12 months of de-identified order history, and a named owner
on the benefits product team.
