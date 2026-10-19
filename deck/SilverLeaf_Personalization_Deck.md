# SilverLeaf Benefits — A Home Page for Every Member

*Personalizing the OTC benefits store on Databricks*
Business review for SilverLeaf Benefits leadership · All data synthetic

---

## Slide 1 — The outcome

**75% more members see a product they go on to buy.**

- In a 60-day holdout test, today's best-seller home page showed **31%** of members a product they
  later bought for the first time. The personalized home page showed **54%**.
- Every member's page is ordered for their health conditions, purchase history and allowance,
  refreshed nightly and served in milliseconds.
- Built on one platform, governed end to end, and ready for a four-week pilot on real order history.

**The ask:** a 4-week pilot on one plan's members. Week 1 repeats this test on your data before any
member sees a change.

---

## Slide 2 — The problem: one page for very different members

| What happens today | What it costs |
|---|---|
| Every member sees the same best-sellers | A diabetic member on $50 scrolls past walkers and hearing-aid batteries |
| Members don't find what fits their conditions | **Allowance goes unspent**, and the benefit feels worthless |
| No reason to come back each month | Fewer orders per member, weaker renewal story to plan sponsors |
| Plans can't show the benefit is used | Harder to defend the benefit in bids and in member-experience (CAHPS) surveys |

> "Members are leaving money on the table, and our health-plan clients notice."

---

## Slide 3 — What members see now

| Section | What it does for the member |
|---|---|
| ✨ **Welcome banner** | "Robert, you have $50 this month. Your usual test strips and a low-sodium pick are ready below." Written nightly by AI, then checked |
| 🛍️ **Shop Your Categories** | Categories in *their* order: Diabetic Care first if that's what they need |
| ✨ **Recommended for You** | "Members like you also bought…", blended with what fits their conditions |
| 🔁 **Buy It Again** | One-tap reorder of consumables they buy every month |
| 🩺 **Picked for Your Health** | Products for their conditions they haven't tried yet |
| 🔎 **Ask SilverLeaf** | "Something for arthritis pain under $15": plain-English product search |

New members with no history still get condition-matched picks and best-sellers, so no page is ever empty.

---

## Slide 4 — Does it beat today's page? (the proof)

**Method:** hide the last 60 days of purchases, rebuild every approach from earlier data only, then
check which products each member went on to buy **for the first time**. 918 members tested.

| Approach | Members shown a product they later bought (HitRate@10) | Ranking quality (NDCG@10) | vs. today |
|---|---|---|---|
| Today: same best-sellers for everyone | **30.9%** | 0.096 | — |
| "Members like you also bought" only | 41.0% | 0.143 | +32% |
| Matched to health conditions only | 52.1% | 0.201 | +68% |
| **Personalized (blend): what we serve** | **54.1%** | **0.207** | **+75% / +114%** |

- The evaluation **chose the production model**: the blend won, so the blend is what members see.
- The right product also lands **twice as close to the top of the shelf** (NDCG +114%), and the top of the shelf is where members actually click.
- *Synthetic data, so treat the absolute numbers as directional. The same test runs on real order history in pilot week 1.*

---

## Slide 5 — Value to SilverLeaf and its plan sponsors

**KPIs this moves**

| KPI | Owner | How personalization moves it |
|---|---|---|
| **Benefit utilization** (% of allowance spent) | VP Member Experience | Members find relevant products faster |
| **Orders per member per month** | Store / merchandising | Buy It Again plus relevant discovery |
| **Monthly active shoppers** | Benefits product | A page worth coming back to |
| **Member retention / disenrollment** | Plan sponsor | A benefit members feel and use |
| **Member experience scores** (CAHPS) | Plan sponsor Stars team | Fewer "couldn't find what I needed" moments |

**Illustrative value model.** Every input is an assumption, to be replaced with pilot data.

| Input (assumed) | Value |
|---|---|
| Members with OTC benefit | 200,000 |
| Average monthly allowance | $60 |
| Utilization today | 45% |
| Utilization lift from personalization (conservative vs. +75% discovery lift) | +3 pts |
| **Additional benefit dollars reaching members / year** | 200,000 × $60 × 12 × 3% = **$4.3M** |
| Orders per member lift at the same rate | ≈ +7% order volume through the store |

> For the executive sponsor: more of the benefit the plan pays for reaches members.
> For the domain owner: more orders and more repeat visits, with every rule visible and adjustable.

---

## Slide 6 — Safe for a Medicare population

| Risk | Control |
|---|---|
| AI makes a medical claim ("treats", "cures") | Every banner is checked; any failure falls back to a plain template |
| Health condition shown on screen | Banner check rejects any message naming a condition; the AI prompt never includes conditions |
| AI invents a product, price or discount | Checks reject any product not in the member's picks and any amount other than their real allowance |
| Do the checks actually work? | Planted bad messages run through the checks every night. If one gets through, the job stops |
| Analysts see who has what condition | Health conditions are masked unless you're on the care team; analyst view drops names and bands age |
| Product search leaks member data | The search assistant can only see the product catalog. Spend and account questions are refused |

**Result on the latest run:** 2,000 banners generated, all 2,000 passed every check, and 7 of 7 planted bad messages were blocked. Earlier runs were used to tune the checks: removing false alarms ("health" was matching "heal") so safe banners aren't replaced for no reason.

---

## Slide 7 — How it works: one platform, one nightly job

```
Raw member, order & catalog feeds
  → Lakeflow: ingest + data-quality checks        (bad rows logged, never silently dropped)
  → Unity Catalog: one governed copy, tagged and masked
  → ML: personalized shelves, tested against today's page every night
  → GenAI: welcome banner, written and checked in batch
  → Lakebase: page served in single-digit milliseconds
  → Genie Agent: plain-English product search, catalog only
  → Databricks App: the member home page
```

- **No new infrastructure:** no separate search service, feature store or model server to run.
- **Serverless:** you pay for nightly job minutes, a database that scales to zero, and per-message AI calls.
- **Your data, your rules:** point five settings at your own tables; every rule is readable code.

---

## Slide 8 — What the technical team gets

| Need | Delivered by |
|---|---|
| Reliable ingestion with quality gates | Lakeflow Declarative Pipeline, Auto Loader, `EXPECT` rules, event-log metrics |
| Governance auditors can check | Unity Catalog tags, column mask, de-identified view, least-privilege service principal, lineage |
| Models chosen by evidence | MLflow-tracked temporal-holdout evaluation; champion promoted by metric |
| GenAI that can be audited | `ai_query` batch generation; per-message guardrail results in a table; self-test gate |
| Millisecond serving without a cache tier | Lakebase synced tables (reverse ETL from Delta) |
| Natural-language search that can't leak | Genie Agent scoped to one view, config versioned in git |
| Repeatable deploys | One Declarative Automation Bundle: pipeline + job + app |

---

## Slide 9 — Pilot plan (4 weeks)

| Week | What happens | Decision it supports |
|---|---|---|
| 1 | Load 12 months of de-identified order history; rerun this exact holdout test on real data | Is the lift real for *your* members? |
| 2 | Tune shelves with merchandising; review guardrails with compliance | Is it right for the brand and safe? |
| 3–4 | A/B test: 50% of one plan's members get the personalized page | Utilization, orders/member, repeat visits |
| End | Readout to executive sponsor and plan-sponsor team | Go / no-go for all plans |

**Success criteria (proposed):** +2 pts benefit utilization or +5% orders per member in the test group,
with zero guardrail incidents reaching members.

---

## Slide 10 — The ask

1. **Executive sponsor:** approve a 4-week pilot on one plan.
2. **Benefits product owner:** name an owner to tune shelves and review the member page.
3. **Data team:** a data-sharing agreement for 12 months of de-identified order history.
4. **Compliance:** a 30-minute review of the guardrail checks before week 3.

> "In four weeks you'll know, on your own members, how much more of the benefit gets used —
> and the pipeline to deliver it will already be built."
