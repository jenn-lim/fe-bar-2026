# Evidence 06 — Databricks App (SilverLeaf member portal)

Streamlit app serving the personalized home page from Lakebase, with the Genie-powered
"Ask SilverLeaf" product finder. Runs as its own service principal.

## App status
```
name: silverleaf-home
compute_status: ACTIVE
app_status: RUNNING - App has status: App is running
service_principal_client_id: b6ff88a9-8be4-4e4f-b2e2-03c03f7f14bb
url: https://silverleaf-home-7474656026899190.aws.databricksapps.com
```

## How the app is evidenced (the evaluator reads text, not the UI)
The app performs no computation — it only reads precomputed serving tables and calls Genie:
- **Personalized home page** reads `members_online` / `recommendations_online` /
  `category_recommendations_online` from Lakebase by member_id — those tables and a sample
  lookup are shown in evidence/03_lakebase.md.
- **"Ask SilverLeaf"** calls the catalog-scoped Genie space — transcripts in evidence/05_genie.md.
- App source: app/app.py (data access lines 40-90, Genie integration lines ~92-150).

## Deploy log (app started successfully)
```
✓ Preparing source code for new app deployment.
✓ Preparing source code for new app deployment.
✓ Installing packages...
✓ App started successfully
You can access the app at https://silverleaf-home-7474656026899190.aws.databricksapps.com
```
