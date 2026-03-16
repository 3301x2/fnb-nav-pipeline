# FNB NAV — Analytics Pipeline

A production-grade data pipeline for customer segmentation, competitive benchmarking, and client pitch analytics built on BigQuery.

## What it does

```
Raw FNB transaction data (2.2B+ rows)
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  01_staging                                                  │
│  Join lookups, rename demographics, strip PII                │
│  Partition by month, cluster by category + destination       │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  02_intermediate                                             │
│  RFM features (20+ per customer), quintile scores,           │
│  spend per customer × category × destination                 │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  03_ml                                                       │
│  K-means clustering + logistic regression churn prediction   │
│  BigQuery ML, in-warehouse training and inference            │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  04_marts                                                    │
│  8 dashboard-ready tables: segments, benchmarks, trends,     │
│  demographics, geo, churn risk, behavioral patterns          │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Dashboard (Streamlit)                                       │
│  Pick category → pick client → everything filters live       │
│  Competitors auto-anonymize (Competitor 1, 2, 3...)          │
│  No SQL changes needed between client pitches                │
└─────────────────────────────────────────────────────────────┘
```

## Quick start

```bash
# 1. Clone
git clone <repo-url> && cd fnb-nav-pipeline

# 2. Authenticate
gcloud auth login
gcloud config set project fmn-sandbox

# 3. (Optional) Create infrastructure with Terraform
cd terraform && terraform init && terraform apply && cd ..

# 4. Run the full pipeline
bash scripts/run.sh sandbox          # all steps on fmn-sandbox
bash scripts/run.sh sandbox 1        # just staging
bash scripts/run.sh production 3     # ML models on fmn-production

# 5. Launch dashboard
pip install -r dashboards/requirements.txt
streamlit run dashboards/app.py

# or target production data:
BQ_PROJECT=fmn-production streamlit run dashboards/app.py
```

## Project structure

```
fnb-nav-pipeline/
├── sql/
│   ├── 01_staging/
│   │   ├── stg_transactions.sql      ← joins lookups, partitioned by month
│   │   └── stg_customers.sql         ← deduplicates, renames demo_* fields
│   ├── 02_intermediate/
│   │   ├── int_rfm_features.sql      ← 20+ behavioral features per customer
│   │   ├── int_rfm_scores.sql        ← quintile scoring (1-5)
│   │   ├── int_customer_category_spend.sql  ← share of wallet for any client
│   │   └── int_destination_metrics.sql      ← per-destination KPIs
│   ├── 03_ml/
│   │   ├── train_model.sql           ← k-means (9 features, k=5)
│   │   ├── predict_and_name.sql      ← cluster assignment + naming
│   │   ├── train_churn_model.sql     ← logistic regression churn classifier
│   │   └── predict_churn.sql         ← churn probability scoring
│   └── 04_marts/
│       ├── mart_cluster_profiles.sql
│       ├── mart_cluster_summary.sql
│       ├── mart_behavioral_summary.sql
│       ├── mart_geo_summary.sql
│       ├── mart_churn_risk.sql
│       ├── mart_monthly_trends.sql
│       ├── mart_demographic_summary.sql
│       └── mart_destination_benchmarks.sql
│
├── notebooks/
│   ├── 01_data_exploration.ipynb     ← what's in the data?
│   ├── 02_feature_analysis.ipynb     ← RFM distributions + correlations
│   ├── 03_cluster_profiling.ipynb    ← segment stories + elbow method + churn eval
│   └── 04_client_pitch_analysis.ipynb ← generate pitch deck insights for any client
│
├── dashboards/
│   ├── app.py                        ← 12-page Streamlit dashboard
│   ├── requirements.txt
│   └── viewers.txt                   ← authorized dashboard viewers
│
├── terraform/
│   └── main.tf                       ← create / destroy all BigQuery resources
│
├── scripts/
│   ├── run.sh                        ← pipeline runner (sandbox/production)
│   ├── validate.sh                   ← row counts + quality checks
│   ├── deploy.sh                     ← deploy dashboard to Cloud Run
│   ├── grant_access.sh               ← grant viewer access to dashboard
│   ├── apply_template.py             ← inject content into branded Word template
│   └── md2docx.sh                    ← convert markdown to styled Word doc
│
├── docs/
│   ├── architecture_executive.md     ← solution overview (for managers)
│   ├── architecture_technical.md     ← detailed architecture (for engineers)
│   └── data_dictionary.md            ← full field reference
│
├── Dockerfile                        ← Cloud Run container
├── .dockerignore
└── README.md
```

## Key design decisions

**SQL builds everything, dashboard filters.** The pipeline processes all categories and all destinations. When you pitch Adidas this week and Nike next week, just pick from a dropdown — no SQL changes, no re-running.

**Environment-aware pipeline.** SQL files use a `__PROJECT__` placeholder. The runner substitutes the actual project ID at runtime so the same code targets sandbox or production.

**Competitor anonymization happens at the dashboard level.** `mart_destination_benchmarks` stores real destination names. The Streamlit app shows the selected client by name and labels everyone else as "Competitor 1", "Competitor 2", etc.

**Naming conventions fixed at staging.** Raw `demo_*` fields are renamed once in `stg_customers` (demo_1 → gender, demo_2 → age, demo_7 → estimated_income, etc.). Every downstream table uses human-readable names.

**Partitioning + clustering for cost.** `stg_transactions` is partitioned by `EFF_DATE` (monthly) and clustered by `CATEGORY_TWO` + `DESTINATION`. On the 2.2B row production table, this means queries that filter by date and category only scan a fraction of the data.

**Terraform for lifecycle management.** `terraform destroy` removes all datasets and tables cleanly. No orphaned resources.

## Pipeline layers

| Layer | Tables | Purpose |
|-------|--------|---------|
| staging | 2 | Clean, joined, PII-free source of truth |
| analytics | 4 + 2 models | Features, scores, spend metrics, k-means + churn models |
| marts | 8 | Dashboard-ready analytical tables |

## Dashboard pages

1. **Executive Summary** — Pipeline KPIs + client headline numbers
2. **Customer Segments** — K-means clusters with profiles and actions
3. **Spend Share** — Client share of wallet within category
4. **Demographics** — Age, gender, income breakdown per category
5. **Trends** — Monthly client vs category spend
6. **Behavioral** — Time-of-day, weekend patterns per segment
7. **Geo Insights** — Province and municipality spend maps
8. **Churn Risk** — ML logistic regression churn scoring
9. **Benchmarks** — Client vs anonymized competitors
10. **ROI Simulator** — Scenario modelling for pitch decks
11. **ML Evaluation** — Model performance metrics and validation
12. **Data Health** — Row counts and quality checks

## Built by

Prosper Sikhwari
