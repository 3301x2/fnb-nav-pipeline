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
│  K-means clustering, logistic regression churn prediction,   │
│  CLV linear regression. BigQuery ML, in-warehouse training   │
└────────────────────────┬────────────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  04_marts                                                    │
│  16 dashboard-ready tables: segments, benchmarks, trends,    │
│  demographics, geo, churn risk, behavioral patterns,         │
│  CLV, cohort retention, category intelligence, pitch scoring │
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
│   │   ├── predict_churn.sql         ← churn probability scoring
│   │   ├── train_clv_model.sql       ← CLV linear regression model
│   │   └── predict_clv.sql           ← lifetime value per customer
│   ├── 04_marts/
│   │   ├── mart_cluster_profiles.sql
│   │   ├── mart_cluster_summary.sql
│   │   ├── mart_behavioral_summary.sql
│   │   ├── mart_geo_summary.sql
│   │   ├── mart_churn_risk_rules.sql ← fallback rule-based churn (not in pipeline)
│   │   ├── mart_monthly_trends.sql
│   │   ├── mart_demographic_summary.sql
│   │   ├── mart_destination_benchmarks.sql
│   │   ├── mart_cohort_retention.sql
│   │   ├── mart_category_affinity.sql
│   │   ├── mart_category_scorecard.sql
│   │   ├── mart_pitch_opportunities.sql
│   │   ├── mart_churn_explained.sql
│   │   ├── mart_spend_momentum.sql
│   │   └── mart_category_propensity.sql
│   └── 05_looker_views/
│       └── create_views.sql          ← 19 Looker Studio views
│
├── notebooks/
│   ├── 01_data_exploration.ipynb     ← what's in the data?
│   ├── 02_feature_analysis.ipynb     ← RFM distributions + correlations
│   ├── 03_cluster_profiling.ipynb    ← segment stories + elbow method + churn eval
│   ├── 04_client_pitch_analysis.ipynb ← generate pitch deck insights for any client
│   ├── 05_cohort_retention.ipynb     ← cohort retention curves
│   ├── 06_pitch_opportunities.ipynb  ← pitch scoring and opportunity ranking
│   ├── 07_category_affinity.ipynb    ← cross-category shopping patterns
│   ├── 08_category_scorecard.ipynb   ← portfolio health analysis
│   ├── 09_model_validation.ipynb     ← ML model performance checks
│   ├── 10_customer_lifetime_value.ipynb ← CLV model training and evaluation
│   ├── 11_churn_deep_dive.ipynb      ← churn explainability analysis
│   └── 12_growth_opportunities.ipynb ← growth and momentum insights
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
│   ├── md2docx.sh                    ← convert markdown to styled Word doc
│   ├── generate_report.py            ← generate insights report (Python)
│   ├── generate_report.sh            ← generate insights report (shell wrapper)
│   ├── record_demo.py                ← record dashboard demo video
│   ├── record_walkthrough.sh         ← record pipeline walkthrough
│   ├── generate_context.sh           ← generate repo context for LLMs
│   ├── cost_report.sh                ← BigQuery cost/usage report
│   ├── generate_insights_pdf.py      ← generate PDF insights report
│   ├── looker_generator.py           ← generate Looker Studio dashboards
│   └── check_and_grant.sh            ← check permissions and grant access
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
| analytics | 4 + 3 models | Features, scores, spend metrics, k-means + churn + CLV models |
| marts | 16 | Dashboard-ready analytical tables |

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

## Notebooks

| # | Notebook | Description |
|---|----------|-------------|
| 01 | data_exploration | Initial data profiling and quality assessment |
| 02 | feature_analysis | RFM feature distributions and correlations |
| 03 | cluster_profiling | Segment stories, elbow method, and churn evaluation |
| 04 | client_pitch_analysis | Generate pitch deck insights for any client |
| 05 | cohort_retention | Customer retention curves by signup cohort |
| 06 | pitch_opportunities | Pitch scoring and opportunity ranking |
| 07 | category_affinity | Cross-category shopping pattern analysis |
| 08 | category_scorecard | Portfolio health metrics per category |
| 09 | model_validation | ML model performance and validation checks |
| 10 | customer_lifetime_value | CLV model training, evaluation, and tier analysis |
| 11 | churn_deep_dive | Churn explainability and top risk factors |
| 12 | growth_opportunities | Growth momentum and expansion insights |

## Built by

Prosper Sikhwari
