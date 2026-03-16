# FNB NAV Data Platform — Technical Architecture

**Audience:** Engineering, data team, technical stakeholders  
**Classification:** Internal  
**Date:** March 2026  
**Author:** Prosper Sikhwari

> **Note on version control and data security:** No shared or public repositories have been provisioned for this project. All source code is managed via local Git on the developer's machine using VS Code. No data, credentials, PII, or proprietary information is stored externally or committed to any repository. The repository contains only SQL scripts, Python application code, Terraform configuration, Jupyter notebooks, and documentation — zero data files. When a shared internal repository (e.g. Cloud Source Repositories, internal GitHub/GitLab) is provisioned, the codebase can be migrated with a single `git remote set-url` command.

---

## TL;DR

We built a full analytics pipeline on BigQuery that processes 2.2 billion transactions and 7.2 million customers into a 12-page interactive dashboard. Two ML models run inside BigQuery: k-means segmentation (Davies-Bouldin 1.29, 5 clean segments) and logistic regression churn prediction (83% accuracy, 666K customers flagged as at-risk representing R31.6B in spend).

The Streamlit dashboard is a development and feedback tool only — once stakeholders sign off on the views, the final client-facing dashboard will be built in Looker Studio as per requirements.

The pipeline uses raw SQL + bash scripts. It works, it scales, and it runs on production data today. Dataform is the natural next step for scheduling, incremental loads, and testing — but the migration is mechanical (~25 hours), not architectural. Every SELECT statement stays the same.

---

## 1. State evolution — before, now, future

### 1.1 BEFORE: manual, fragmented, no version control

```
┌──────────────────────────────────────────────────────────────────────┐
│                        fmn-sandbox (BigQuery)                        │
│                                                                      │
│  ┌─────────────────┐     ┌──────────────────┐                        │
│  │ customer_spend   │     │ spend_lookups     │                        │
│  │  base_data       │     │  6 lookup tables  │                        │
│  │  transaction_data│     │                   │                        │
│  │  (2.2B rows)     │     │                   │                        │
│  └────────┬────────┘     └────────┬──────────┘                        │
│           │                       │                                    │
│           ▼                       ▼                                    │
│  ┌──────────────────────────────────────────┐                        │
│  │  30+ loose shared queries (no naming      │                        │
│  │  convention, duplicates, client-specific)  │                        │
│  │                                           │                        │
│  │  "Adidas Audience Extractions"            │                        │
│  │  "Kauai Extraction"                       │                        │
│  │  "Kauai extract"  ← duplicate             │                        │
│  │  "PNP audience testing for Kevin"         │                        │
│  │  "random lookups for testing"             │                        │
│  └──────────────────┬───────────────────────┘                        │
│                     │                                                 │
│                     ▼                                                 │
│  ┌──────────────────────────────────────────┐                        │
│  │  16+ unversioned notebooks               │                        │
│  │  (no standard structure)                  │                        │
│  └──────────────────┬───────────────────────┘                        │
│                     │                                                 │
│                     ▼                                                 │
│  ┌──────────────────────────────────────────┐                        │
│  │  Manual Excel exports                     │                        │
│  │  "I would then export and in Excel        │                        │
│  │   create some useful visuals"             │                        │
│  └──────────────────────────────────────────┘                        │
│                                                                      │
│  Problems:                                                           │
│  ✗ No version control                                                │
│  ✗ No pipeline — manual query → export → Excel                      │
│  ✗ demo_* field names (nobody knows what demo_5 means)               │
│  ✗ Recode everything per client pitch                                │
│  ✗ No ML — basic manual RFM                                         │
│  ✗ No competitor anonymization                                       │
│  ✗ No cost optimization (full table scans)                           │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.2 NOW: automated pipeline, ML, version-controlled

```
┌──────────────────────────────────────────────────────────────────────┐
│  Local Git repo: fnb-nav-pipeline (VS Code, not public)              │
│                                                                      │
│  bash scripts/run.sh 0 → 1 → 2 → 3 → 4 → 5                        │
│                                                                      │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐            │
│  │ Raw data     │    │ Lookups      │    │ Demographics │            │
│  │ 2.2B txns    │───▶│ 6 tables     │───▶│ 7.2M custs  │            │
│  └──────┬──────┘    └──────────────┘    └──────┬───────┘            │
│         │                                       │                    │
│         ▼                                       ▼                    │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  01_STAGING                                           │            │
│  │  stg_transactions (partitioned by month, clustered    │            │
│  │    by CATEGORY_TWO + DESTINATION)                     │            │
│  │  stg_customers (demo_* → gender, age, income, etc.)  │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  02_INTERMEDIATE                                      │            │
│  │  int_rfm_features (20+ features per customer)         │            │
│  │  int_rfm_scores (quintile 1-5)                        │            │
│  │  int_customer_category_spend (all clients, all cats)  │            │
│  │  int_destination_metrics (benchmarks per destination)  │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  03_ML (BigQuery ML — in-warehouse)                   │            │
│  │  ┌─────────────────────┐  ┌────────────────────────┐ │            │
│  │  │ K-means clustering  │  │ Logistic regression    │ │            │
│  │  │ 9 features, k=5     │  │ churn classifier       │ │            │
│  │  │ DB index: 1.295     │  │ 15 features            │ │            │
│  │  │ 5 segments           │  │ Accuracy: 0.831        │ │            │
│  │  └──────────┬──────────┘  └──────────┬─────────────┘ │            │
│  │             │                         │               │            │
│  │             ▼                         ▼               │            │
│  │  mart_cluster_output      mart_churn_risk             │            │
│  │  (5.8M customers)         (5.5M scored)               │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  04_MARTS (8 tables — dashboard reads from here)      │            │
│  │  cluster_profiles    │ cluster_summary                │            │
│  │  behavioral_summary  │ geo_summary                    │            │
│  │  monthly_trends      │ demographic_summary            │            │
│  │  destination_benchmarks │ churn_risk                  │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  STREAMLIT DASHBOARD (development + feedback only)    │            │
│  │  12 pages │ category/client dropdowns                 │            │
│  │  auto-anonymization │ ML evaluation                   │            │
│  │                                                       │            │
│  │  ⚠ NOT the final deliverable.                        │            │
│  │  Purpose: validate data, get stakeholder sign-off     │            │
│  │  on which views matter, iterate quickly.              │            │
│  │  Final dashboard: Looker Studio (per requirements).   │            │
│  └──────────────────────────────────────────────────────┘            │
│                                                                      │
│  ✓ Version controlled (local Git, VS Code)                           │
│  ✓ No data or credentials in repository                              │
│  ✓ One command builds everything                                     │
│  ✓ Proper naming conventions throughout                              │
│  ✓ Two ML models (segmentation + churn)                              │
│  ✓ Any client from a dropdown (no recode)                            │
│  ✓ Partitioned + clustered for cost                                  │
│  ✓ Terraform: destroy in one shot                                    │
│  ✓ 4 notebooks for exploration + pitches                             │
└──────────────────────────────────────────────────────────────────────┘
```

### 1.3 FUTURE: Dataform + Looker Studio + incremental

```
┌──────────────────────────────────────────────────────────────────────┐
│  Dataform (BigQuery native orchestration)                            │
│                                                                      │
│  Automatic DAG │ Scheduled runs │ Incremental loads │ Assertions     │
│                                                                      │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  01_STAGING (incremental — MERGE new rows only)       │            │
│  │  First run: full load (2.2B rows, one-time cost)      │            │
│  │  Daily runs: append delta only (~5M rows/day)          │            │
│  │  Cost savings: ~90% reduction in scan costs            │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  02_INTERMEDIATE (ref() dependencies, auto-ordered)   │            │
│  │  Assertions: non-null, uniqueness, row conditions     │            │
│  │  is_incremental() on customer_category_spend          │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  03_ML (BigQuery ML — unchanged)                      │            │
│  │  K-means + Churn classifier                           │            │
│  │  Wrapped in Dataform operations                       │            │
│  │                                                       │            │
│  │  Future: Vertex AI for boosted trees when             │            │
│  │  region support arrives in africa-south1              │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│                         ▼                                             │
│  ┌──────────────────────────────────────────────────────┐            │
│  │  04_MARTS (unchanged — full refresh from staged data) │            │
│  │  + mart_pitch_ready (anonymization in SQL, not app)   │            │
│  │  + Row-level security for POPIA compliance            │            │
│  └──────────────────────┬───────────────────────────────┘            │
│                         │                                             │
│              ┌──────────┴──────────┐                                  │
│              ▼                     ▼                                   │
│  ┌────────────────────┐ ┌──────────────────────────────┐             │
│  │ LOOKER STUDIO       │ │ STREAMLIT (micro-frontend)   │             │
│  │ (final deliverable) │ │ ROI Simulator only            │             │
│  │                     │ │ (embedded via iframe)          │             │
│  │ Client-facing       │ │                               │             │
│  │ Pitch decks         │ └──────────────────────────────┘             │
│  │ Brand-compliant     │                                              │
│  │ Creative team owns  │                                              │
│  │ styling             │                                              │
│  └─────────────────────┘                                              │
│                                                                      │
│  ✓ Everything from "Now" plus:                                       │
│  ✓ Scheduled daily runs (no manual trigger)                          │
│  ✓ Incremental loads (90% cost reduction)                            │
│  ✓ Automatic dependency graph (DAG)                                  │
│  ✓ Native data testing (assertions on every model)                   │
│  ✓ Auto-generated data catalog                                       │
│  ✓ Anonymization in SQL (not application layer)                      │
│  ✓ POPIA-compliant row-level security                                │
│  ✓ Looker Studio for client-facing dashboards                        │
│  ✓ Creative team can own fonts/colors/branding                       │
│  ✓ Internal shared repository (Cloud Source Repos or GitLab)         │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. Executive summary

This document presents the architecture of the FNB NAV Data Platform, a production-grade analytics pipeline built on Google BigQuery. It processes 2.2 billion transaction rows and 7.2 million customer profiles into actionable insights for client pitch decks, customer segmentation, churn prediction, and competitive benchmarking.

The solution was implemented using raw SQL scripts with a shell-based orchestrator. This document evaluates the trade-offs of that approach against a Dataform-based implementation and provides a clear migration path.

## 3. Current architecture (SQL + bash)

### 3.1 Pipeline layers

The pipeline follows a four-layer medallion architecture, executed sequentially via a shell script:

| Layer | Tables | Purpose | Key feature |
|-------|--------|---------|-------------|
| **01_staging** | stg_transactions, stg_customers | Clean, join lookups, strip PII, rename fields | Partitioned by month, clustered by category + destination |
| **02_intermediate** | int_rfm_features, int_rfm_scores, int_customer_category_spend, int_destination_metrics | RFM features, quintile scores, spend share, destination KPIs | Share-of-wallet per customer × category × destination |
| **03_ml** | kmeans_customer_segments (MODEL), churn_classifier (MODEL), mart_cluster_output, mart_churn_risk | K-means segmentation (k=5), logistic regression churn prediction | BigQuery ML: in-warehouse training and inference |
| **04_marts** | 8 dashboard-ready tables | Profiles, summaries, trends, benchmarks, geo, demographics | Dashboard reads from marts only; SQL builds everything |

### 3.2 Execution model

The pipeline is executed via `bash scripts/run.sh` with step-by-step support:

- `bash scripts/run.sh` — runs all steps sequentially (0 through 5)
- `bash scripts/run.sh 1` — runs staging only, prints verification queries
- `bash scripts/run.sh 3` — runs ML models only (k-means + churn classifier)

Each step depends on the previous. Execution order is enforced by the script, not by a dependency graph.

### 3.3 ML models

| Model | Type | Features | Key metric | Output |
|-------|------|----------|------------|--------|
| **Customer segmentation** | K-means (unsupervised) | 9 RFM behavioral features | Davies-Bouldin: 1.295 | 5 segments: Champions through Dormant |
| **Churn prediction** | Logistic regression (supervised) | 15 features (behavioral + demographic) | Accuracy: 0.831, F1: 0.556 | Probability score per customer (0–100%) |

### 3.4 Dashboard strategy

```
  Development workflow:

  Phase 1 (now)              Phase 2 (next)            Phase 3 (final)
  ───────────────           ─────────────────          ──────────────────
  Streamlit prototype       Stakeholder review         Looker Studio build
  
  ✓ All 12 views built      "These 8 views matter"     Brand-compliant
  ✓ Data validated           "Change X, add Y"          Creative team styles
  ✓ Iterate in real-time    "This is ready"             Client-facing pitch
  ✓ Quick feedback loops                                Owned by the team
```

The current Streamlit dashboard is a **development and feedback tool**, not the final deliverable. Its purpose is to validate the data, prototype views, and iterate quickly with stakeholders. The final client-facing dashboard will be built in **Looker Studio** as per team requirements.

The ROI Simulator page (which requires sliders and live calculations) may remain as a Streamlit micro-frontend embedded via iframe in Looker Studio.

### 3.5 Version control

All source code is managed via **local Git on the developer's machine** using VS Code. The repository contains only code and documentation — zero data, credentials, or PII. When an internal shared repository is provisioned, migration requires a single `git remote set-url` command.

### 3.6 Infrastructure management

Terraform manages all BigQuery resources (3 datasets, 14 tables). A single `terraform destroy` command removes all pipeline resources cleanly, enabling rapid environment teardown and rebuild.

---

## 4. Dataform architecture (proposed)

### 4.1 What changes

Dataform is BigQuery's native transformation framework. It wraps SQL in SQLX files with dependency management, scheduling, and testing built in. The migration is mechanical: every SELECT statement stays the same.

| Aspect | Current (SQL + bash) | Dataform |
|--------|---------------------|----------|
| **Table creation** | CREATE OR REPLACE TABLE in every .sql file | Dataform generates DDL from config block |
| **Dependency management** | Enforced by script execution order (steps 0–5) | Automatic DAG from ref() function calls |
| **Project ID** | Hardcoded in every SQL file (51 references) | Set once in dataform.json |
| **Incremental logic** | Custom DECLARE/IF/INSERT per file | Built-in is_incremental() macro |
| **Data testing** | Separate validate.sh with manual checks | Native assertions: uniqueKey, nonNull, rowConditions |
| **Scheduling** | Manual: bash scripts/run.sh | Built-in cron scheduling in GCP console |
| **Documentation** | Separate data_dictionary.md | Inline column descriptions; auto-generated catalog |

### 4.2 What stays the same

- Every SELECT statement, every JOIN, every calculation
- The four-layer structure (staging, intermediate, ml, marts)
- BigQuery ML models (wrapped in Dataform operations)
- Partitioning and clustering configurations
- The Looker Studio dashboard (reads from marts only)

### 4.3 File conversion example

**Current** (`stg_transactions.sql`):

```sql
CREATE OR REPLACE TABLE `fmn-sandbox.staging.stg_transactions`
PARTITION BY DATE_TRUNC(EFF_DATE, MONTH)
CLUSTER BY CATEGORY_TWO, DESTINATION AS
SELECT ... FROM `fmn-sandbox.customer_spend.transaction_data` t ...
```

**Dataform** (`stg_transactions.sqlx`):

```sql
config {
  type: "table",
  schema: "staging",
  partition_by: "DATE_TRUNC(EFF_DATE, MONTH)",
  cluster_by: ["CATEGORY_TWO", "DESTINATION"]
}
SELECT ... FROM ${ref('raw_transaction_data')} t ...
```

---

## 5. Trade-off analysis

### 5.1 When to use the current approach (SQL + bash)

- Rapid prototyping: zero setup, no framework to install
- First-time full load: the initial 2.2B row materialisation requires CREATE OR REPLACE regardless
- Environments without Dataform access
- Simple pipelines with fewer than 20 models and a single developer
- Portability: raw SQL runs on any warehouse with minor dialect changes

### 5.2 When to use Dataform

- Ongoing production workloads with scheduled runs
- Incremental loads (avoid scanning 2.2B rows every run)
- Multiple developers (DAG prevents conflicts)
- Data quality enforcement (assertions on every model)
- Cost control (incremental processing = ~90% scan reduction)

### 5.3 Risk matrix

| Risk | SQL + bash | Dataform | Mitigation |
|------|-----------|----------|------------|
| **Partial failure mid-run** | High: re-runs scan all data | Low: DAG retries only failed nodes | Manual re-run from failed step |
| **Cost overrun** | High: 2.2B row full scans | Low: incremental only | Incremental logic (Phase 2) |
| **Wrong execution order** | Medium: script enforces, but editable | None: ref() enforces DAG | Step-by-step runner |
| **Data quality** | Medium: post-hoc checks only | Low: assertions block downstream | 5 quality checks in validate.sh |
| **Onboarding** | Medium: must read run.sh | Low: visual DAG | README + data dictionary |

---

## 6. Migration path

### 6.1 Estimated effort

| Task | Files | Effort | Complexity |
|------|-------|--------|------------|
| Initialise Dataform repo | 1 | 1 hour | Low |
| Convert staging to .sqlx | 2 | 2 hours | Low |
| Convert intermediate to .sqlx | 4 | 3 hours | Low |
| Convert ML to Dataform operations | 4 | 4 hours | Medium |
| Convert marts to .sqlx | 8 | 3 hours | Low |
| Add assertions | 16 | 4 hours | Low |
| Add incremental logic | 3 | 4 hours | Medium |
| Testing + handoff | All | 4 hours | Low |
| **Total** | **16 files** | **~25 hours** | **Low–Medium** |

### 6.2 What gets deleted

- `scripts/run.sh` — replaced by Dataform scheduler
- `scripts/validate.sh` — replaced by assertions
- `terraform/main.tf` — Dataform manages table creation
- All CREATE OR REPLACE statements
- All hardcoded project ID references (51 occurrences)

---

## 7. Production results

### 7.1 Scale

| Metric | Value |
|--------|-------|
| Raw transactions processed | **2,237,540,286** |
| Customers staged | **7,257,025** |
| Customers segmented (k-means) | **5,789,281** |
| Customers scored (churn ML) | **5,531,237** |
| Customer-category-destination spend rows | **212,809,215** |
| Destination benchmarks | **14,536** |

### 7.2 ML model performance

| Metric | K-means segmentation | Churn classifier |
|--------|---------------------|-----------------|
| Model type | K-means (unsupervised) | Logistic regression (supervised) |
| Davies-Bouldin index | **1.295** (< 2.0 = good) | N/A |
| Accuracy | N/A | **0.831** |
| F1 score | N/A | **0.556** |
| Cluster balance | Largest: 30.1% (< 40%) | N/A |
| Business insight | 8.2% of customers drive 44.5% of revenue | 666K at risk = R31.6B spend |

---

## 8. Recommendation

The current SQL + bash implementation is appropriate for the current phase: initial deployment, validation, and stakeholder buy-in.

**Dataform migration is recommended as the next phase, not because the current approach is flawed, but because the pipeline has proven its value and now needs production hardening.**

Trigger migration when:

- The pipeline needs a recurring schedule (daily/weekly)
- A second developer joins the project
- BigQuery scan costs exceed threshold due to full refreshes
- Dataform access is provisioned

---

## 9. Appendix: file inventory

| File | Purpose |
|------|---------|
| sql/01_staging/stg_transactions.sql | Join lookups, partition, cluster, strip PII |
| sql/01_staging/stg_customers.sql | Deduplicate, rename demo_* to real names |
| sql/02_intermediate/int_rfm_features.sql | 20+ behavioral features per customer |
| sql/02_intermediate/int_rfm_scores.sql | Quintile scoring 1–5 |
| sql/02_intermediate/int_customer_category_spend.sql | Per customer × category × destination spend |
| sql/02_intermediate/int_destination_metrics.sql | Per-destination KPIs within category |
| sql/03_ml/train_model.sql | K-means clustering (9 features, k=5) |
| sql/03_ml/predict_and_name.sql | Cluster assignment + segment naming |
| sql/03_ml/train_churn_model.sql | Logistic regression churn classifier |
| sql/03_ml/predict_churn.sql | Churn probability scoring |
| sql/04_marts/ (8 files) | Dashboard-ready analytical tables |
| dashboards/app.py | 12-page Streamlit dashboard (dev/feedback only) |
| notebooks/ (4 files) | BigQuery exploration + client pitch notebooks |
| terraform/main.tf | Infrastructure as code (create/destroy) |
| scripts/run.sh, validate.sh | Pipeline runner + validation |
| scripts/md2docx.sh | Convert markdown docs to branded Word format |
| docs/architecture_executive.md | This document (manager version) |
| docs/architecture_technical.md | Detailed technical architecture |
| docs/data_dictionary.md | Full field reference |
| docs/template.docx | Team-branded Word template |
