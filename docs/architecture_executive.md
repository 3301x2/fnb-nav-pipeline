# FNB NAV Data Platform — Solution Overview

**Audience:** Leadership and stakeholders  
**Classification:** Internal  
**Date:** March 2026  
**Author:** Prosper Sikhwari

> **Note on version control:** No shared or public repositories have been provisioned for this project. All source code is managed via local Git on the developer's machine using VS Code. No data, credentials, or proprietary information is stored externally. When a shared repository is provisioned, the codebase can be migrated in minutes.

---

## TL;DR

We transformed a manual, fragmented analytics environment into an automated pipeline that processes **2.2 billion transactions** and **7.2 million customers** to produce client-ready insights. Two machine learning models run inside BigQuery — customer segmentation and churn prediction. A prototype dashboard lets the team pitch any client by selecting from a dropdown. No recoding, no re-running.

---

## 1. What changed

### Before

```
  Manual process (per client pitch):
  
  ┌────────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────────┐
  │ Write query │────▶│ Run in       │────▶│ Export to    │────▶│ Build       │
  │ from scratch│     │ BigQuery     │     │ Excel        │     │ visuals     │
  └────────────┘     └──────────────┘     └──────────────┘     └─────────────┘
  
  Repeat for every client. Recode filters. Manual anonymization.
  No version control. No ML. Field names like "demo_5" (meaning unknown).
  ~30 loose queries, ~16 unversioned notebooks, duplicate work.
```

### After

```
  Automated process (all clients, one pipeline):
  
  ┌────────────┐     ┌──────────────┐     ┌──────────────┐     ┌─────────────┐
  │ One command │────▶│ Pipeline     │────▶│ ML models    │────▶│ Dashboard   │
  │ builds all  │     │ stages and   │     │ segment and  │     │ pick client │
  │             │     │ cleans data  │     │ score churn  │     │ from dropdown│
  └────────────┘     └──────────────┘     └──────────────┘     └─────────────┘
  
  Monday: pitch Adidas.  Wednesday: pitch Nike.  Friday: pitch Shell.
  Same pipeline. Same dashboard. Just pick from the dropdown.
```

---

## 2. Business value delivered

### Customer segmentation (ML)

The pipeline automatically groups 5.8 million customers into 5 behavioral segments:

| Segment | % of customers | % of revenue | What to do |
|---------|---------------|-------------|------------|
| **Champions** | 8.2% | 44.5% | Retain with exclusive offers |
| **Loyal High Value** | 14.8% | 22.3% | Cross-sell into new categories |
| **Steady Mid-Tier** | 30.1% | 18.7% | Upsell to higher tiers |
| **At Risk** | 19.9% | 8.3% | Re-engage before they leave |
| **Dormant** | 27.0% | 6.2% | Win-back or accept attrition |

**Headline:** Champions are 8.2% of customers but drive 44.5% of revenue.

### Churn prediction (ML)

The model scores every customer with a churn probability (0–100%):

- **666,301 customers** (12%) flagged as Critical or High risk
- **R31.6 billion** in historical spend at risk
- A **10% re-engagement rate** would recover ~R3.16 billion

### Client pitch readiness

- **Share of wallet:** For any client, see what % of customers' category spend they capture vs competitors
- **Benchmarks:** Client shown by name, competitors anonymized as Competitor 1, 2, 3
- **Trends:** Monthly client spend vs category total
- **Demographics:** Who shops in the category by age, gender, income
- **Geography:** Province and municipality spend concentration

---

## 3. Dashboard strategy

```
  Phase 1 (now)              Phase 2 (next)            Phase 3 (final)
  ───────────────           ─────────────────          ──────────────────
  Streamlit prototype       Stakeholder review         Looker Studio build
  
  ✓ All 12 views built      "These 8 views matter"     Brand-compliant
  ✓ Data validated           "Change X, add Y"          Creative team styles
  ✓ Iterate in real-time    "This is ready"             Client-facing pitch
  ✓ Quick feedback loops                                Owned by the team
```

The Streamlit dashboard is a **development and feedback tool only**. Its purpose is to prove the data works and get sign-off on which views matter. The final client-facing dashboard will be built in **Looker Studio** as per team requirements, where the creative team can own fonts, colors, and branding.

---

## 4. What's next

| Phase | What | Why | Effort |
|-------|------|-----|--------|
| **Incremental loads** | Only process new data each run, not the full 2.2B rows | Reduces BigQuery costs by ~90% | 1 week |
| **SQL anonymization** | Move competitor anonymization from the app into the database | Security best practice, easier Looker migration | 2 days |
| **Looker Studio dashboard** | Build final client-facing views from the prototype | Team standard, brand-compliant | 2 weeks |
| **Dataform migration** | Replace bash scripts with BigQuery's native orchestration | Scheduling, testing, dependency management | 1 week |
| **POPIA compliance** | Data classification tags, row-level security policies | Legal requirement for SA banking data | 1 week |

---

## 5. Scale

| What | How much |
|------|----------|
| Raw transactions processed | **2.2 billion** |
| Customers profiled | **7.2 million** |
| Customers segmented by ML | **5.8 million** |
| Customers scored for churn | **5.5 million** |
| Categories available for pitches | **All** (dropdown selection) |
| Destinations benchmarked | **14,536** |

---

## 6. Technical summary (one paragraph)

The solution is a four-layer SQL pipeline (staging → features → ML → dashboard tables) running on Google BigQuery in the `fmn-sandbox` project, `africa-south1` region. Two BigQuery ML models are trained in-warehouse: k-means clustering for customer segmentation and logistic regression for churn prediction. All tables are partitioned and clustered for cost efficiency. Infrastructure is managed by Terraform (one-command teardown). The pipeline runs via a single shell script with step-by-step execution. Four Jupyter notebooks provide interactive exploration for the analytics team.
