# Per-Client Customer Segmentation — Design Note

**Audience:** Analytics team and stakeholders
**Classification:** Internal
**Date:** April 2026
**Author:** Prosper Sikhwari

> **Note on version control:** No shared or public repositories have been provisioned for this project. All source code is managed via local Git on the developer's machine using VS Code. Scripts in this repo are written here and pulled from the sandbox machine to run against BigQuery.

---

## TL;DR

Before this change, the Clicks pitch and the Pick n Pay pitch both showed **identical** customer-segment numbers — same 8.2% Champions, same 44.5% of revenue. The reason was simple: the K-means model was trained once on all 5.8 million FNB customers, and every report was reading the same global table without a client filter.

We evaluated two options and shipped **Option A**: keep one global model, but compute the segment distribution **relative to each client's own customer base**. This fixes the identical-numbers problem, keeps segments comparable across clients, and adds no new models to maintain. **Option B** (one model per client) was considered and rejected — details below.

---

## 1. The problem

```
  Before (what the screenshot showed):
  
  ┌─────────────────────┐        ┌─────────────────────┐
  │ Clicks report       │        │ PNP report          │
  │ Customer Segmentation│        │ Customer Segmentation│
  │                     │        │                     │
  │ 5,288,205 customers │        │ 5,288,205 customers │  ← identical
  │ Champions: 8.2%     │        │ Champions: 8.2%     │  ← identical
  │ drive 44.5% revenue │        │ drive 44.5% revenue │  ← identical
  └─────────────────────┘        └─────────────────────┘
  
  Both reports were reading marts.mart_cluster_output globally.
  No destination filter. Same numbers for every client, every time.
```

**Root cause**

| File | What it does | The problem |
|------|--------------|-------------|
| `sql/03_ml/train_model.sql` | Trains K-means on `analytics.int_rfm_scores` | No client filter — one global model |
| `sql/03_ml/predict_and_name.sql` | One row per customer in `marts.mart_cluster_output` with a single `segment_name` | No destination column — a customer has one label, regardless of where they shop |
| `scripts/generate_report_v3.py` | Summarised the table with no WHERE clause | Same mix returned for every client |

---

## 2. Options considered

### Option A — Global model, per-client mix *(adopted)*

Keep the existing global K-means. Add a new mart that joins each client's customer base to the global segment labels and computes the distribution **for that client's customers only**.

```
  Global model                    Per-client mix
  ──────────────                 ───────────────
  Train once on 5.8M customers   Join mart_cluster_output
  Champions = FNB-wide top 8%     to int_customer_category_spend
  Same segment definitions        GROUP BY DESTINATION, CATEGORY_TWO, segment_name
  for every report                Different % for every client
```

A "Champion" still means the same thing — top spender across all of FNB. But the **share** of Champions changes per client: Clicks might be 11% Champions, Pick n Pay 6%, a fuel station 3%.

### Option B — One K-means per client *(considered, rejected)*

Train a separate K-means model for each destination. Each client gets its own 5 clusters. A "Champion" at Clicks means something different from a "Champion" at Pick n Pay.

---

## 3. Why we picked Option A

| Dimension | Option A | Option B |
|-----------|----------|----------|
| Number of models to train | **1** | ~80 (one per destination) |
| Number of models to monitor and retrain | **1** | ~80 |
| Training cost | Same as today | ~80× |
| Handles low-volume clients | **Yes** — just skip them | No — small clients fail or get unstable clusters |
| Segments comparable across clients | **Yes** — Champion = Champion everywhere | No — each client has its own definition |
| Cross-client storytelling ("Our Champions over-index at Adidas") | **Yes** | Very hard — the word "Champion" changes meaning |
| Time to implement | **~1 day** | ~2 weeks |
| Implementation blast radius | One new mart, one ~20-line script change | Rewrite train/predict SQL, add model-per-client orchestration, rebuild all downstream marts |
| When a new client is added | Works immediately | Needs a new model trained |

### Pros of Option B (that we'd be giving up)

- **Bespoke definitions.** A pharmacy's "Champion" could be defined entirely by pharmacy-category behaviour, not by overall FNB spend. Useful if a client wants ML segmentation purely on their own customer base.
- **Tighter clusters for specialist clients.** A client whose customers all look similar on FNB-wide features (e.g. a funeral-services provider) might get meaningless global segments. A bespoke model would find real separation.

### Cons of Option B (why we're not doing it)

- **Segments stop being comparable.** Saying "Adidas Champions spend 3× the Pick n Pay Champions" becomes incoherent — the two words don't refer to the same construct.
- **Low-volume destinations break.** K-means on 2,000 customers with 9 features produces unstable, reshuffling clusters. We'd need a minimum-customer threshold and a fallback path — that fallback is Option A anyway, so we'd be building both.
- **Retraining cost multiplies.** Every monthly refresh becomes ~80 model fits instead of 1. At scale, that's a real BigQuery bill.
- **Operational surface area.** Each model needs its own evaluation, drift monitoring, and "is this still healthy" check. One team member can't realistically monitor 80 models by eye.

If a specific client asks for a bespoke model later (e.g. "segment Clicks customers using only their Clicks behaviour"), we can build that as a **one-off** on top of Option A without changing the core pipeline.

---

## 4. What changed in the code

### New

- `sql/04_marts/mart_client_segment_mix.sql` — per-client × category segment distribution. Grain: `DESTINATION × CATEGORY_TWO × segment_name`.

### Modified

- `scripts/run.sh` — Step 4 now builds `mart_client_segment_mix` before `mart_audience_catalog`.
- `scripts/generate_report_v3.py` — the `revenue` query (segment mix for the report) now pulls from `mart_client_segment_mix` filtered to the pitched client, with fallback to the FNB-wide mix for low-volume clients below the 1,000-customer threshold.

### Columns on the new mart

| Column | Meaning |
|--------|---------|
| `DESTINATION` | Merchant (e.g. CLICKS, PICK N PAY) |
| `CATEGORY_TWO` | Category (e.g. Pharmacies and Wellbeing, Groceries) |
| `segment_name` | Global segment label (Champions, Loyal High Value, …) |
| `segment_customers` | Customers in this client × category × segment |
| `segment_spend` | Spend from this segment at this client in this category |
| `client_total_customers` | Denominator for customer % |
| `client_total_spend` | Denominator for spend % |
| `pct_of_client_customers` | **The number to show in the report** — what % of this client's customers are in this segment |
| `pct_of_client_spend` | What % of this client's spend comes from this segment |
| `fnb_pct_of_customers` | FNB-wide benchmark for comparison |
| `index_vs_fnb` | 100 = same as FNB-wide; >100 = over-indexed at this client |

### How to run

```bash
# From the sandbox machine after pulling this branch:
bash scripts/run.sh sandbox 4      # rebuilds marts including mart_client_segment_mix
bash scripts/run.sh production 4   # same for production

# Verify:
#   SELECT DESTINATION, CATEGORY_TWO, segment_name,
#          segment_customers, pct_of_client_customers, index_vs_fnb
#   FROM marts.mart_client_segment_mix
#   WHERE DESTINATION IN ('CLICKS', 'PICK N PAY')
#     AND CATEGORY_TWO IN ('Pharmacies and Wellbeing', 'Groceries')
#   ORDER BY DESTINATION, CATEGORY_TWO, pct_of_client_customers DESC;
```

---

## 5. Impact on the Audiences tab

The dashboard's **Audiences** tab (backed by `marts.mart_audience_catalog` and `marts.mart_audience_members`) is **unaffected** by Option A.

```
  Audiences tab sources                        Option A adds
  ─────────────────────                       ──────────────
  mart_audience_catalog  ──── unchanged ───   mart_client_segment_mix
  mart_audience_members  ──── unchanged ───   (new, parallel table)
```

Why it's unaffected:

- The "Behavioral" audiences on that tab (B01 *High value champions*, B02 *Loyal high value*, etc.) are defined by `segment_name = 'Champions' | 'Loyal High Value'` from `mart_cluster_output`. That's the **global label**, which Option A keeps exactly as-is.
- Demographic, Lifestyle, Geographic, Seasonal, and Cross-category audiences don't touch the segmentation model at all.

What Option A **enables** on the Audiences tab (future work, not done here):

- We can add per-client audiences like "Clicks Champions" by joining `mart_audience_members` to `mart_client_segment_mix`. That would let an advertiser pick "customers who are Champions *at this specific merchant*" rather than only FNB-wide Champions. If you want this next, it's one extra UNION block in `mart_audience_catalog.sql`.

---

## 6. Impact on the Customer Segments dashboard page

The Streamlit **Customer Segments** page (`dashboards/app.py`) still reads from `mart_cluster_profiles` and `mart_cluster_summary`, which describe segment **definitions** (Champion = avg spend R X, avg txns Y). Those definitions are unchanged, so the page keeps working with no edit.

If we want the page to show the *selected client's* mix instead of the FNB-wide mix, we'd add a filter that reads `mart_client_segment_mix` when a client is selected in the sidebar. That's a small follow-up, not required for this fix.

---

## 7. Scope and limits

| What this change does | What it does not do |
|-----------------------|---------------------|
| Gives each client a different segment distribution in pitch reports | Change what a "Champion" means |
| Adds an over-/under-index vs FNB-wide benchmark | Train new ML models |
| Covers every destination above 1,000 customers in a category | Cover clients below 1,000 customers (they fall back to the global mix) |
| Reuses the existing K-means output | Address bespoke per-client model requests (build those as one-offs on top) |

---

## 8. Reference — files touched

| File | Change |
|------|--------|
| `sql/04_marts/mart_client_segment_mix.sql` | New — per-client segment distribution mart |
| `scripts/run.sh` | Added mart to Step 4 build and validation counts |
| `scripts/generate_report_v3.py` | Swapped global `revenue` query for per-client mart, with fallback |
