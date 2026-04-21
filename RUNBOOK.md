# RUNBOOK — Per-Client Segmentation + Audience Marketplace Fix

**What this does:** adds per-client segment distribution, adds a per-client audience overlap table, and regenerates the HTML dashboard so it stops showing identical numbers across clients.

**What this does NOT do:** retrain any ML models, reprocess the 2.2B-row staging table, or touch anything in Steps 1–3 of the pipeline.

**Expected cost:** under R10 for the marts. Views cost R0.

---

## 1 · Pull the code

```bash
cd ~/fnb-nav-pipeline
git pull
```

## 2 · Build the two new marts (Step 4)

```bash
bash scripts/run.sh sandbox 4
```

This creates/refreshes:
- `marts.mart_client_segment_mix` — per-client × category segment distribution
- `marts.mart_audience_client_overlap` — per-client audience overlap (pre-aggregated for cheap dashboard reads)

Alongside the existing 10 marts (unchanged). **Expected cost: ~R10.**

## 3 · Refresh the Looker views (free)

```bash
bash scripts/run.sh sandbox 6
```

Creates/replaces:
- `marts.v_client_segment_mix`
- `marts.v_audience_catalog`
- `marts.v_audience_client_overlap`

Alongside the existing 19 views. **Cost: R0** — views don't store data.

## 4 · Verify the new objects landed

```bash
bq ls fmn-sandbox:marts | grep -E "mart_client_segment_mix|mart_audience_client_overlap|v_client_segment_mix|v_audience_catalog|v_audience_client_overlap"
```

You should see **5 rows**. If any are missing, re-run the step that creates them.

Quick sanity check — Clicks vs Pick n Pay should now show different numbers:

```bash
bq query --use_legacy_sql=false --project_id=fmn-sandbox "
SELECT DESTINATION, CATEGORY_TWO, segment_name,
       pct_of_client_customers, pct_of_client_spend, index_vs_fnb
FROM \`fmn-sandbox.marts.mart_client_segment_mix\`
WHERE DESTINATION IN ('CLICKS', 'PICK N PAY')
  AND CATEGORY_TWO IN ('Pharmacies and Wellbeing', 'Groceries')
ORDER BY DESTINATION, CATEGORY_TWO, pct_of_client_customers DESC;"
```

## 5 · Regenerate the HTML dashboard

```bash
python3 scripts/generate_dashboard.py
```

This queries BigQuery and writes `nav_dashboard.html` + `nav_dashboard_cache.json` in the current directory. Takes a minute or two.

After this is done once, for cosmetic tweaks you can regenerate from cache without touching BigQuery:

```bash
python3 scripts/generate_dashboard.py --cached
```

Open `nav_dashboard.html` in your browser. Check:
- **Overview page:** segment pie + revenue bar now say "— {client} in {category}" when a client is selected in the filters.
- **Audiences page:** header now says "FNB-wide · not filtered by client" (honest labelling), and there's a new "Top audiences among this client's customers" table at the bottom that reacts to the Client + Category filters.
- Switch Client between e.g. Clicks and Pick n Pay — the Overview segment numbers change. This is the fix.

## 6 · Generate the Looker Studio "everything" dashboard URL

```bash
python3 scripts/looker_generator.py --dashboard robust
```

Prints a Linking-API URL. Open it, Looker opens with all 10 data sources pre-filled for the one robust dashboard. Follow `docs/looker_build_guide_v2.html` from the "Global filters" section onwards.

## 7 · Regenerate the client pitch reports (optional)

If you also want the HTML/PDF pitch reports to show per-client segmentation instead of the FNB-wide mix:

```bash
python3 scripts/generate_report_v3.py --client "Clicks" --category "Pharmacies and Wellbeing"
python3 scripts/generate_report_v3.py --client "Pick n Pay" --category "Groceries"
```

Each report will now show different segment numbers. Already updated to use `mart_client_segment_mix`.

---

## DO NOT RUN

These all retrigger heavy work and cost money:

| Command | Why to avoid |
|---|---|
| `bash scripts/run.sh` | Runs **everything** including ML retraining |
| `bash scripts/run.sh sandbox` | Same — no step arg means all steps |
| `bash scripts/run.sh sandbox 1` | 2.2B-row staging rebuild |
| `bash scripts/run.sh sandbox 2` | Full intermediate rebuild |
| `bash scripts/run.sh sandbox 3` | **Retrains K-means + churn model** — several rand |

## If anything breaks

| Symptom | Fix |
|---|---|
| `mart_client_segment_mix` not found | Re-run Step 2 above |
| View creation fails with "table not found" | You ran Step 3 before Step 2 — run them in order |
| HTML dashboard still shows identical numbers across clients | You didn't regenerate after Step 5, or you opened the cached file. Re-run without `--cached`. |
| HTML dashboard shows empty overlap table | Selected client × category is below the 1,000-customer threshold — pick a bigger client or a different category |
| BigQuery bill spike | Someone ran `run.sh` without a step arg. Check with `gcloud billing`. |

---

*Last updated: April 2026 — see `docs/per_client_segmentation.md` for the design rationale.*
