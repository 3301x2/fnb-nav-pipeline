#!/usr/bin/env python3
"""
generate_report_v2.py
---
Generates a comprehensive analytical report from all mart tables.
Mirrors the depth of the 12 notebooks — not just KPIs,
but full segment profiles, model validation, cohort curves,
churn drivers, affinity analysis, CLV distributions, and
growth opportunity sizing.

Usage:
    python scripts/generate_report_v2.py
    BQ_PROJECT=fmn-production python scripts/generate_report_v2.py

Output: nav_analytics_report.html (+ .pdf if playwright installed)
"""

import os, json, sys
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
LOCATION = 'africa-south1'
client = bigquery.Client(project=PROJECT, location=LOCATION)
HTML_OUT = 'nav_analytics_report.html'
PDF_OUT = 'nav_analytics_report.pdf'

def q(sql):
    return client.query(sql).to_dataframe()

def safe(sql, fallback=None):
    try:
        df = q(sql)
        if df.empty: return fallback
        return df
    except Exception as e:
        print(f'    ⚠ {e}')
        return fallback

def fmt(val):
    if val is None or pd.isna(val): return 'N/A'
    val = float(val)
    if abs(val) >= 1e9: return f'R{val/1e9:.1f}B'
    if abs(val) >= 1e6: return f'R{val/1e6:.1f}M'
    if abs(val) >= 1e3: return f'R{val/1e3:.0f}k'
    return f'R{val:,.0f}'

def jl(df, col):
    """JSON list from column"""
    if df is None: return '[]'
    return json.dumps([str(x) for x in df[col]])

def jn(df, col):
    """JSON numbers from column"""
    if df is None: return '[]'
    return json.dumps([round(float(x), 2) if pd.notna(x) else 0 for x in df[col]])

def jnm(df, col):
    """JSON numbers in millions"""
    if df is None: return '[]'
    return json.dumps([round(float(x)/1e6, 1) if pd.notna(x) else 0 for x in df[col]])

# ---
# PULL ALL DATA
# ---

print(f'Pulling data from {PROJECT}...')

# 1. Pipeline scale
print('  pipeline overview')
overview = safe(f"""
    SELECT 'Transactions' AS m, COUNT(*) AS v FROM `{PROJECT}.staging.stg_transactions`
    UNION ALL SELECT 'Customers', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
    UNION ALL SELECT 'Segmented', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
    UNION ALL SELECT 'Churn scored', COUNT(*) FROM `{PROJECT}.marts.mart_churn_risk`
    UNION ALL SELECT 'Destinations', COUNT(DISTINCT DESTINATION) FROM `{PROJECT}.marts.mart_destination_benchmarks`
""")
ov = dict(zip(overview['m'], overview['v'])) if overview is not None else {}

# 2. Full segment profiles
print('  segment profiles')
profiles = safe(f"""
    SELECT * FROM `{PROJECT}.marts.mart_cluster_profiles` ORDER BY avg_total_spend DESC
""")

# 3. Segment summary with actions
print('  segment summary')
summary = safe(f"""
    SELECT * FROM `{PROJECT}.marts.mart_cluster_summary` ORDER BY avg_total_spend DESC
""")

# 4. Revenue concentration
print('  revenue concentration')
revenue = safe(f"""
    SELECT segment_name,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_customers,
           ROUND(SUM(val_trns) * 100.0 / SUM(SUM(val_trns)) OVER(), 1) AS pct_revenue,
           ROUND(SUM(val_trns), 0) AS total_revenue,
           ROUND(AVG(val_trns), 0) AS avg_spend
    FROM `{PROJECT}.marts.mart_cluster_output`
    GROUP BY segment_name ORDER BY pct_revenue DESC
""")

# 5. ML model evaluation — k-means
print('  kmeans evaluation')
kmeans_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)")
kmeans_training = safe(f"""
    SELECT iteration, ROUND(loss, 4) AS loss
    FROM ML.TRAINING_INFO(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)
    ORDER BY iteration
""")
centroids = safe(f"""
    SELECT centroid_id, feature, ROUND(numerical_value, 2) AS value
    FROM ML.CENTROIDS(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)
    ORDER BY centroid_id, feature
""")

# 6. ML model evaluation — churn
print('  churn evaluation')
churn_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.churn_classifier`)")

# 7. Churn risk distribution
print('  churn risk')
churn = safe(f"""
    SELECT churn_risk_level, COUNT(*) AS customers,
           ROUND(AVG(churn_probability)*100, 1) AS avg_prob,
           ROUND(SUM(total_spend), 0) AS spend_at_risk,
           ROUND(AVG(days_since_last), 0) AS avg_days,
           ROUND(AVG(txns_last_3m), 1) AS avg_recent_txns
    FROM `{PROJECT}.marts.mart_churn_risk`
    GROUP BY churn_risk_level
    ORDER BY CASE churn_risk_level WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
""")

# 8. Churn explained — top reasons
print('  churn explained')
churn_reasons = safe(f"""
    SELECT reason_1, COUNT(*) AS customers,
           ROUND(AVG(churn_probability)*100, 1) AS avg_prob,
           ROUND(SUM(total_spend), 0) AS spend
    FROM `{PROJECT}.marts.mart_churn_explained`
    GROUP BY reason_1 ORDER BY customers DESC LIMIT 8
""")

# 9. Behavioral
print('  behavioral')
behavioral = safe(f"SELECT * FROM `{PROJECT}.marts.mart_behavioral_summary` ORDER BY avg_txns_per_customer DESC")

# 10. Cohort retention
print('  cohort retention')
retention = safe(f"""
    SELECT months_since_first, ROUND(AVG(retention_pct), 1) AS avg_retention
    FROM `{PROJECT}.marts.mart_cohort_retention`
    WHERE cohort_size >= 1000 AND months_since_first BETWEEN 0 AND 12
    GROUP BY months_since_first ORDER BY months_since_first
""")

# 11. Category scorecard
print('  category scorecard')
categories = safe(f"""
    SELECT CATEGORY_TWO, total_customers, ROUND(total_spend, 0) AS total_spend,
           growth_pct, avg_churn_pct, health_status, pct_champions, pct_dormant,
           top_destination_name, top_dest_market_share, num_destinations
    FROM `{PROJECT}.marts.mart_category_scorecard`
    WHERE growth_pct IS NOT NULL
    ORDER BY total_spend DESC LIMIT 25
""")

# 12. Pitch opportunities
print('  pitch opportunities')
pitches = safe(f"""
    SELECT DESTINATION, CATEGORY_TWO, market_share_pct,
           ROUND(addressable_market, 0) AS addressable,
           ROUND(pitch_score, 1) AS score, recommended_action,
           customers, ROUND(spend_per_customer, 0) AS spc,
           gap_to_leader_pct, penetration_pct
    FROM `{PROJECT}.marts.mart_pitch_opportunities`
    ORDER BY pitch_score DESC LIMIT 20
""")

# 13. Category affinity
print('  category affinity')
affinity = safe(f"""
    SELECT category_a, category_b, shared_customers, lift,
           pct_a_also_shops_b, pct_b_also_shops_a
    FROM `{PROJECT}.marts.mart_category_affinity`
    WHERE lift > 1.2 ORDER BY shared_customers DESC LIMIT 15
""")

# 14. Spend momentum
print('  spend momentum')
momentum = safe(f"""
    SELECT momentum_status, COUNT(*) AS customers,
           ROUND(AVG(total_spend_12m), 0) AS avg_spend,
           ROUND(AVG(spend_change_pct), 1) AS avg_change,
           ROUND(AVG(urgency_score), 1) AS avg_urgency
    FROM `{PROJECT}.marts.mart_spend_momentum`
    GROUP BY momentum_status
    ORDER BY CASE momentum_status WHEN 'Declining' THEN 1 WHEN 'Slowing' THEN 2 WHEN 'Steady' THEN 3 WHEN 'Accelerating' THEN 4 ELSE 5 END
""")

# 15. Demographics
print('  demographics')
demo_age = safe(f"""
    SELECT age_group, SUM(customers) AS customers, ROUND(SUM(total_spend), 0) AS spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE age_group IS NOT NULL GROUP BY age_group ORDER BY age_group
""")
demo_gender = safe(f"""
    SELECT gender_label, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE gender_label IS NOT NULL AND gender_label != 'Unknown'
    GROUP BY gender_label
""")
demo_income = safe(f"""
    SELECT income_group, SUM(customers) AS customers, ROUND(SUM(total_spend), 0) AS spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE income_group IS NOT NULL AND income_group != 'Unknown'
    GROUP BY income_group ORDER BY spend DESC
""")

# 16. Geo
print('  geo')
geo = safe(f"""
    SELECT PROVINCE, SUM(total_spend) AS spend, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_geo_summary`
    WHERE PROVINCE IS NOT NULL GROUP BY PROVINCE ORDER BY spend DESC LIMIT 10
""")

# 17. Monthly trends (top category)
print('  trends')
top_cat = safe(f"""
    SELECT CATEGORY_TWO FROM `{PROJECT}.marts.mart_destination_benchmarks`
    GROUP BY CATEGORY_TWO ORDER BY SUM(total_spend) DESC LIMIT 1
""")
top_cat_name = top_cat.iloc[0]['CATEGORY_TWO'] if top_cat is not None else 'N/A'
trends = safe(f"""
    SELECT CAST(month AS STRING) AS m, SUM(total_spend) AS spend
    FROM `{PROJECT}.marts.mart_monthly_trends`
    WHERE CATEGORY_TWO = '{top_cat_name}'
    GROUP BY month ORDER BY month
""")

# 18. CLV
print('  CLV')
clv = safe(f"""
    SELECT clv_tier, COUNT(*) AS customers,
           ROUND(AVG(predicted_clv), 0) AS avg_clv,
           ROUND(AVG(historical_spend), 0) AS avg_hist,
           ROUND(SUM(predicted_clv), 0) AS total_clv
    FROM `{PROJECT}.marts.mart_customer_clv`
    GROUP BY clv_tier ORDER BY avg_clv DESC
""")
clv_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.clv_predictor`)")

# 19. Category propensity
print('  propensity')
propensity = safe(f"""
    SELECT segment_name, CATEGORY_TWO, propensity_level,
           unadopted_customers, ROUND(potential_revenue, 0) AS pot_rev,
           ROUND(adoption_rate_pct, 1) AS adopt_pct
    FROM `{PROJECT}.marts.mart_category_propensity`
    WHERE propensity_level IN ('Very High', 'High')
    ORDER BY pot_rev DESC LIMIT 15
""")

# ---
# BUILD HTML
# ---
print('\nBuilding report...')
now = datetime.now().strftime('%d %B %Y')

# Helper: segment profile cards
def segment_cards():
    if profiles is None or summary is None: return '<p>Data not available</p>'
    html = ''
    seg_colors = {'Champions': '#1a365d', 'Loyal High Value': '#2E75B6', 'Steady Mid-Tier': '#4CAF50', 'At Risk': '#FF9800', 'Dormant': '#f44336'}
    for _, p in profiles.iterrows():
        s = summary[summary['segment_name'] == p['segment_name']]
        desc = s.iloc[0]['business_description'] if not s.empty else ''
        action = s.iloc[0]['recommended_action'] if not s.empty else ''
        color = seg_colors.get(p['segment_name'], '#607D8B')
        html += f'''
        <div class="segment-card" style="border-left: 5px solid {color};">
            <div class="seg-header">
                <h3 style="color:{color}; margin:0;">{p['segment_name']}</h3>
                <span class="seg-pct">{p['pct_of_total']}% of customers</span>
            </div>
            <p class="seg-desc">{desc}</p>
            <div class="seg-metrics">
                <div class="seg-m"><span class="seg-v">{int(p['customer_count']):,}</span><span class="seg-l">Customers</span></div>
                <div class="seg-m"><span class="seg-v">{fmt(p['avg_total_spend'])}</span><span class="seg-l">Avg spend</span></div>
                <div class="seg-m"><span class="seg-v">{p['avg_transactions']:.0f}</span><span class="seg-l">Avg txns</span></div>
                <div class="seg-m"><span class="seg-v">{p['avg_recency_days']:.0f}d</span><span class="seg-l">Recency</span></div>
                <div class="seg-m"><span class="seg-v">{p['avg_merchants']:.0f}</span><span class="seg-l">Merchants</span></div>
                <div class="seg-m"><span class="seg-v">{p['avg_active_months']:.0f}mo</span><span class="seg-l">Active months</span></div>
            </div>
            <div class="seg-demo">
                <span>Top age: <strong>{p['top_age_group']}</strong></span>
                <span>Top income: <strong>{p['top_income_group']}</strong></span>
                <span>Avg age: <strong>{p['avg_age']:.0f}</strong></span>
                <span>Avg income: <strong>{fmt(p['avg_income'])}</strong></span>
            </div>
            <div class="seg-action"><strong>→ {action}</strong></div>
        </div>'''
    return html

# Helper: churn reasons
def churn_reasons_html():
    if churn_reasons is None: return '<p>Data not available</p>'
    html = '<table><tr><th>Primary driver</th><th>Affected customers</th><th>Avg churn probability</th><th>Spend at risk</th></tr>'
    for _, r in churn_reasons.iterrows():
        html += f'<tr><td>{r["reason_1"]}</td><td>{int(r["customers"]):,}</td><td>{r["avg_prob"]:.1f}%</td><td>{fmt(r["spend"])}</td></tr>'
    return html + '</table>'

# friendly feature names for the centroid table
FEATURE_NAMES = {
    'NR_TRNS_WEEK': 'Weekday transactions',
    'NR_TRNS_WEEKEND': 'Weekend transactions',
    'active_destinations': 'Merchants visited',
    'active_months': 'Active months',
    'active_nav_categories': 'Categories shopped',
    'avg_val': 'Avg transaction value (R)',
    'lst_trns_days': 'Days since last purchase',
    'nr_trns': 'Total transactions',
    'val_trns': 'Total spend (R)',
}

# Helper: centroid table
def centroid_html():
    if centroids is None: return '<p>Data not available</p>'
    pivot = centroids.pivot(index='feature', columns='centroid_id', values='value')
    html = '<table><tr><th>Feature</th>'
    for c in pivot.columns:
        html += f'<th>Cluster {c}</th>'
    html += '</tr>'
    for feat, row in pivot.iterrows():
        friendly = FEATURE_NAMES.get(feat, feat.replace('_', ' ').title())
        html += f'<tr><td><strong>{friendly}</strong></td>'
        vals = [row[c] if pd.notna(row[c]) else 0 for c in pivot.columns]
        max_v, min_v = max(vals), min(vals)
        for v in vals:
            bg = '#e8f5e9' if v == max_v else ('#ffebee' if v == min_v else '')
            style = f' style="background:{bg}"' if bg else ''
            html += f'<td{style}>{v:,.1f}</td>'
        html += '</tr>'
    return html + '</table>'

# Helper: generic table
def table(df, cols, headers, fmt_cols=None):
    if df is None: return '<p>Data not available</p>'
    fmt_cols = fmt_cols or {}
    html = '<table><tr>' + ''.join(f'<th>{h}</th>' for h in headers) + '</tr>'
    for _, r in df.iterrows():
        html += '<tr>'
        for c in cols:
            v = r[c]
            if c in fmt_cols:
                v = fmt_cols[c](v)
            elif isinstance(v, float) and abs(v) > 1000:
                v = f'{v:,.0f}'
            html += f'<td>{v}</td>'
        html += '</tr>'
    return html + '</table>'

# ML metrics
km_db = f"{kmeans_eval.iloc[0]['davies_bouldin_index']:.4f}" if kmeans_eval is not None else 'N/A'
km_msd = f"{kmeans_eval.iloc[0]['mean_squared_distance']:.4f}" if kmeans_eval is not None else 'N/A'
ch_acc = f"{churn_eval.iloc[0].get('accuracy', 0):.3f}" if churn_eval is not None else 'N/A'
ch_f1 = f"{churn_eval.iloc[0].get('f1_score', 0):.3f}" if churn_eval is not None else 'N/A'
ch_prec = f"{churn_eval.iloc[0].get('precision', 0):.3f}" if churn_eval is not None else 'N/A'
ch_rec = f"{churn_eval.iloc[0].get('recall', 0):.3f}" if churn_eval is not None else 'N/A'

clv_r2 = f"{clv_eval.iloc[0].get('r2_score', 0):.4f}" if clv_eval is not None else 'N/A'
clv_mae = f"{clv_eval.iloc[0].get('mean_absolute_error', 0):,.0f}" if clv_eval is not None else 'N/A'

# Churn headline
at_risk_cust = at_risk_spend = 0
if churn is not None:
    cr_hi = churn[churn['churn_risk_level'].isin(['Critical', 'High'])]
    at_risk_cust = int(cr_hi['customers'].sum())
    at_risk_spend = cr_hi['spend_at_risk'].sum()

# Champion headline
champ_line = ''
if revenue is not None:
    c = revenue[revenue['segment_name'] == 'Champions']
    if not c.empty:
        champ_line = f"Champions are {c.iloc[0]['pct_customers']}% of customers but drive {c.iloc[0]['pct_revenue']}% of revenue"

# Chart JSON
seg_labels = jl(profiles, 'segment_name')
seg_counts = jn(profiles, 'customer_count')
seg_spend = jn(profiles, 'avg_total_spend')
rev_labels = jl(revenue, 'segment_name')
rev_c = jn(revenue, 'pct_customers')
rev_r = jn(revenue, 'pct_revenue')
churn_labels = jl(churn, 'churn_risk_level')
churn_custs = jn(churn, 'customers')
churn_spend_m = jnm(churn, 'spend_at_risk')
ret_labels = jl(retention, 'months_since_first')
ret_vals = jn(retention, 'avg_retention')
train_labels = jl(kmeans_training, 'iteration')
train_loss = jn(kmeans_training, 'loss')
trend_labels = jl(trends, 'm')
trend_spend = jnm(trends, 'spend')
age_labels = jl(demo_age, 'age_group')
age_custs = jn(demo_age, 'customers')
gender_labels = jl(demo_gender, 'gender_label')
gender_custs = jn(demo_gender, 'customers')
income_labels = jl(demo_income, 'income_group')
income_spend_m = jnm(demo_income, 'spend')
geo_labels = jl(geo, 'PROVINCE')
geo_spend_m = jnm(geo, 'spend')
mom_labels = jl(momentum, 'momentum_status')
mom_custs = jn(momentum, 'customers')
clv_labels = jl(clv, 'clv_tier')
clv_custs = jn(clv, 'customers')
clv_avg = jn(clv, 'avg_clv')
beh_labels = jl(behavioral, 'segment_name')
beh_morning = jn(behavioral, 'pct_morning')
beh_afternoon = jn(behavioral, 'pct_afternoon')
beh_evening = jn(behavioral, 'pct_evening')
beh_weekend = jn(behavioral, 'pct_weekend')

# Category health table
cat_html = ''
if categories is not None:
    for _, r in categories.iterrows():
        h = r['health_status'].lower()
        g = f"+{r['growth_pct']:.1f}%" if r['growth_pct'] > 0 else f"{r['growth_pct']:.1f}%"
        cat_html += f'''<tr><td><strong>{r['CATEGORY_TWO']}</strong></td><td>{int(r['total_customers']):,}</td>
            <td>{fmt(r['total_spend'])}</td><td>{g}</td><td>{r['avg_churn_pct']:.1f}%</td>
            <td>{r['pct_champions']:.1f}%</td><td>{r['pct_dormant']:.1f}%</td>
            <td>{r['top_destination_name']}</td><td><span class="badge b-{h}">{r['health_status']}</span></td></tr>\n'''

# Pitch table
pitch_html = ''
if pitches is not None:
    for _, r in pitches.iterrows():
        a = r['recommended_action'].split(' - ')[0].lower()
        pitch_html += f'''<tr><td><strong>{r['DESTINATION']}</strong></td><td>{r['CATEGORY_TWO']}</td>
            <td>{int(r['customers']):,}</td><td>{r['market_share_pct']:.1f}%</td><td>{r['penetration_pct']:.1f}%</td>
            <td>{fmt(r['addressable'])}</td><td>{r['gap_to_leader_pct']:.1f}%</td>
            <td><strong>{r['score']}</strong></td><td><span class="badge b-{a}">{r['recommended_action'].split(' - ')[0]}</span></td></tr>\n'''

# Affinity table
aff_html = ''
if affinity is not None:
    for _, r in affinity.iterrows():
        aff_html += f'''<tr><td>{r['category_a']}</td><td>{r['category_b']}</td>
            <td>{int(r['shared_customers']):,}</td><td><strong>{r['lift']:.1f}x</strong></td>
            <td>{r['pct_a_also_shops_b']:.0f}%</td><td>{r['pct_b_also_shops_a']:.0f}%</td></tr>\n'''

# Momentum table
mom_html = ''
if momentum is not None:
    for _, r in momentum.iterrows():
        s = r['momentum_status'].lower()
        mom_html += f'''<tr><td><span class="badge b-{s}">{r['momentum_status']}</span></td>
            <td>{int(r['customers']):,}</td><td>{fmt(r['avg_spend'])}</td>
            <td>{r['avg_change']:+.1f}%</td><td>{r['avg_urgency']:.1f}</td></tr>\n'''

# Propensity table
prop_html = ''
if propensity is not None:
    for _, r in propensity.iterrows():
        prop_html += f'''<tr><td><strong>{r['segment_name']}</strong></td><td>{r['CATEGORY_TWO']}</td>
            <td>{r['propensity_level']}</td><td>{int(r['unadopted_customers']):,}</td>
            <td>{fmt(r['pot_rev'])}</td><td>{r['adopt_pct']}%</td></tr>\n'''

# CLV table
clv_html = ''
if clv is not None:
    for _, r in clv.iterrows():
        clv_html += f'''<tr><td><strong>{r['clv_tier']}</strong></td><td>{int(r['customers']):,}</td>
            <td>{fmt(r['avg_clv'])}</td><td>{fmt(r['avg_hist'])}</td><td>{fmt(r['total_clv'])}</td></tr>\n'''

# ---
# HTML TEMPLATE
# ---

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FNB NAV — Analytics Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:'DM Sans',sans-serif; background:#fafbfc; color:#1a202c; line-height:1.6; }}

/* Header */
.hdr {{ background:linear-gradient(135deg, #0f172a 0%, #1e3a5f 50%, #1a365d 100%); color:white; padding:60px 40px 50px; }}
.hdr h1 {{ font-size:2.8rem; font-weight:700; letter-spacing:-0.5px; }}
.hdr .sub {{ font-size:1.15rem; opacity:0.7; margin-top:8px; }}
.hdr .meta {{ margin-top:20px; font-size:0.85rem; opacity:0.5; }}
.hdr .kpis {{ display:flex; gap:30px; margin-top:35px; flex-wrap:wrap; }}
.hdr .kpi {{ background:rgba(255,255,255,0.08); border-radius:12px; padding:20px 28px; border:1px solid rgba(255,255,255,0.1); }}
.hdr .kpi .v {{ font-size:1.8rem; font-weight:700; }}
.hdr .kpi .l {{ font-size:0.8rem; opacity:0.6; margin-top:4px; }}

/* Nav */
.nav {{ background:#fff; border-bottom:1px solid #e2e8f0; padding:12px 40px; position:sticky; top:0; z-index:100; overflow-x:auto; white-space:nowrap; }}
.nav a {{ display:inline-block; padding:8px 16px; margin-right:4px; border-radius:8px; text-decoration:none; color:#64748b; font-size:0.85rem; font-weight:500; transition:all 0.2s; }}
.nav a:hover,.nav a.active {{ background:#f1f5f9; color:#1e3a5f; }}

/* Container */
.ctn {{ max-width:1280px; margin:0 auto; padding:30px 24px; }}

/* Section */
.sec {{ background:#fff; border-radius:16px; padding:40px; margin-bottom:28px; box-shadow:0 1px 3px rgba(0,0,0,0.04); border:1px solid #f1f5f9; }}
.sec h2 {{ font-size:1.6rem; font-weight:700; color:#0f172a; margin-bottom:6px; }}
.sec .sdesc {{ color:#64748b; font-size:0.95rem; margin-bottom:28px; line-height:1.5; }}

/* Insight callout */
.insight {{ background:linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%); border-left:4px solid #f59e0b; border-radius:8px; padding:16px 22px; margin:20px 0; font-weight:500; }}
.insight.danger {{ background:linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%); border-left-color:#ef4444; }}
.insight.success {{ background:linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border-left-color:#22c55e; }}

/* Charts */
.chart-row {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; margin:20px 0; }}
.chart-box {{ position:relative; height:340px; }}
.chart-full {{ position:relative; height:340px; grid-column:span 2; }}
@media(max-width:768px) {{ .chart-row {{ grid-template-columns:1fr; }} .chart-full {{ grid-column:span 1; }} }}

/* Metric cards */
.metrics {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(160px, 1fr)); gap:16px; margin:20px 0; }}
.mc {{ background:#f8fafc; border-radius:12px; padding:20px; text-align:center; border-top:3px solid #2E75B6; }}
.mc .v {{ font-size:1.5rem; font-weight:700; color:#0f172a; }}
.mc .l {{ font-size:0.78rem; color:#94a3b8; margin-top:4px; }}

/* Tables */
table {{ width:100%; border-collapse:collapse; margin:16px 0; font-size:0.85rem; }}
th {{ background:#0f172a; color:white; padding:10px 14px; text-align:left; font-weight:600; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.5px; }}
td {{ padding:9px 14px; border-bottom:1px solid #f1f5f9; }}
tr:hover {{ background:#f8fafc; }}

/* Badges */
.badge {{ display:inline-block; padding:3px 10px; border-radius:20px; font-size:0.75rem; font-weight:600; }}
.b-growing,.b-grow,.b-accelerating {{ background:#dcfce7; color:#166534; }}
.b-stable,.b-steady,.b-defend {{ background:#dbeafe; color:#1e40af; }}
.b-slowing,.b-protect,.b-attack {{ background:#fef3c7; color:#92400e; }}
.b-declining,.b-opportunity {{ background:#fee2e2; color:#991b1b; }}
.b-monitor,.b-new {{ background:#f1f5f9; color:#475569; }}

/* Segment cards */
.segment-card {{ background:#fff; border-radius:12px; padding:24px; margin:16px 0; border:1px solid #f1f5f9; }}
.seg-header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:10px; }}
.seg-pct {{ background:#f1f5f9; padding:4px 12px; border-radius:20px; font-size:0.8rem; color:#64748b; font-weight:600; }}
.seg-desc {{ color:#64748b; font-size:0.9rem; margin-bottom:16px; line-height:1.5; }}
.seg-metrics {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(100px, 1fr)); gap:12px; margin-bottom:14px; }}
.seg-m {{ text-align:center; background:#f8fafc; border-radius:8px; padding:10px; }}
.seg-v {{ display:block; font-size:1.1rem; font-weight:700; color:#0f172a; }}
.seg-l {{ display:block; font-size:0.7rem; color:#94a3b8; margin-top:2px; }}
.seg-demo {{ display:flex; gap:20px; flex-wrap:wrap; font-size:0.82rem; color:#64748b; margin-bottom:10px; }}
.seg-action {{ background:#f0fdf4; border-radius:8px; padding:10px 16px; color:#166534; font-size:0.88rem; }}

/* Model card */
.model-card {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(140px, 1fr)); gap:16px; margin:20px 0; }}
.model-m {{ background:#f8fafc; border-radius:10px; padding:18px; text-align:center; }}
.model-m .v {{ font-size:1.3rem; font-weight:700; color:#1e3a5f; }}
.model-m .l {{ font-size:0.75rem; color:#94a3b8; margin-top:4px; }}

/* Footer */
.ftr {{ text-align:center; padding:40px; color:#94a3b8; font-size:0.82rem; }}
.ftr strong {{ color:#64748b; }}

/* Methodology */
.method {{ font-family:'JetBrains Mono',monospace; font-size:0.8rem; background:#f8fafc; border-radius:8px; padding:16px; border-left:3px solid #94a3b8; margin:16px 0; color:#475569; }}

/* Print / PDF styles */
@media print {{
    body {{ font-size:11pt; -webkit-print-color-adjust:exact; print-color-adjust:exact; }}
    .nav {{ display:none; }}
    .hdr {{ padding:30px 25px 25px; }}
    .hdr h1 {{ font-size:2rem; }}
    .hdr .kpis {{ gap:15px; }}
    .hdr .kpi {{ padding:12px 18px; }}
    .hdr .kpi .v {{ font-size:1.3rem; }}
    .ctn {{ padding:15px; }}
    .sec {{ padding:25px; margin-bottom:18px; page-break-inside:avoid; break-inside:avoid; }}
    .sec h2 {{ font-size:1.3rem; }}
    .chart-row {{ gap:16px; }}
    .chart-box {{ height:280px !important; }}
    .chart-full {{ height:280px !important; }}
    table {{ font-size:9pt; }}
    th {{ padding:7px 10px; font-size:8pt; }}
    td {{ padding:6px 10px; }}
    .segment-card {{ padding:16px; margin:10px 0; }}
    .seg-metrics {{ gap:8px; }}
    .seg-v {{ font-size:0.95rem; }}
    .model-card {{ gap:10px; }}
    .model-m {{ padding:12px; }}
    .model-m .v {{ font-size:1.1rem; }}
    .method {{ font-size:0.7rem; padding:10px; }}
    .ftr {{ padding:20px; }}
}}
</style>
</head>
<body>

<!-- ═══ HEADER ═══ -->
<div class="hdr">
    <h1>FNB NAV Analytics Report</h1>
    <div class="sub">Data & Media Network — Comprehensive Insights</div>
    <div class="meta">{now} · Project: {PROJECT} · Confidential — Internal Use Only</div>
    <div class="kpis">
        <div class="kpi"><div class="v">{ov.get('Transactions',0):,}</div><div class="l">Transactions processed</div></div>
        <div class="kpi"><div class="v">{ov.get('Customers',0):,}</div><div class="l">Customers profiled</div></div>
        <div class="kpi"><div class="v">{ov.get('Segmented',0):,}</div><div class="l">ML-segmented</div></div>
        <div class="kpi"><div class="v">{ov.get('Churn scored',0):,}</div><div class="l">Churn-scored</div></div>
        <div class="kpi"><div class="v">{ov.get('Destinations',0):,}</div><div class="l">Destinations</div></div>
    </div>
</div>

<!-- ═══ NAV ═══ -->
<div class="nav">
    <a href="#segments">Segments</a>
    <a href="#revenue">Revenue</a>
    <a href="#models">ML Models</a>
    <a href="#churn">Churn</a>
    <a href="#momentum">Momentum</a>
    <a href="#retention">Retention</a>
    <a href="#categories">Categories</a>
    <a href="#pitch">Pitch Opps</a>
    <a href="#affinity">Cross-Sell</a>
    <a href="#clv">CLV</a>
    <a href="#propensity">Propensity</a>
    <a href="#demographics">Demographics</a>
    <a href="#geo">Geography</a>
    <a href="#behavioral">Behavioral</a>
    <a href="#trends">Trends</a>
</div>

<div class="ctn">

<!-- ═══ 1. SEGMENTS ═══ -->
<div class="sec" id="segments">
    <h2>Customer Segmentation</h2>
    <p class="sdesc">K-means ML model groups {ov.get('Segmented',0):,} customers into 5 behavioral segments based on 9 RFM features: total spend, transaction count, recency, average value, active months, merchant diversity, category diversity, weekend vs weekday patterns. The algorithm finds natural groupings — no manual rules.</p>
    <div class="insight">{champ_line}</div>
    <div class="chart-row">
        <div class="chart-box"><canvas id="segPie"></canvas></div>
        <div class="chart-box"><canvas id="segBar"></canvas></div>
    </div>
    {segment_cards()}
</div>

<!-- ═══ 2. REVENUE ═══ -->
<div class="sec" id="revenue">
    <h2>Revenue Concentration</h2>
    <p class="sdesc">The Pareto principle in action — a small group of customers drives a disproportionate share of revenue. This has direct implications for retention strategy: losing Champions costs more than losing Dormant customers.</p>
    <div class="chart-row"><div class="chart-full"><canvas id="revChart"></canvas></div></div>
</div>

<!-- ═══ 3. ML MODELS ═══ -->
<div class="sec" id="models">
    <h2>ML Model Validation</h2>
    <p class="sdesc">Three models trained in BigQuery ML (in-warehouse, no data egress). This section validates that the models produce meaningful, well-separated results.</p>

    <h3 style="margin:20px 0 10px; color:#1e3a5f;">K-Means Clustering</h3>
    <div class="model-card">
        <div class="model-m"><div class="v">{km_db}</div><div class="l">Davies-Bouldin (< 2.0 = good)</div></div>
        <div class="model-m"><div class="v">{km_msd}</div><div class="l">Mean squared distance</div></div>
        <div class="model-m"><div class="v">5</div><div class="l">Clusters (k)</div></div>
        <div class="model-m"><div class="v">9</div><div class="l">Input features</div></div>
    </div>
    <div class="method">Features: val_trns, nr_trns, lst_trns_days, avg_val, active_months, active_destinations, active_nav_categories, NR_TRNS_WEEKEND, NR_TRNS_WEEK · standardize_features = TRUE · max_iterations = 20</div>
    <div class="chart-row"><div class="chart-full"><canvas id="trainChart"></canvas></div></div>

    <h4 style="margin:20px 0 10px;">Cluster Centroids</h4>
    <p class="sdesc">The center of each cluster — the average standardized feature values that define each segment. Green = highest, red = lowest.</p>
    {centroid_html()}

    <h3 style="margin:30px 0 10px; color:#1e3a5f;">Churn Classifier</h3>
    <div class="model-card">
        <div class="model-m"><div class="v">{ch_acc}</div><div class="l">Accuracy</div></div>
        <div class="model-m"><div class="v">{ch_prec}</div><div class="l">Precision</div></div>
        <div class="model-m"><div class="v">{ch_rec}</div><div class="l">Recall</div></div>
        <div class="model-m"><div class="v">{ch_f1}</div><div class="l">F1 Score</div></div>
    </div>
    <div class="method">Type: LOGISTIC_REG (BOOSTED_TREE not supported in africa-south1) · 15 features · auto_class_weights = TRUE · Training: 9-month observation → 3-month outcome window · Churned = active in observation, zero transactions in outcome</div>

    <h3 style="margin:30px 0 10px; color:#1e3a5f;">CLV Predictor</h3>
    <div class="model-card">
        <div class="model-m"><div class="v">{clv_r2}</div><div class="l">R² Score</div></div>
        <div class="model-m"><div class="v">{clv_mae}</div><div class="l">Mean Absolute Error (R)</div></div>
    </div>
    <div class="method">Type: LINEAR_REG · 14 features including spend_trend · Label: actual spend in next 6 months · Predictions capped at 99th percentile to prevent outlier extrapolation</div>
</div>

<!-- ═══ 4. CHURN ═══ -->
<div class="sec" id="churn">
    <h2>Churn Risk Analysis</h2>
    <p class="sdesc">Every customer scored with a churn probability (0–100%) based on 15 behavioral and demographic features. The model identifies who is likely to leave and the churn explained mart tells us WHY.</p>
    <div class="insight danger"><strong>{at_risk_cust:,}</strong> customers are Critical or High risk, representing <strong>{fmt(at_risk_spend)}</strong> in historical spend. A 10% re-engagement rate would recover ~{fmt(at_risk_spend * 0.1)}.</div>
    <div class="chart-row">
        <div class="chart-box"><canvas id="churnPie"></canvas></div>
        <div class="chart-box"><canvas id="churnBar"></canvas></div>
    </div>

    <h3 style="margin:20px 0 10px; color:#ef4444;">Why customers churn</h3>
    <p class="sdesc">Top drivers extracted from the churn explained mart — the primary reason each Critical/High risk customer is flagged.</p>
    {churn_reasons_html()}
</div>

<!-- ═══ 5. MOMENTUM ═══ -->
<div class="sec" id="momentum">
    <h2>Spend Momentum</h2>
    <p class="sdesc">Are customers spending more or less than they used to? Compares average monthly spend in the recent 6 months vs prior 6 months. A customer spending R5k/month but trending down 10% monthly is more urgent than one spending R2k but trending up.</p>
    <div class="chart-row">
        <div class="chart-box"><canvas id="momChart"></canvas></div>
        <div class="chart-box">
            <table><tr><th>Status</th><th>Customers</th><th>Avg 12m spend</th><th>Spend change</th><th>Urgency</th></tr>{mom_html}</table>
        </div>
    </div>
</div>

<!-- ═══ 6. RETENTION ═══ -->
<div class="sec" id="retention">
    <h2>Cohort Retention</h2>
    <p class="sdesc">Of customers who first transacted in a given month, what percentage are still active after N months? Averaged across all cohorts with 1,000+ customers. This is the fundamental health metric — are we keeping customers?</p>
    <div class="chart-row"><div class="chart-full"><canvas id="retChart"></canvas></div></div>
</div>

<!-- ═══ 7. CATEGORIES ═══ -->
<div class="sec" id="categories">
    <h2>Category Portfolio Health</h2>
    <p class="sdesc">Bird's-eye view of every category: total spend, growth trend (recent 3m vs prior 3m), churn exposure, segment mix (Champions vs Dormant), and market leader. Use this to identify growing vs declining categories and where churn risk is concentrated.</p>
    <table><tr><th>Category</th><th>Customers</th><th>Total spend</th><th>Growth</th><th>Churn risk</th><th>% Champions</th><th>% Dormant</th><th>Leader</th><th>Health</th></tr>{cat_html}</table>
</div>

<!-- ═══ 8. PITCH ═══ -->
<div class="sec" id="pitch">
    <h2>Pitch Opportunities</h2>
    <p class="sdesc">Ranked by composite score (0–100): 30% market size, 30% gap to leader, 20% low churn risk, 20% spend efficiency. Higher = better opportunity. The addressable market shows how much category spend they don't capture yet.</p>
    <table><tr><th>Destination</th><th>Category</th><th>Customers</th><th>Share</th><th>Penetration</th><th>Addressable</th><th>Gap to leader</th><th>Score</th><th>Action</th></tr>{pitch_html}</table>
</div>

<!-- ═══ 9. AFFINITY ═══ -->
<div class="sec" id="affinity">
    <h2>Cross-Sell: Category Affinity</h2>
    <p class="sdesc">Which categories are shopped together? Lift > 1 means customers who shop category A are more likely to also shop category B than random chance. A lift of 3.2x means they're 3.2 times more likely. Use this for cross-sell campaigns and bundle offers.</p>
    <table><tr><th>Category A</th><th>Category B</th><th>Shared customers</th><th>Lift</th><th>% A also in B</th><th>% B also in A</th></tr>{aff_html}</table>
</div>

<!-- ═══ 10. CLV ═══ -->
<div class="sec" id="clv">
    <h2>Customer Lifetime Value</h2>
    <p class="sdesc">Predicted spend over the next 6 months using a linear regression model trained on 14 behavioral features. Customers are tiered into Platinum through Basic based on predicted CLV quintiles. Predictions capped at the 99th percentile to prevent outlier extrapolation.</p>
    <div class="chart-row">
        <div class="chart-box"><canvas id="clvPie"></canvas></div>
        <div class="chart-box"><canvas id="clvBar"></canvas></div>
    </div>
    <table><tr><th>Tier</th><th>Customers</th><th>Avg predicted CLV</th><th>Avg historical</th><th>Total predicted</th></tr>{clv_html}</table>
</div>

<!-- ═══ 11. PROPENSITY ═══ -->
<div class="sec" id="propensity">
    <h2>Category Propensity</h2>
    <p class="sdesc">For each customer segment, which new categories are they most likely to adopt? Based on what similar customers (same segment, demographics) already shop. High propensity + large unadopted pool = campaign opportunity.</p>
    <table><tr><th>Segment</th><th>Category</th><th>Propensity</th><th>Unadopted customers</th><th>Potential revenue</th><th>Current adoption</th></tr>{prop_html}</table>
</div>

<!-- ═══ 12. DEMOGRAPHICS ═══ -->
<div class="sec" id="demographics">
    <h2>Customer Demographics</h2>
    <p class="sdesc">Age, gender, and income distribution across all categories. Use for audience profiling and media planning.</p>
    <div class="chart-row">
        <div class="chart-box"><canvas id="ageChart"></canvas></div>
        <div class="chart-box"><canvas id="genderChart"></canvas></div>
    </div>
    <div class="chart-row"><div class="chart-full"><canvas id="incomeChart"></canvas></div></div>
</div>

<!-- ═══ 13. GEO ═══ -->
<div class="sec" id="geo">
    <h2>Geographic Distribution</h2>
    <p class="sdesc">Top 10 provinces by total spend across all categories.</p>
    <div class="chart-row"><div class="chart-full"><canvas id="geoChart"></canvas></div></div>
</div>

<!-- ═══ 14. BEHAVIORAL ═══ -->
<div class="sec" id="behavioral">
    <h2>Shopping Behavior by Segment</h2>
    <p class="sdesc">When do different segments shop? How diverse are their habits? This drives campaign timing and channel strategy.</p>
    <div class="chart-row">
        <div class="chart-box"><canvas id="behChart"></canvas></div>
        <div class="chart-box"><canvas id="behWknd"></canvas></div>
    </div>
</div>

<!-- ═══ 15. TRENDS ═══ -->
<div class="sec" id="trends">
    <h2>Monthly Spend Trend</h2>
    <p class="sdesc">Total monthly spend for {top_cat_name} (top category by spend).</p>
    <div class="chart-row"><div class="chart-full"><canvas id="trendChart"></canvas></div></div>
</div>

</div>

<div class="ftr">
    <strong>FNB NAV Data Platform</strong> — Data & Media Network<br>
    Built by Prosper Sikhwari · {datetime.now().strftime('%B %Y')}<br>
    {PROJECT} · 15 analytical sections · {ov.get('Transactions',0):,} transactions processed
</div>

<script>
Chart.defaults.font.family="'DM Sans',sans-serif";
Chart.defaults.plugins.legend.labels.usePointStyle=true;
const C=['#1e3a5f','#2E75B6','#4CAF50','#FF9800','#f44336','#9C27B0','#00BCD4','#607D8B'];
const RC=['#f44336','#FF9800','#fbc02d','#4CAF50','#2196f3'];
const MC=['#f44336','#FF9800','#607D8B','#4CAF50','#2196f3'];
const r=(v)=>{{if(v>=1e6)return'R'+(v/1e6).toFixed(0)+'M';if(v>=1e3)return'R'+(v/1e3).toFixed(0)+'k';return'R'+v;}};

new Chart('segPie',{{type:'doughnut',data:{{labels:{seg_labels},datasets:[{{data:{seg_counts},backgroundColor:C,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customer distribution',font:{{size:14,weight:600}}}}}}}}}});
new Chart('segBar',{{type:'bar',data:{{labels:{seg_labels},datasets:[{{label:'Avg spend',data:{seg_spend},backgroundColor:C,borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Average spend per segment',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:function(v){{return r(v);}}}}}}}}}}}});
new Chart('revChart',{{type:'bar',data:{{labels:{rev_labels},datasets:[{{label:'% customers',data:{rev_c},backgroundColor:'#94a3b8',borderRadius:6}},{{label:'% revenue',data:{rev_r},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Revenue concentration: customers vs revenue share',font:{{size:14,weight:600}}}}}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'%'}}}}}}}}}});
new Chart('trainChart',{{type:'line',data:{{labels:{train_labels},datasets:[{{label:'Training loss',data:{train_loss},borderColor:'#1e3a5f',backgroundColor:'rgba(30,58,95,0.1)',borderWidth:3,fill:true,tension:0.3,pointRadius:5,pointBackgroundColor:'#1e3a5f'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'K-Means training convergence (loss per iteration)',font:{{size:14,weight:600}}}}}}}}}});
new Chart('churnPie',{{type:'doughnut',data:{{labels:{churn_labels},datasets:[{{data:{churn_custs},backgroundColor:RC,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customers by churn risk level',font:{{size:14,weight:600}}}}}}}}}});
new Chart('churnBar',{{type:'bar',data:{{labels:{churn_labels},datasets:[{{label:'Spend at risk (R millions)',data:{churn_spend_m},backgroundColor:RC,borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Spend at risk by level',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:function(v){{return'R'+v+'M';}}}}}}}}}}}});
new Chart('momChart',{{type:'doughnut',data:{{labels:{mom_labels},datasets:[{{data:{mom_custs},backgroundColor:MC,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customer spend momentum',font:{{size:14,weight:600}}}}}}}}}});
new Chart('retChart',{{type:'line',data:{{labels:{ret_labels},datasets:[{{label:'Avg retention %',data:{ret_vals},borderColor:'#1e3a5f',backgroundColor:'rgba(30,58,95,0.1)',borderWidth:3,fill:true,tension:0.3,pointRadius:6,pointBackgroundColor:'#1e3a5f'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customer retention over time (avg across cohorts)',font:{{size:14,weight:600}}}}}},scales:{{x:{{title:{{display:true,text:'Months since first transaction'}}}},y:{{beginAtZero:true,max:100,title:{{display:true,text:'Retention %'}}}}}}}}}});
new Chart('ageChart',{{type:'bar',data:{{labels:{age_labels},datasets:[{{label:'Customers',data:{age_custs},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customers by age group',font:{{size:14,weight:600}}}},legend:{{display:false}}}}}}}});
new Chart('genderChart',{{type:'doughnut',data:{{labels:{gender_labels},datasets:[{{data:{gender_custs},backgroundColor:['#1e3a5f','#E91E63','#607D8B'],borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Gender distribution',font:{{size:14,weight:600}}}}}}}}}});
new Chart('incomeChart',{{type:'bar',data:{{labels:{income_labels},datasets:[{{label:'Spend (R millions)',data:{income_spend_m},backgroundColor:'#2E75B6',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Spend by income group',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:function(v){{return'R'+v+'M';}}}}}}}}}}}});
new Chart('geoChart',{{type:'bar',data:{{labels:{geo_labels},datasets:[{{label:'Spend (R millions)',data:{geo_spend_m},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Spend by province',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:function(v){{return'R'+v+'M';}}}}}}}}}}}});
new Chart('behChart',{{type:'bar',data:{{labels:{beh_labels},datasets:[{{label:'Morning',data:{beh_morning},backgroundColor:'#FF9800',borderRadius:4}},{{label:'Afternoon',data:{beh_afternoon},backgroundColor:'#1e3a5f',borderRadius:4}},{{label:'Evening',data:{beh_evening},backgroundColor:'#4CAF50',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Time-of-day shopping patterns',font:{{size:14,weight:600}}}}}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'%'}}}}}}}}}});
new Chart('behWknd',{{type:'bar',data:{{labels:{beh_labels},datasets:[{{label:'Weekend %',data:{beh_weekend},backgroundColor:'#9C27B0',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Weekend transaction share',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{beginAtZero:true,title:{{display:true,text:'%'}}}}}}}}}});
new Chart('trendChart',{{type:'line',data:{{labels:{trend_labels},datasets:[{{label:'Total spend (R millions)',data:{trend_spend},borderColor:'#1e3a5f',backgroundColor:'rgba(30,58,95,0.1)',borderWidth:3,fill:true,tension:0.3,pointRadius:4,pointBackgroundColor:'#1e3a5f'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Monthly spend — {top_cat_name}',font:{{size:14,weight:600}}}}}},scales:{{y:{{ticks:{{callback:function(v){{return'R'+v+'M';}}}}}}}}}}}});

if(document.getElementById('clvPie')){{
new Chart('clvPie',{{type:'doughnut',data:{{labels:{clv_labels},datasets:[{{data:{clv_custs},backgroundColor:['#0f172a','#1e3a5f','#2E75B6','#4CAF50','#94a3b8'],borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customers by CLV tier',font:{{size:14,weight:600}}}}}}}}}});
new Chart('clvBar',{{type:'bar',data:{{labels:{clv_labels},datasets:[{{label:'Avg predicted CLV',data:{clv_avg},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Average CLV per tier',font:{{size:14,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:function(v){{return r(v);}}}}}}}}}}}});
}}
</script>

</body>
</html>"""

with open(HTML_OUT, 'w') as f:
    f.write(html)
print(f'\n✓ Saved: {HTML_OUT}')
print(f'  15 sections, {ov.get("Transactions",0):,} transactions')

# PDF — screenshot the full page and convert to PDF pages
# this captures exactly what you see in the browser including Chart.js canvases
try:
    from playwright.sync_api import sync_playwright
    from PIL import Image
    import time, math

    print('Generating PDF (screenshot method)...')
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1400, 'height': 900})
        page.goto(f'file://{os.path.abspath(HTML_OUT)}', wait_until='networkidle')
        time.sleep(6)  # let all charts fully render

        # hide the sticky nav for cleaner screenshots
        page.evaluate("document.querySelector('.nav').style.display='none'")
        time.sleep(1)

        # take a full-page screenshot
        screenshot_path = 'report_screenshot.png'
        page.screenshot(path=screenshot_path, full_page=True)
        browser.close()

    # split the long screenshot into landscape A4 pages
    img = Image.open(screenshot_path)
    w, h = img.size

    # landscape A4 at 150 DPI: 1754 x 1240 px
    # scale image to fit width, then slice into pages
    page_w = 1754
    scale = page_w / w
    img = img.resize((page_w, int(h * scale)), Image.LANCZOS)
    w, h = img.size

    page_h = 1240  # landscape A4 height at 150dpi
    margin = 40
    usable_h = page_h - (margin * 2)
    num_pages = math.ceil(h / usable_h)

    pages = []
    for i in range(num_pages):
        # create white A4 page
        page_img = Image.new('RGB', (page_w, page_h), 'white')
        # crop the section from the screenshot
        top = i * usable_h
        bottom = min(top + usable_h, h)
        section = img.crop((0, top, w, bottom))
        # paste onto the page with margin
        page_img.paste(section, (0, margin))
        pages.append(page_img)

    # save as multi-page PDF
    if pages:
        pages[0].save(PDF_OUT, save_all=True, append_images=pages[1:], resolution=150)
        print(f'✓ Saved: {PDF_OUT} ({len(pages)} pages, landscape)')

    # cleanup screenshot
    os.remove(screenshot_path)

except ImportError as e:
    missing = str(e).split("'")[1] if "'" in str(e) else str(e)
    print(f'\nFor PDF: pip install playwright Pillow && playwright install chromium')
    print(f'  (missing: {missing})')
except Exception as e:
    print(f'\nPDF failed: {e}')
    print(f'HTML report is still available: {HTML_OUT}')
