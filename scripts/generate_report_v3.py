#!/usr/bin/env python3
"""
generate_report_v3.py
---
Comprehensive HTML analytics report covering:
  1. What was asked at the meeting (client pitch, RFM, segments)
  2. Everything Prosper added after (churn, CLV, momentum, etc.)

Usage:
  python scripts/generate_report_v3.py
  python scripts/generate_report_v3.py --client Adidas --category "Clothing & Apparel"
  python scripts/generate_report_v3.py --client "Pick n Pay" --category Groceries
  BQ_PROJECT=fmn-production python scripts/generate_report_v3.py

Output: nav_report.html + nav_report.pdf (if playwright installed)
"""

import os, json, argparse
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('--client', '-c', default=None, help='Client for pitch section')
parser.add_argument('--category', '-C', default=None, help='Category for pitch section')
parser.add_argument('--top-competitors', '-n', type=int, default=8, help='Competitors to show')
args = parser.parse_args()

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
bq = bigquery.Client(project=PROJECT, location='africa-south1')
HTML_OUT = 'nav_report.html'
PDF_OUT = 'nav_report.pdf'
N_COMP = args.top_competitors

def q(sql):
    return bq.query(sql).to_dataframe()

def safe(sql, fb=None):
    try:
        df = q(sql)
        return df if not df.empty else fb
    except Exception as e:
        print(f'    ⚠ {e}')
        return fb

def R(v):
    if v is None or pd.isna(v): return 'N/A'
    v = float(v)
    if abs(v) >= 1e9: return f'R{v/1e9:.1f}B'
    if abs(v) >= 1e6: return f'R{v/1e6:.1f}M'
    if abs(v) >= 1e3: return f'R{v/1e3:.0f}k'
    return f'R{v:,.0f}'

def J(df, col):
    if df is None: return '[]'
    return json.dumps([str(x) for x in df[col]])

def JN(df, col):
    if df is None: return '[]'
    return json.dumps([round(float(x),2) if pd.notna(x) else 0 for x in df[col]])

def JM(df, col):
    if df is None: return '[]'
    return json.dumps([round(float(x)/1e6,1) if pd.notna(x) else 0 for x in df[col]])

# ---
# AUTO-DETECT CLIENT IF NOT SPECIFIED
# ---
if args.category is None:
    print('  auto-detecting top category...')
    tc = safe(f"SELECT CATEGORY_TWO FROM `{PROJECT}.marts.mart_destination_benchmarks` GROUP BY 1 ORDER BY SUM(total_spend) DESC LIMIT 1")
    CATEGORY = tc.iloc[0]['CATEGORY_TWO'] if tc is not None else 'Clothing & Apparel'
else:
    CATEGORY = args.category

if args.client is None:
    print(f'  auto-detecting top client in {CATEGORY}...')
    td = safe(f"SELECT DESTINATION FROM `{PROJECT}.marts.mart_destination_benchmarks` WHERE CATEGORY_TWO='{CATEGORY}' ORDER BY total_spend DESC LIMIT 1")
    CLIENT = td.iloc[0]['DESTINATION'] if td is not None else 'Adidas'
else:
    CLIENT = args.client

print(f'\n  Category: {CATEGORY}')
print(f'  Client:   {CLIENT}\n')

# ---
# PULL ALL DATA
# ---
print(f'Pulling data from {PROJECT}...')

# Scale
print('  pipeline scale')
ov = safe(f"""
    SELECT 'txns' AS m, COUNT(*) AS v FROM `{PROJECT}.staging.stg_transactions`
    UNION ALL SELECT 'custs', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
    UNION ALL SELECT 'segs', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
    UNION ALL SELECT 'churn', COUNT(*) FROM `{PROJECT}.marts.mart_churn_risk`
    UNION ALL SELECT 'dests', COUNT(DISTINCT DESTINATION) FROM `{PROJECT}.marts.mart_destination_benchmarks`
""")
OV = dict(zip(ov['m'], ov['v'])) if ov is not None else {}

# ─── CLIENT PITCH DATA ────────────────────────────────────────
print(f'  client: {CLIENT} in {CATEGORY}')

# Client KPIs
client_kpi = safe(f"""
    SELECT * FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE CATEGORY_TWO='{CATEGORY}' AND DESTINATION='{CLIENT}'
""")

# Competitors
competitors = safe(f"""
    SELECT * FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE CATEGORY_TWO='{CATEGORY}'
    ORDER BY total_spend DESC LIMIT {N_COMP + 1}
""")

# Share of wallet
sow = safe(f"""
    SELECT
        CASE
            WHEN share_of_wallet_pct >= 80 THEN '80-100% (Loyalist)'
            WHEN share_of_wallet_pct >= 50 THEN '50-80% (Primary)'
            WHEN share_of_wallet_pct >= 20 THEN '20-50% (Secondary)'
            ELSE '1-20% (Occasional)'
        END AS band,
        COUNT(DISTINCT UNIQUE_ID) AS customers,
        ROUND(SUM(dest_spend), 0) AS spend
    FROM `{PROJECT}.analytics.int_customer_category_spend`
    WHERE CATEGORY_TWO='{CATEGORY}' AND DESTINATION='{CLIENT}'
    GROUP BY band ORDER BY band
""")

# Monthly trend
cat_trend = safe(f"""
    SELECT CAST(month AS STRING) AS m, SUM(total_spend) AS spend
    FROM `{PROJECT}.marts.mart_monthly_trends`
    WHERE CATEGORY_TWO='{CATEGORY}'
    GROUP BY month ORDER BY month
""")
client_trend = safe(f"""
    SELECT CAST(month AS STRING) AS m, SUM(total_spend) AS spend
    FROM `{PROJECT}.marts.mart_monthly_trends`
    WHERE CATEGORY_TWO='{CATEGORY}' AND DESTINATION='{CLIENT}'
    GROUP BY month ORDER BY month
""")

# Client demographics
client_demo = safe(f"""
    SELECT gender_label, age_group, income_group,
           SUM(customers) AS customers, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE CATEGORY_TWO='{CATEGORY}'
    GROUP BY gender_label, age_group, income_group
""")
client_age = safe(f"""
    SELECT age_group, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE CATEGORY_TWO='{CATEGORY}' AND age_group IS NOT NULL
    GROUP BY age_group ORDER BY age_group
""")
client_gender = safe(f"""
    SELECT gender_label, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE CATEGORY_TWO='{CATEGORY}' AND gender_label IS NOT NULL AND gender_label!='Unknown'
    GROUP BY gender_label
""")
client_income = safe(f"""
    SELECT income_group, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE CATEGORY_TWO='{CATEGORY}' AND income_group IS NOT NULL AND income_group!='Unknown'
    GROUP BY income_group ORDER BY spend DESC
""")

# Client geo
client_geo = safe(f"""
    SELECT PROVINCE, SUM(total_spend) AS spend, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_geo_summary`
    WHERE CATEGORY_TWO='{CATEGORY}' AND PROVINCE IS NOT NULL
    GROUP BY PROVINCE ORDER BY spend DESC LIMIT 10
""")

# ─── SEGMENT DATA ─────────────────────────────────────────────
# profiles/summary describe segment DEFINITIONS (global — same meaning for every client).
# revenue is the per-client segment MIX — pulled from mart_client_segment_mix so that
# different clients show different distributions instead of the FNB-wide numbers.
# Falls back to the global mix if the client × category isn't in the mart (low volume).
print('  segments')
profiles = safe(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_profiles` ORDER BY avg_total_spend DESC")
summary = safe(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_summary` ORDER BY avg_total_spend DESC")
revenue = safe(f"""
    SELECT segment_name,
        pct_of_client_customers AS pct_cust,
        pct_of_client_spend     AS pct_rev
    FROM `{PROJECT}.marts.mart_client_segment_mix`
    WHERE DESTINATION = '{CLIENT}' AND CATEGORY_TWO = '{CATEGORY}'
    ORDER BY pct_rev DESC
""")
if revenue is None:
    print(f'  ⚠ no per-client segment mix for {CLIENT} × {CATEGORY} — falling back to FNB-wide')
    revenue = safe(f"""
        SELECT segment_name,
            ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct_cust,
            ROUND(SUM(val_trns)*100.0/SUM(SUM(val_trns)) OVER(),1) AS pct_rev
        FROM `{PROJECT}.marts.mart_cluster_output` GROUP BY 1 ORDER BY pct_rev DESC
    """)

# ─── ML VALIDATION ────────────────────────────────────────────
print('  ML models')
km_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.kmeans_customer_segments`)")
km_train = safe(f"SELECT iteration, ROUND(loss,4) AS loss FROM ML.TRAINING_INFO(MODEL `{PROJECT}.analytics.kmeans_customer_segments`) ORDER BY iteration")
centroids = safe(f"SELECT centroid_id, feature, ROUND(numerical_value,2) AS value FROM ML.CENTROIDS(MODEL `{PROJECT}.analytics.kmeans_customer_segments`) ORDER BY centroid_id, feature")
ch_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.churn_classifier`)")
clv_eval = safe(f"SELECT * FROM ML.EVALUATE(MODEL `{PROJECT}.analytics.clv_predictor`)")

# ─── CHURN ────────────────────────────────────────────────────
print('  churn')
churn = safe(f"""
    SELECT churn_risk_level, COUNT(*) AS custs,
        ROUND(AVG(churn_probability)*100,1) AS avg_prob,
        ROUND(SUM(total_spend),0) AS spend,
        ROUND(AVG(days_since_last),0) AS avg_days
    FROM `{PROJECT}.marts.mart_churn_risk` GROUP BY 1
    ORDER BY CASE churn_risk_level WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
""")
churn_reasons = safe(f"""
    SELECT reason_1, COUNT(*) AS custs, ROUND(AVG(churn_probability)*100,1) AS prob, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_churn_explained` GROUP BY 1 ORDER BY custs DESC LIMIT 8
""")

# ─── MOMENTUM ─────────────────────────────────────────────────
print('  momentum')
momentum = safe(f"""
    SELECT momentum_status, COUNT(*) AS custs, ROUND(AVG(total_spend_12m),0) AS spend,
        ROUND(AVG(spend_change_pct),1) AS chg, ROUND(AVG(urgency_score),1) AS urg
    FROM `{PROJECT}.marts.mart_spend_momentum` GROUP BY 1
    ORDER BY CASE momentum_status WHEN 'Declining' THEN 1 WHEN 'Slowing' THEN 2 WHEN 'Steady' THEN 3 WHEN 'Accelerating' THEN 4 ELSE 5 END
""")

# ─── RETENTION ────────────────────────────────────────────────
print('  retention')
retention = safe(f"""
    SELECT months_since_first, ROUND(AVG(retention_pct),1) AS ret
    FROM `{PROJECT}.marts.mart_cohort_retention`
    WHERE cohort_size>=1000 AND months_since_first BETWEEN 0 AND 12
    GROUP BY 1 ORDER BY 1
""")

# ─── CATEGORIES ───────────────────────────────────────────────
print('  categories')
categories = safe(f"""
    SELECT CATEGORY_TWO, total_customers, ROUND(total_spend,0) AS total_spend,
        growth_pct, avg_churn_pct, health_status, pct_champions, pct_dormant,
        top_destination_name, top_dest_market_share, num_destinations
    FROM `{PROJECT}.marts.mart_category_scorecard` WHERE growth_pct IS NOT NULL
    ORDER BY total_spend DESC LIMIT 25
""")

# ─── PITCH OPPS ───────────────────────────────────────────────
print('  pitch opportunities')
pitches = safe(f"""
    SELECT DESTINATION, CATEGORY_TWO, market_share_pct, customers,
        ROUND(addressable_market,0) AS addr, ROUND(pitch_score,1) AS score,
        recommended_action, penetration_pct, gap_to_leader_pct
    FROM `{PROJECT}.marts.mart_pitch_opportunities` ORDER BY pitch_score DESC LIMIT 20
""")

# ─── AFFINITY ─────────────────────────────────────────────────
print('  affinity')
affinity = safe(f"""
    SELECT category_a, category_b, shared_customers, lift, pct_a_also_shops_b, pct_b_also_shops_a
    FROM `{PROJECT}.marts.mart_category_affinity` WHERE lift>1.2 ORDER BY shared_customers DESC LIMIT 15
""")

# ─── CLV ──────────────────────────────────────────────────────
print('  CLV')
clv = safe(f"""
    SELECT clv_tier, COUNT(*) AS custs, ROUND(AVG(predicted_clv),0) AS avg_clv,
        ROUND(AVG(historical_spend),0) AS avg_hist, ROUND(SUM(predicted_clv),0) AS total_clv
    FROM `{PROJECT}.marts.mart_customer_clv` GROUP BY 1 ORDER BY avg_clv DESC
""")

# ─── PROPENSITY ───────────────────────────────────────────────
print('  propensity')
propensity = safe(f"""
    SELECT segment_name, CATEGORY_TWO, propensity_level, unadopted_customers,
        ROUND(potential_revenue,0) AS pot, ROUND(adoption_rate_pct,1) AS adopt
    FROM `{PROJECT}.marts.mart_category_propensity`
    WHERE propensity_level IN ('Very High','High') ORDER BY pot DESC LIMIT 15
""")

# ─── BEHAVIORAL ───────────────────────────────────────────────
print('  behavioral')
behavioral = safe(f"SELECT * FROM `{PROJECT}.marts.mart_behavioral_summary` ORDER BY avg_txns_per_customer DESC")

# ---
# BUILD HTML HELPERS
# ---
now = datetime.now().strftime('%d %B %Y')

def kpi_card(label, value, color='#2E75B6'):
    return f'<div class="mc" style="border-top-color:{color}"><div class="v">{value}</div><div class="l">{label}</div></div>'

def tbl(headers, rows_data, fmt_map=None):
    """Generic table builder. rows_data is list of dicts or list of lists."""
    h = ''.join(f'<th>{h}</th>' for h in headers)
    body = ''
    for row in rows_data:
        cells = ''
        for i, val in enumerate(row):
            if fmt_map and i in fmt_map:
                val = fmt_map[i](val)
            cells += f'<td>{val}</td>'
        body += f'<tr>{cells}</tr>\n'
    return f'<table><tr>{h}</tr>{body}</table>'

def df_table(df, cols, headers, fmt_cols=None):
    if df is None: return '<p class="na">Data not available — table may still be building</p>'
    fmt_cols = fmt_cols or {}
    rows = []
    for _, r in df.iterrows():
        row = []
        for c in cols:
            v = r[c]
            if c in fmt_cols:
                v = fmt_cols[c](v)
            elif isinstance(v, float) and abs(v) >= 1000:
                v = f'{v:,.0f}'
            row.append(v)
        rows.append(row)
    return tbl(headers, rows)

# Client KPI values
ck = client_kpi.iloc[0] if client_kpi is not None and not client_kpi.empty else None

# Churn headline
ar_c = ar_s = 0
if churn is not None:
    ch_hi = churn[churn['churn_risk_level'].isin(['Critical','High'])]
    ar_c = int(ch_hi['custs'].sum())
    ar_s = ch_hi['spend'].sum()

# Champion headline
champ_line = ''
if revenue is not None:
    c = revenue[revenue['segment_name']=='Champions']
    if not c.empty:
        champ_line = f"Champions are {c.iloc[0]['pct_cust']}% of customers but drive {c.iloc[0]['pct_rev']}% of revenue"

# Competitor table
comp_html = ''
if competitors is not None:
    for _, r in competitors.iterrows():
        is_client = '★' if r['DESTINATION'] == CLIENT else ''
        bold = 'font-weight:700;' if r['DESTINATION'] == CLIENT else ''
        comp_html += f'<tr style="{bold}"><td>{is_client} {r["DESTINATION"]}</td><td>{int(r["customers"]):,}</td><td>{R(r["total_spend"])}</td><td>{r["market_share_pct"]:.1f}%</td><td>{r["penetration_pct"]:.1f}%</td><td>{R(r["avg_txn_value"])}</td><td>{R(r["spend_per_customer"])}</td><td>#{int(r["spend_rank"])}</td></tr>\n'

# SOW table
sow_html = ''
if sow is not None:
    for _, r in sow.iterrows():
        sow_html += f'<tr><td>{r["band"]}</td><td>{int(r["customers"]):,}</td><td>{R(r["spend"])}</td></tr>\n'

# Segment cards
def seg_cards():
    if profiles is None or summary is None: return ''
    colors = {'Champions':'#0f172a','Loyal High Value':'#1e3a5f','Steady Mid-Tier':'#4CAF50','At Risk':'#FF9800','Dormant':'#f44336'}
    html = ''
    for _, p in profiles.iterrows():
        s = summary[summary['segment_name']==p['segment_name']]
        desc = s.iloc[0]['business_description'] if not s.empty else ''
        action = s.iloc[0]['recommended_action'] if not s.empty else ''
        col = colors.get(p['segment_name'], '#607D8B')
        html += f'''<div class="seg" style="border-left:5px solid {col}">
            <div class="seg-h"><h3 style="color:{col};margin:0">{p['segment_name']}</h3><span class="badge">{p['pct_of_total']}%</span></div>
            <p class="seg-d">{desc}</p>
            <div class="seg-g">
                <div class="seg-m"><b>{int(p['customer_count']):,}</b><br><small>Customers</small></div>
                <div class="seg-m"><b>{R(p['avg_total_spend'])}</b><br><small>Avg spend</small></div>
                <div class="seg-m"><b>{p['avg_transactions']:.0f}</b><br><small>Avg txns</small></div>
                <div class="seg-m"><b>{p['avg_recency_days']:.0f}d</b><br><small>Recency</small></div>
                <div class="seg-m"><b>{p['avg_merchants']:.0f}</b><br><small>Merchants</small></div>
                <div class="seg-m"><b>{p['avg_active_months']:.0f}mo</b><br><small>Active</small></div>
            </div>
            <div class="seg-f">Top age: <b>{p['top_age_group']}</b> · Top income: <b>{p['top_income_group']}</b> · Avg age: <b>{p['avg_age']:.0f}</b></div>
            <div class="seg-a">→ {action}</div>
        </div>'''
    return html

# Centroid table with heatmap and friendly names
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

def centroid_tbl():
    if centroids is None: return '<p class="na">Not available</p>'
    piv = centroids.pivot(index='feature', columns='centroid_id', values='value')
    h = '<table><tr><th>Feature</th>' + ''.join(f'<th>C{c}</th>' for c in piv.columns) + '</tr>'
    for feat, row in piv.iterrows():
        friendly = FEATURE_NAMES.get(feat, feat.replace('_', ' ').title())
        vals = [row[c] if pd.notna(row[c]) else 0 for c in piv.columns]
        mx, mn = max(vals), min(vals)
        h += f'<tr><td><b>{friendly}</b></td>'
        for v in vals:
            bg = '#dcfce7' if v==mx else ('#fee2e2' if v==mn else '')
            st = f' style="background:{bg}"' if bg else ''
            h += f'<td{st}>{v:,.1f}</td>'
        h += '</tr>'
    return h + '</table>'

# ML metric values
km_db = f"{km_eval.iloc[0]['davies_bouldin_index']:.4f}" if km_eval is not None else 'N/A'
ch_acc = f"{ch_eval.iloc[0].get('accuracy',0):.3f}" if ch_eval is not None else 'N/A'
ch_f1 = f"{ch_eval.iloc[0].get('f1_score',0):.3f}" if ch_eval is not None else 'N/A'
ch_prec = f"{ch_eval.iloc[0].get('precision',0):.3f}" if ch_eval is not None else 'N/A'
ch_rec = f"{ch_eval.iloc[0].get('recall',0):.3f}" if ch_eval is not None else 'N/A'
clv_r2 = f"{clv_eval.iloc[0].get('r2_score',0):.4f}" if clv_eval is not None else 'N/A'
clv_mae = f"{clv_eval.iloc[0].get('mean_absolute_error',0):,.0f}" if clv_eval is not None else 'N/A'

# Chart JSON
seg_labels=J(profiles,'segment_name'); seg_counts=JN(profiles,'customer_count'); seg_spend=JN(profiles,'avg_total_spend')
rev_labels=J(revenue,'segment_name'); rev_c=JN(revenue,'pct_cust'); rev_r=JN(revenue,'pct_rev')
churn_labels=J(churn,'churn_risk_level'); churn_custs=JN(churn,'custs'); churn_spend_m=JM(churn,'spend')
ret_labels=J(retention,'months_since_first'); ret_vals=JN(retention,'ret')
train_labels=J(km_train,'iteration'); train_loss=JN(km_train,'loss')
cat_trend_labels=J(cat_trend,'m'); cat_trend_spend=JM(cat_trend,'spend')
cli_trend_labels=J(client_trend,'m'); cli_trend_spend=JM(client_trend,'spend')
cage_labels=J(client_age,'age_group'); cage_custs=JN(client_age,'customers')
cgender_labels=J(client_gender,'gender_label'); cgender_custs=JN(client_gender,'customers')
cincome_labels=J(client_income,'income_group'); cincome_spend=JM(client_income,'spend')
cgeo_labels=J(client_geo,'PROVINCE'); cgeo_spend=JM(client_geo,'spend')
sow_labels=J(sow,'band'); sow_custs=JN(sow,'customers')
mom_labels=J(momentum,'momentum_status'); mom_custs=JN(momentum,'custs')
clv_labels=J(clv,'clv_tier'); clv_custs=JN(clv,'custs'); clv_avg=JN(clv,'avg_clv')
beh_labels=J(behavioral,'segment_name')
beh_m=JN(behavioral,'pct_morning'); beh_a=JN(behavioral,'pct_afternoon'); beh_e=JN(behavioral,'pct_evening'); beh_w=JN(behavioral,'pct_weekend')

# Competitor chart
comp_labels = comp_share = comp_pen = comp_spc = '[]'
if competitors is not None:
    comp_labels = json.dumps([str(x) for x in competitors['DESTINATION']])
    comp_share = JN(competitors, 'market_share_pct')
    comp_pen = JN(competitors, 'penetration_pct')
    comp_spc = JN(competitors, 'spend_per_customer')

# Category health table
cat_html = ''
if categories is not None:
    for _, r in categories.iterrows():
        h = r['health_status'].lower()
        g = f"+{r['growth_pct']:.1f}%" if r['growth_pct']>0 else f"{r['growth_pct']:.1f}%"
        cat_html += f'<tr><td><b>{r["CATEGORY_TWO"]}</b></td><td>{int(r["total_customers"]):,}</td><td>{R(r["total_spend"])}</td><td>{g}</td><td>{r["avg_churn_pct"]:.1f}%</td><td>{r["pct_champions"]:.1f}%</td><td>{r["pct_dormant"]:.1f}%</td><td>{r["top_destination_name"]}</td><td><span class="b b-{h}">{r["health_status"]}</span></td></tr>\n'

# Pitch opportunities table
pitch_html = ''
if pitches is not None:
    for _, r in pitches.iterrows():
        a = r['recommended_action'].split(' - ')[0].lower()
        pitch_html += f'<tr><td><b>{r["DESTINATION"]}</b></td><td>{r["CATEGORY_TWO"]}</td><td>{int(r["customers"]):,}</td><td>{r["market_share_pct"]:.1f}%</td><td>{R(r["addr"])}</td><td><b>{r["score"]}</b></td><td><span class="b b-{a}">{r["recommended_action"].split(" - ")[0]}</span></td></tr>\n'

# Affinity table
aff_html = ''
if affinity is not None:
    for _, r in affinity.iterrows():
        aff_html += f'<tr><td>{r["category_a"]}</td><td>{r["category_b"]}</td><td>{int(r["shared_customers"]):,}</td><td><b>{r["lift"]:.1f}x</b></td><td>{r["pct_a_also_shops_b"]:.0f}%</td><td>{r["pct_b_also_shops_a"]:.0f}%</td></tr>\n'

# Momentum table
mom_html = ''
if momentum is not None:
    for _, r in momentum.iterrows():
        s = r['momentum_status'].lower()
        mom_html += f'<tr><td><span class="b b-{s}">{r["momentum_status"]}</span></td><td>{int(r["custs"]):,}</td><td>{R(r["spend"])}</td><td>{r["chg"]:+.1f}%</td><td>{r["urg"]:.1f}</td></tr>\n'

# Churn reasons table
cr_html = ''
if churn_reasons is not None:
    for _, r in churn_reasons.iterrows():
        cr_html += f'<tr><td>{r["reason_1"]}</td><td>{int(r["custs"]):,}</td><td>{r["prob"]:.1f}%</td><td>{R(r["spend"])}</td></tr>\n'

# CLV table
clv_html = ''
if clv is not None:
    for _, r in clv.iterrows():
        clv_html += f'<tr><td><b>{r["clv_tier"]}</b></td><td>{int(r["custs"]):,}</td><td>{R(r["avg_clv"])}</td><td>{R(r["avg_hist"])}</td><td>{R(r["total_clv"])}</td></tr>\n'

# Propensity table
prop_html = ''
if propensity is not None:
    for _, r in propensity.iterrows():
        prop_html += f'<tr><td><b>{r["segment_name"]}</b></td><td>{r["CATEGORY_TWO"]}</td><td>{r["propensity_level"]}</td><td>{int(r["unadopted_customers"]):,}</td><td>{R(r["pot"])}</td><td>{r["adopt"]}%</td></tr>\n'

# Churn detail table
churn_tbl = ''
if churn is not None:
    for _, r in churn.iterrows():
        churn_tbl += f'<tr><td><b>{r["churn_risk_level"]}</b></td><td>{int(r["custs"]):,}</td><td>{r["avg_prob"]:.1f}%</td><td>{R(r["spend"])}</td><td>{r["avg_days"]:.0f}</td></tr>\n'

# Highlight client row color in competitor charts
comp_colors = '[]'
if competitors is not None:
    comp_colors = json.dumps(['#f59e0b' if d==CLIENT else '#1e3a5f' for d in competitors['DESTINATION']])

# ---
# HTML
# ---
print('Building HTML...')

html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NAV Analytics — {CLIENT} in {CATEGORY}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:'DM Sans',sans-serif;background:#fafbfc;color:#1a202c;line-height:1.6}}
.hdr{{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 50%,#1a365d 100%);color:white;padding:50px 40px 40px}}.hdr h1{{font-size:2.5rem;font-weight:700}}.hdr .sub{{opacity:.7;margin-top:4px;font-size:1.05rem}}.hdr .meta{{margin-top:16px;font-size:.82rem;opacity:.5}}
.hdr .kpis{{display:flex;gap:24px;margin-top:30px;flex-wrap:wrap}}.hdr .kpi{{background:rgba(255,255,255,.08);border-radius:10px;padding:16px 24px;border:1px solid rgba(255,255,255,.1)}}.hdr .kpi .v{{font-size:1.6rem;font-weight:700}}.hdr .kpi .l{{font-size:.75rem;opacity:.6;margin-top:2px}}
.nav{{background:#fff;border-bottom:1px solid #e2e8f0;padding:10px 40px;position:sticky;top:0;z-index:100;overflow-x:auto;white-space:nowrap}}.nav a{{display:inline-block;padding:6px 14px;margin-right:2px;border-radius:6px;text-decoration:none;color:#64748b;font-size:.82rem;font-weight:500}}.nav a:hover{{background:#f1f5f9;color:#1e3a5f}}
.ctn{{max-width:1280px;margin:0 auto;padding:28px 20px}}
.sec{{background:#fff;border-radius:14px;padding:36px;margin-bottom:24px;box-shadow:0 1px 3px rgba(0,0,0,.04);border:1px solid #f1f5f9}}.sec h2{{font-size:1.5rem;font-weight:700;color:#0f172a;margin-bottom:4px}}.sec .sd{{color:#64748b;font-size:.92rem;margin-bottom:24px;line-height:1.5}}
.ins{{border-radius:8px;padding:14px 20px;margin:16px 0;font-weight:500;font-size:.95rem}}.ins-y{{background:#fffbeb;border-left:4px solid #f59e0b}}.ins-r{{background:#fef2f2;border-left:4px solid #ef4444}}.ins-g{{background:#f0fdf4;border-left:4px solid #22c55e}}
.cr{{display:grid;grid-template-columns:1fr 1fr;gap:22px;margin:18px 0}}.cb{{position:relative;height:320px}}.cf{{position:relative;height:320px;grid-column:span 2}}@media(max-width:768px){{.cr{{grid-template-columns:1fr}}.cf{{grid-column:span 1}}}}
.mc-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin:18px 0}}.mc{{background:#f8fafc;border-radius:10px;padding:18px;text-align:center;border-top:3px solid #2E75B6}}.mc .v{{font-size:1.4rem;font-weight:700;color:#0f172a}}.mc .l{{font-size:.72rem;color:#94a3b8;margin-top:3px}}
table{{width:100%;border-collapse:collapse;margin:14px 0;font-size:.83rem}}th{{background:#0f172a;color:white;padding:9px 12px;text-align:left;font-weight:600;font-size:.78rem;text-transform:uppercase;letter-spacing:.4px}}td{{padding:8px 12px;border-bottom:1px solid #f1f5f9}}tr:hover{{background:#f8fafc}}
.b{{display:inline-block;padding:2px 9px;border-radius:16px;font-size:.72rem;font-weight:600}}.b-growing,.b-grow,.b-accelerating{{background:#dcfce7;color:#166534}}.b-stable,.b-steady,.b-defend{{background:#dbeafe;color:#1e40af}}.b-slowing,.b-protect,.b-attack{{background:#fef3c7;color:#92400e}}.b-declining,.b-opportunity{{background:#fee2e2;color:#991b1b}}.b-monitor,.b-new{{background:#f1f5f9;color:#475569}}
.seg{{background:#fff;border-radius:10px;padding:20px;margin:14px 0;border:1px solid #f1f5f9}}.seg-h{{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}}.badge{{background:#f1f5f9;padding:3px 10px;border-radius:16px;font-size:.78rem;color:#64748b;font-weight:600}}.seg-d{{color:#64748b;font-size:.88rem;margin-bottom:14px}}.seg-g{{display:grid;grid-template-columns:repeat(auto-fit,minmax(90px,1fr));gap:10px;margin-bottom:12px}}.seg-m{{text-align:center;background:#f8fafc;border-radius:6px;padding:8px}}.seg-m b{{font-size:1rem;color:#0f172a}}.seg-m small{{font-size:.68rem;color:#94a3b8}}.seg-f{{font-size:.8rem;color:#64748b;margin-bottom:8px}}.seg-a{{background:#f0fdf4;border-radius:6px;padding:8px 14px;color:#166534;font-size:.85rem;font-weight:500}}
.meth{{font-family:'JetBrains Mono',monospace;font-size:.78rem;background:#f8fafc;border-radius:6px;padding:14px;border-left:3px solid #94a3b8;margin:14px 0;color:#475569}}
.mm{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:14px;margin:16px 0}}.mm>div{{background:#f8fafc;border-radius:8px;padding:16px;text-align:center}}.mm .v{{font-size:1.2rem;font-weight:700;color:#1e3a5f}}.mm .l{{font-size:.72rem;color:#94a3b8;margin-top:3px}}
.na{{color:#94a3b8;font-style:italic;padding:20px 0}}
.ftr{{text-align:center;padding:36px;color:#94a3b8;font-size:.8rem}}
</style></head><body>

<div class="hdr">
<h1>NAV Analytics Report</h1>
<div class="sub">{CLIENT} in {CATEGORY} — Data & Media Network</div>
<div class="meta">{now} · {PROJECT} · Confidential</div>
<div class="kpis">
{kpi_card('Transactions', f"{OV.get('txns',0):,}")}
{kpi_card('Customers', f"{OV.get('custs',0):,}")}
{kpi_card('Segmented', f"{OV.get('segs',0):,}")}
{kpi_card('Churn scored', f"{OV.get('churn',0):,}")}
{kpi_card('Destinations', f"{OV.get('dests',0):,}")}
</div></div>

<div class="nav">
<a href="#pitch">Client Pitch</a><a href="#bench">Benchmarks</a><a href="#sow">Share of Wallet</a><a href="#trend">Trends</a>
<a href="#cdemo">Demographics</a><a href="#cgeo">Geography</a>
<a href="#segs">Segments</a><a href="#rev">Revenue</a><a href="#ml">ML Models</a>
<a href="#churn">Churn</a><a href="#mom">Momentum</a><a href="#ret">Retention</a>
<a href="#cats">Categories</a><a href="#opps">Opportunities</a><a href="#aff">Cross-Sell</a>
<a href="#clv">CLV</a><a href="#prop">Propensity</a><a href="#beh">Behavioral</a>
</div>

<div class="ctn">

<!-- ═══ SECTION 1: CLIENT PITCH KPIs ═══ -->
<div class="sec" id="pitch">
<h2>{CLIENT} — Pitch Overview</h2>
<p class="sd">Key performance metrics for <b>{CLIENT}</b> within <b>{CATEGORY}</b>. This is what you present to the client in a pitch meeting.</p>
<div class="mc-row">
{kpi_card('Customers', f"{int(ck['customers']):,}" if ck is not None else 'N/A', '#1e3a5f')}
{kpi_card('Total Spend', R(ck['total_spend']) if ck is not None else 'N/A', '#2E75B6')}
{kpi_card('Market Share', f"{ck['market_share_pct']:.1f}%" if ck is not None else 'N/A', '#4CAF50')}
{kpi_card('Penetration', f"{ck['penetration_pct']:.1f}%" if ck is not None else 'N/A', '#FF9800')}
{kpi_card('Avg Transaction', R(ck['avg_txn_value']) if ck is not None else 'N/A', '#9C27B0')}
{kpi_card('Spend/Customer', R(ck['spend_per_customer']) if ck is not None else 'N/A', '#f44336')}
{kpi_card('Rank in Category', f"#{int(ck['spend_rank'])}" if ck is not None else 'N/A', '#607D8B')}
{kpi_card('Avg Share of Wallet', f"{ck['avg_share_of_wallet']:.1f}%" if ck is not None else 'N/A', '#00BCD4')}
</div></div>

<!-- ═══ SECTION 2: BENCHMARKS ═══ -->
<div class="sec" id="bench">
<h2>{CLIENT} vs Competitors</h2>
<p class="sd">Top {N_COMP} players in <b>{CATEGORY}</b> ranked by total spend. {CLIENT} highlighted with ★.</p>
<div class="cr">
<div class="cb"><canvas id="cShare"></canvas></div>
<div class="cb"><canvas id="cSpc"></canvas></div>
</div>
<table><tr><th></th><th>Destination</th><th>Customers</th><th>Total Spend</th><th>Market Share</th><th>Penetration</th><th>Avg Txn</th><th>Spend/Customer</th><th>Rank</th></tr>{comp_html}</table>
</div>

<!-- ═══ SECTION 3: SHARE OF WALLET ═══ -->
<div class="sec" id="sow">
<h2>Share of Wallet — {CLIENT}</h2>
<p class="sd">For each customer who shops at <b>{CLIENT}</b>, what percentage of their <b>{CATEGORY}</b> spend goes there? Loyalists (80%+) are your base. Occasionals (1-20%) are your growth opportunity.</p>
<div class="cr">
<div class="cb"><canvas id="sowChart"></canvas></div>
<div class="cb">
<table><tr><th>Wallet Band</th><th>Customers</th><th>Client Spend</th></tr>{sow_html}</table>
</div></div></div>

<!-- ═══ SECTION 4: TRENDS ═══ -->
<div class="sec" id="trend">
<h2>Monthly Trend — {CLIENT} vs {CATEGORY}</h2>
<p class="sd">The gap between the two lines is competitor spend. A growing gap means {CLIENT} is losing share; a narrowing gap means they're gaining.</p>
<div class="cr"><div class="cf"><canvas id="trendChart"></canvas></div></div>
</div>

<!-- ═══ SECTION 5: CLIENT DEMOGRAPHICS ═══ -->
<div class="sec" id="cdemo">
<h2>Who Shops in {CATEGORY}?</h2>
<p class="sd">Demographic profile of customers in this category. Use for audience targeting and media planning.</p>
<div class="cr">
<div class="cb"><canvas id="cAge"></canvas></div>
<div class="cb"><canvas id="cGender"></canvas></div>
</div>
<div class="cr"><div class="cf"><canvas id="cIncome"></canvas></div></div>
</div>

<!-- ═══ SECTION 6: CLIENT GEO ═══ -->
<div class="sec" id="cgeo">
<h2>Geographic Spend — {CATEGORY}</h2>
<p class="sd">Where is the money? Top 10 provinces by total category spend.</p>
<div class="cr"><div class="cf"><canvas id="cGeo"></canvas></div></div>
</div>

<!-- ═══ SECTION 7: SEGMENTS ═══ -->
<div class="sec" id="segs">
<h2>Customer Segmentation</h2>
<p class="sd">K-means ML model groups {OV.get('segs',0):,} customers into 5 behavioral segments based on 9 RFM features.</p>
<div class="ins ins-y">{champ_line}</div>
<div class="cr"><div class="cb"><canvas id="segPie"></canvas></div><div class="cb"><canvas id="segBar"></canvas></div></div>
{seg_cards()}
</div>

<!-- ═══ SECTION 8: REVENUE ═══ -->
<div class="sec" id="rev">
<h2>Revenue Concentration</h2>
<p class="sd">The Pareto principle: a small group drives most revenue. Losing Champions costs more than losing Dormant.</p>
<div class="cr"><div class="cf"><canvas id="revChart"></canvas></div></div>
</div>

<!-- ═══ SECTION 9: ML MODELS ═══ -->
<div class="sec" id="ml">
<h2>ML Model Validation</h2>
<p class="sd">Three models trained in BigQuery ML. This section proves they produce meaningful results.</p>
<h3 style="margin:20px 0 8px;color:#1e3a5f">K-Means Clustering</h3>
<div class="mm"><div><div class="v">{km_db}</div><div class="l">Davies-Bouldin (< 2.0)</div></div><div><div class="v">5</div><div class="l">Clusters</div></div><div><div class="v">9</div><div class="l">Features</div></div></div>
<div class="meth">Features: val_trns, nr_trns, lst_trns_days, avg_val, active_months, active_destinations, active_nav_categories, NR_TRNS_WEEKEND, NR_TRNS_WEEK · standardize=TRUE</div>
<div class="cr"><div class="cf"><canvas id="trainChart"></canvas></div></div>
<h4 style="margin:16px 0 8px">Cluster Centroids</h4>
{centroid_tbl()}
<h3 style="margin:24px 0 8px;color:#1e3a5f">Churn Classifier</h3>
<div class="mm"><div><div class="v">{ch_acc}</div><div class="l">Accuracy</div></div><div><div class="v">{ch_prec}</div><div class="l">Precision</div></div><div><div class="v">{ch_rec}</div><div class="l">Recall</div></div><div><div class="v">{ch_f1}</div><div class="l">F1</div></div></div>
<div class="meth">LOGISTIC_REG · 15 features · auto_class_weights=TRUE · 9mo observation → 3mo outcome</div>
<h3 style="margin:24px 0 8px;color:#1e3a5f">CLV Predictor</h3>
<div class="mm"><div><div class="v">{clv_r2}</div><div class="l">R² Score</div></div><div><div class="v">{clv_mae}</div><div class="l">MAE (R)</div></div></div>
<div class="meth">LINEAR_REG · 14 features · predictions capped at 99th percentile</div>
</div>

<!-- ═══ SECTION 10: CHURN ═══ -->
<div class="sec" id="churn">
<h2>Churn Risk</h2>
<p class="sd">Every customer scored 0-100% churn probability. The explained mart tells us WHY.</p>
<div class="ins ins-r"><b>{ar_c:,}</b> customers are Critical/High risk = <b>{R(ar_s)}</b> spend at risk. 10% recovery = {R(ar_s*0.1)}</div>
<div class="cr"><div class="cb"><canvas id="churnPie"></canvas></div><div class="cb"><canvas id="churnBar"></canvas></div></div>
<h3 style="margin:16px 0 8px;color:#ef4444">Why customers churn</h3>
<table><tr><th>Primary driver</th><th>Customers</th><th>Avg probability</th><th>Spend at risk</th></tr>{cr_html}</table>
<h3 style="margin:16px 0 8px">Risk level breakdown</h3>
<table><tr><th>Level</th><th>Customers</th><th>Avg churn %</th><th>Spend at risk</th><th>Avg days since last</th></tr>{churn_tbl}</table>
</div>

<!-- ═══ SECTION 11: MOMENTUM ═══ -->
<div class="sec" id="mom">
<h2>Spend Momentum</h2>
<p class="sd">Recent 6 months vs prior 6 months. Declining high-spenders are more urgent than steady low-spenders.</p>
<div class="cr"><div class="cb"><canvas id="momChart"></canvas></div>
<div class="cb"><table><tr><th>Status</th><th>Customers</th><th>Avg 12m spend</th><th>Change</th><th>Urgency</th></tr>{mom_html}</table></div></div>
</div>

<!-- ═══ SECTION 12: RETENTION ═══ -->
<div class="sec" id="ret">
<h2>Cohort Retention</h2>
<p class="sd">Of customers who first transacted in a given month, what % are still active N months later?</p>
<div class="cr"><div class="cf"><canvas id="retChart"></canvas></div></div>
</div>

<!-- ═══ SECTION 13: CATEGORIES ═══ -->
<div class="sec" id="cats">
<h2>Category Portfolio Health</h2>
<p class="sd">Every category: spend, growth, churn risk, segment mix, and market leader.</p>
<table><tr><th>Category</th><th>Customers</th><th>Spend</th><th>Growth</th><th>Churn</th><th>% Champ</th><th>% Dormant</th><th>Leader</th><th>Health</th></tr>{cat_html}</table>
</div>

<!-- ═══ SECTION 14: PITCH OPPORTUNITIES ═══ -->
<div class="sec" id="opps">
<h2>Pitch Opportunities</h2>
<p class="sd">Ranked by composite score: 30% market size, 30% gap to leader, 20% low churn, 20% spend efficiency.</p>
<table><tr><th>Destination</th><th>Category</th><th>Customers</th><th>Share</th><th>Addressable</th><th>Score</th><th>Action</th></tr>{pitch_html}</table>
</div>

<!-- ═══ SECTION 15: CROSS-SELL ═══ -->
<div class="sec" id="aff">
<h2>Cross-Sell: Category Affinity</h2>
<p class="sd">Categories shopped together. Lift > 1 = more likely than random. Use for bundle offers.</p>
<table><tr><th>Category A</th><th>Category B</th><th>Shared</th><th>Lift</th><th>% A→B</th><th>% B→A</th></tr>{aff_html}</table>
</div>

<!-- ═══ SECTION 16: CLV ═══ -->
<div class="sec" id="clv">
<h2>Customer Lifetime Value</h2>
<p class="sd">Predicted 12-month spend. Tiered by quintile.</p>
<div class="cr"><div class="cb"><canvas id="clvPie"></canvas></div><div class="cb"><canvas id="clvBar"></canvas></div></div>
<table><tr><th>Tier</th><th>Customers</th><th>Avg CLV</th><th>Avg Historical</th><th>Total Predicted</th></tr>{clv_html}</table>
</div>

<!-- ═══ SECTION 17: PROPENSITY ═══ -->
<div class="sec" id="prop">
<h2>Category Propensity</h2>
<p class="sd">Which segments are likely to adopt new categories? High propensity + large unadopted pool = campaign opportunity.</p>
<table><tr><th>Segment</th><th>Category</th><th>Propensity</th><th>Unadopted</th><th>Potential Revenue</th><th>Adoption</th></tr>{prop_html}</table>
</div>

<!-- ═══ SECTION 18: BEHAVIORAL ═══ -->
<div class="sec" id="beh">
<h2>Shopping Behavior</h2>
<p class="sd">When do segments shop? How diverse are their habits? Drives campaign timing.</p>
<div class="cr"><div class="cb"><canvas id="behTime"></canvas></div><div class="cb"><canvas id="behWknd"></canvas></div></div>
</div>

</div>

<div class="ftr"><b>FNB NAV Data Platform</b> — Data & Media Network<br>Built by Prosper Sikhwari · {datetime.now().strftime('%B %Y')} · {PROJECT}<br>18 sections · {CLIENT} in {CATEGORY} · {OV.get('txns',0):,} transactions</div>

<script>
Chart.defaults.font.family="'DM Sans',sans-serif";Chart.defaults.plugins.legend.labels.usePointStyle=true;
const C=['#0f172a','#1e3a5f','#2E75B6','#4CAF50','#FF9800','#f44336','#9C27B0','#00BCD4','#607D8B'];
const RC=['#f44336','#FF9800','#fbc02d','#4CAF50','#2196f3'];
const r=v=>{{if(v>=1e6)return'R'+(v/1e6).toFixed(0)+'M';if(v>=1e3)return'R'+(v/1e3).toFixed(0)+'k';return'R'+v}};

// Competitor charts
new Chart('cShare',{{type:'bar',data:{{labels:{comp_labels},datasets:[{{label:'Market share %',data:{comp_share},backgroundColor:{comp_colors},borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Market share in {CATEGORY}',font:{{size:13,weight:600}}}},legend:{{display:false}}}}}}}});
new Chart('cSpc',{{type:'bar',data:{{labels:{comp_labels},datasets:[{{label:'Spend per customer',data:{comp_spc},backgroundColor:{comp_colors},borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Spend per customer',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>r(v)}}}}}}}}}});

// SOW
new Chart('sowChart',{{type:'bar',data:{{labels:{sow_labels},datasets:[{{label:'Customers',data:{sow_custs},backgroundColor:['#0f172a','#1e3a5f','#2E75B6','#94a3b8'],borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Share of wallet distribution',font:{{size:13,weight:600}}}},legend:{{display:false}}}}}}}});

// Trend
new Chart('trendChart',{{type:'line',data:{{labels:{cat_trend_labels},datasets:[
{{label:'{CATEGORY} total',data:{cat_trend_spend},borderColor:'#94a3b8',borderWidth:2,borderDash:[5,5],tension:.3,pointRadius:3}},
{{label:'{CLIENT}',data:{cli_trend_spend},borderColor:'#f59e0b',backgroundColor:'rgba(245,158,11,.1)',borderWidth:3,fill:true,tension:.3,pointRadius:4,pointBackgroundColor:'#f59e0b'}}
]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Monthly spend: {CLIENT} vs {CATEGORY}',font:{{size:13,weight:600}}}}}},scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

// Client demographics
new Chart('cAge',{{type:'bar',data:{{labels:{cage_labels},datasets:[{{data:{cage_custs},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customers by age — {CATEGORY}',font:{{size:13,weight:600}}}},legend:{{display:false}}}}}}}});
new Chart('cGender',{{type:'doughnut',data:{{labels:{cgender_labels},datasets:[{{data:{cgender_custs},backgroundColor:['#1e3a5f','#E91E63','#607D8B'],borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Gender — {CATEGORY}',font:{{size:13,weight:600}}}}}}}}}});
new Chart('cIncome',{{type:'bar',data:{{labels:{cincome_labels},datasets:[{{label:'Spend (R millions)',data:{cincome_spend},backgroundColor:'#2E75B6',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Spend by income group — {CATEGORY}',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});
new Chart('cGeo',{{type:'bar',data:{{labels:{cgeo_labels},datasets:[{{data:{cgeo_spend},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Spend by province — {CATEGORY}',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

// Segments
new Chart('segPie',{{type:'doughnut',data:{{labels:{seg_labels},datasets:[{{data:{seg_counts},backgroundColor:C,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customer distribution',font:{{size:13,weight:600}}}}}}}}}});
new Chart('segBar',{{type:'bar',data:{{labels:{seg_labels},datasets:[{{data:{seg_spend},backgroundColor:C,borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{title:{{display:true,text:'Avg spend per segment',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>r(v)}}}}}}}}}});
new Chart('revChart',{{type:'bar',data:{{labels:{rev_labels},datasets:[{{label:'% customers',data:{rev_c},backgroundColor:'#94a3b8',borderRadius:6}},{{label:'% revenue',data:{rev_r},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Revenue concentration',font:{{size:13,weight:600}}}}}},scales:{{y:{{title:{{display:true,text:'%'}}}}}}}}}});
new Chart('trainChart',{{type:'line',data:{{labels:{train_labels},datasets:[{{label:'Loss',data:{train_loss},borderColor:'#1e3a5f',backgroundColor:'rgba(30,58,95,.1)',borderWidth:3,fill:true,tension:.3,pointRadius:5}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'K-Means training convergence',font:{{size:13,weight:600}}}}}}}}}});

// Churn
new Chart('churnPie',{{type:'doughnut',data:{{labels:{churn_labels},datasets:[{{data:{churn_custs},backgroundColor:RC,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Churn risk distribution',font:{{size:13,weight:600}}}}}}}}}});
new Chart('churnBar',{{type:'bar',data:{{labels:{churn_labels},datasets:[{{data:{churn_spend_m},backgroundColor:RC,borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Spend at risk (R millions)',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

// Momentum
new Chart('momChart',{{type:'doughnut',data:{{labels:{mom_labels},datasets:[{{data:{mom_custs},backgroundColor:['#f44336','#FF9800','#607D8B','#4CAF50','#2196f3'],borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Spend momentum',font:{{size:13,weight:600}}}}}}}}}});

// Retention
new Chart('retChart',{{type:'line',data:{{labels:{ret_labels},datasets:[{{label:'Avg retention %',data:{ret_vals},borderColor:'#1e3a5f',backgroundColor:'rgba(30,58,95,.1)',borderWidth:3,fill:true,tension:.3,pointRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Customer retention over time',font:{{size:13,weight:600}}}}}},scales:{{x:{{title:{{display:true,text:'Months since first txn'}}}},y:{{beginAtZero:true,max:100,title:{{display:true,text:'%'}}}}}}}}}});

// CLV
if(document.getElementById('clvPie')){{
new Chart('clvPie',{{type:'doughnut',data:{{labels:{clv_labels},datasets:[{{data:{clv_custs},backgroundColor:C,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'CLV tiers',font:{{size:13,weight:600}}}}}}}}}});
new Chart('clvBar',{{type:'bar',data:{{labels:{clv_labels},datasets:[{{data:{clv_avg},backgroundColor:'#1e3a5f',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Avg CLV per tier',font:{{size:13,weight:600}}}},legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:v=>r(v)}}}}}}}}}});
}}

// Behavioral
new Chart('behTime',{{type:'bar',data:{{labels:{beh_labels},datasets:[{{label:'Morning',data:{beh_m},backgroundColor:'#FF9800',borderRadius:4}},{{label:'Afternoon',data:{beh_a},backgroundColor:'#1e3a5f',borderRadius:4}},{{label:'Evening',data:{beh_e},backgroundColor:'#4CAF50',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Time-of-day patterns',font:{{size:13,weight:600}}}}}},scales:{{y:{{title:{{display:true,text:'%'}}}}}}}}}});
new Chart('behWknd',{{type:'bar',data:{{labels:{beh_labels},datasets:[{{data:{beh_w},backgroundColor:'#9C27B0',borderRadius:6}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:true,text:'Weekend transaction %',font:{{size:13,weight:600}}}},legend:{{display:false}}}}}}}});
</script></body></html>"""

with open(HTML_OUT, 'w') as f:
    f.write(html)
print(f'\n✓ {HTML_OUT} — 18 sections, {CLIENT} in {CATEGORY}')

# PDF — screenshot the full page then slice into landscape A4 pages
try:
    from playwright.sync_api import sync_playwright
    from PIL import Image
    import time, math

    print('Generating PDF...')
    with sync_playwright() as p:
        br = p.chromium.launch(headless=True)
        pg = br.new_page(viewport={'width': 1400, 'height': 900})
        pg.goto(f'file://{os.path.abspath(HTML_OUT)}', wait_until='networkidle')
        time.sleep(6)
        pg.evaluate("try{document.querySelector('.nav').style.display='none'}catch(e){}")
        time.sleep(1)
        pg.screenshot(path='_report_ss.png', full_page=True)
        br.close()

    img = Image.open('_report_ss.png')
    w, h = img.size
    page_w = 1754  # landscape A4 at 150dpi
    scale = page_w / w
    img = img.resize((page_w, int(h * scale)), Image.LANCZOS)
    w, h = img.size
    page_h = 1240
    margin = 40
    usable = page_h - margin * 2
    pages = []
    for i in range(math.ceil(h / usable)):
        page_img = Image.new('RGB', (page_w, page_h), 'white')
        section = img.crop((0, i * usable, w, min((i + 1) * usable, h)))
        page_img.paste(section, (0, margin))
        pages.append(page_img)
    if pages:
        pages[0].save(PDF_OUT, save_all=True, append_images=pages[1:], resolution=150)
        print(f'✓ {PDF_OUT} ({len(pages)} pages, landscape)')
    os.remove('_report_ss.png')
except ImportError:
    print(f'\nFor PDF: pip install playwright Pillow && playwright install chromium')
except Exception as e:
    print(f'\nPDF failed: {e}')

print(f'\nTo generate for a different client:')
print(f'  python scripts/generate_report_v3.py --client "Nike" --category "Clothing & Apparel"')
print(f'  python scripts/generate_report_v3.py --client "Pick n Pay" --category "Groceries"')
