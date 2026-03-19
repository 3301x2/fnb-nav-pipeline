#!/usr/bin/env python3
"""
generate_dashboard.py
═══════════════════════════════════════════════════════════════
Generates a fully interactive HTML dashboard with:
  - 6 tabbed pages (Overview, Client Pitch, Segments, Churn & CLV, Categories, Audiences)
  - Category + Client dropdowns that filter all charts live
  - Internal/External toggle for competitor anonymization
  - Chart.js charts that re-render on filter change
  - All data embedded as JSON — no server needed

Usage:
    python scripts/generate_dashboard.py
    BQ_PROJECT=fmn-production python scripts/generate_dashboard.py

Output: nav_dashboard.html
"""

import os, json, sys
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
bq = bigquery.Client(project=PROJECT, location='africa-south1')
OUT = 'nav_dashboard.html'

def q(sql):
    return bq.query(sql).to_dataframe()

def safe(sql):
    try:
        df = q(sql)
        return df if not df.empty else None
    except Exception as e:
        print(f'  ⚠ {e}')
        return None

def to_json(df):
    if df is None: return '[]'
    return df.to_json(orient='records', date_format='iso')

# ═══════════════════════════════════════════════════════════════
# PULL ALL DATA
# ═══════════════════════════════════════════════════════════════
print(f'Pulling data from {PROJECT}...')

print('  overview')
overview = safe(f"""
    SELECT 'txns' AS k, COUNT(*) AS v FROM `{PROJECT}.staging.stg_transactions`
    UNION ALL SELECT 'custs', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
    UNION ALL SELECT 'segs', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
    UNION ALL SELECT 'churn', COUNT(*) FROM `{PROJECT}.marts.mart_churn_risk`
    UNION ALL SELECT 'dests', COUNT(DISTINCT DESTINATION) FROM `{PROJECT}.marts.mart_destination_benchmarks`
""")

print('  benchmarks (all categories × destinations)')
benchmarks = safe(f"""
    SELECT CATEGORY_TWO, DESTINATION, customers, ROUND(total_spend,0) AS total_spend,
        ROUND(market_share_pct,1) AS market_share_pct, ROUND(penetration_pct,1) AS penetration_pct,
        ROUND(avg_txn_value,0) AS avg_txn_value, ROUND(spend_per_customer,0) AS spend_per_customer,
        ROUND(avg_share_of_wallet,1) AS avg_share_of_wallet, spend_rank, transactions
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
""")

print('  segments')
profiles = safe(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_profiles` ORDER BY avg_total_spend DESC")
summary = safe(f"SELECT * FROM `{PROJECT}.marts.mart_cluster_summary`")

print('  revenue concentration')
revenue = safe(f"""
    SELECT segment_name,
        ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct_cust,
        ROUND(SUM(val_trns)*100.0/SUM(SUM(val_trns)) OVER(),1) AS pct_rev
    FROM `{PROJECT}.marts.mart_cluster_output` GROUP BY 1
""")

print('  churn')
churn = safe(f"""
    SELECT churn_risk_level, COUNT(*) AS custs,
        ROUND(AVG(churn_probability)*100,1) AS avg_prob,
        ROUND(SUM(total_spend),0) AS spend, ROUND(AVG(days_since_last),0) AS avg_days
    FROM `{PROJECT}.marts.mart_churn_risk` GROUP BY 1
    ORDER BY CASE churn_risk_level WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
""")

print('  churn reasons')
churn_reasons = safe(f"""
    SELECT reason_1, COUNT(*) AS custs, ROUND(AVG(churn_probability)*100,1) AS prob, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_churn_explained` GROUP BY 1 ORDER BY custs DESC LIMIT 8
""")

print('  CLV')
clv = safe(f"""
    SELECT clv_tier, COUNT(*) AS custs, ROUND(AVG(predicted_clv),0) AS avg_clv,
        ROUND(AVG(historical_spend),0) AS avg_hist, ROUND(SUM(predicted_clv),0) AS total_clv
    FROM `{PROJECT}.marts.mart_customer_clv` GROUP BY 1 ORDER BY avg_clv DESC
""")

print('  momentum')
momentum = safe(f"""
    SELECT momentum_status, COUNT(*) AS custs, ROUND(AVG(total_spend_12m),0) AS spend,
        ROUND(AVG(spend_change_pct),1) AS chg, ROUND(AVG(urgency_score),1) AS urg
    FROM `{PROJECT}.marts.mart_spend_momentum` GROUP BY 1
    ORDER BY CASE momentum_status WHEN 'Declining' THEN 1 WHEN 'Slowing' THEN 2 WHEN 'Steady' THEN 3 WHEN 'Accelerating' THEN 4 ELSE 5 END
""")

print('  retention')
retention = safe(f"""
    SELECT months_since_first AS month, ROUND(AVG(retention_pct),1) AS ret
    FROM `{PROJECT}.marts.mart_cohort_retention`
    WHERE cohort_size>=1000 AND months_since_first BETWEEN 0 AND 12
    GROUP BY 1 ORDER BY 1
""")

print('  behavioral')
behavioral = safe(f"SELECT * FROM `{PROJECT}.marts.mart_behavioral_summary` ORDER BY avg_txns_per_customer DESC")

print('  categories')
categories = safe(f"""
    SELECT CATEGORY_TWO, total_customers, ROUND(total_spend,0) AS total_spend,
        ROUND(growth_pct,1) AS growth_pct, ROUND(avg_churn_pct,1) AS avg_churn_pct,
        health_status, ROUND(pct_champions,1) AS pct_champions, ROUND(pct_dormant,1) AS pct_dormant,
        top_destination_name, num_destinations
    FROM `{PROJECT}.marts.mart_category_scorecard` WHERE growth_pct IS NOT NULL ORDER BY total_spend DESC
""")

print('  pitch opportunities')
pitches = safe(f"""
    SELECT DESTINATION, CATEGORY_TWO, ROUND(market_share_pct,1) AS market_share_pct, customers,
        ROUND(addressable_market,0) AS addressable, ROUND(pitch_score,1) AS score, recommended_action,
        ROUND(penetration_pct,1) AS penetration_pct, ROUND(gap_to_leader_pct,1) AS gap_to_leader_pct
    FROM `{PROJECT}.marts.mart_pitch_opportunities` ORDER BY pitch_score DESC LIMIT 30
""")

print('  affinity')
affinity = safe(f"""
    SELECT category_a, category_b, shared_customers, ROUND(lift,1) AS lift,
        ROUND(pct_a_also_shops_b,0) AS pct_a_b, ROUND(pct_b_also_shops_a,0) AS pct_b_a
    FROM `{PROJECT}.marts.mart_category_affinity` WHERE lift>1.2 ORDER BY shared_customers DESC LIMIT 20
""")

print('  propensity')
propensity = safe(f"""
    SELECT segment_name, CATEGORY_TWO, propensity_level, unadopted_customers,
        ROUND(potential_revenue,0) AS pot, ROUND(adoption_rate_pct,1) AS adopt
    FROM `{PROJECT}.marts.mart_category_propensity`
    WHERE propensity_level IN ('Very High','High') ORDER BY pot DESC LIMIT 20
""")

print('  audiences')
audiences = safe(f"SELECT * FROM `{PROJECT}.marts.mart_audience_catalog` ORDER BY audience_size DESC")

print('  demographics (by category)')
demo = safe(f"""
    SELECT CATEGORY_TWO, age_group, gender_label, income_group,
        SUM(customers) AS customers, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE age_group IS NOT NULL
    GROUP BY 1,2,3,4
""")

print('  geo (by category)')
geo = safe(f"""
    SELECT CATEGORY_TWO, PROVINCE, SUM(total_spend) AS spend, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_geo_summary` WHERE PROVINCE IS NOT NULL
    GROUP BY 1,2
""")

print('  trends (by category × destination)')
trends = safe(f"""
    SELECT CATEGORY_TWO, DESTINATION, CAST(month AS STRING) AS month, ROUND(SUM(total_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_monthly_trends`
    GROUP BY 1,2,3 ORDER BY 1,2,3
""")

print('  store loyalty')
loyalty = safe(f"""
    SELECT CATEGORY_TWO, DESTINATION, customers, ROUND(avg_loyalty_pct,1) AS avg_loyalty,
        ROUND(pct_loyal_50,1) AS pct_loyal_50, ROUND(pct_loyal_80,1) AS pct_loyal_80,
        band_1_store, band_2_stores, band_3_4_stores, band_5_7_stores, band_8_plus
    FROM `{PROJECT}.marts.mart_store_loyalty` WHERE customers >= 500
""")

print('  store time patterns')
timepatterns = safe(f"""
    SELECT CATEGORY_TWO, DESTINATION,
        ROUND(pct_early_morning,1) AS morning, ROUND(pct_mid_morning,1) AS midmorning,
        ROUND(pct_afternoon,1) AS afternoon, ROUND(pct_late_afternoon,1) AS late_afternoon,
        ROUND(pct_evening,1) AS evening, ROUND(pct_weekend,1) AS weekend,
        ROUND(pct_sunday,1) AS sun, ROUND(pct_monday,1) AS mon, ROUND(pct_tuesday,1) AS tue,
        ROUND(pct_wednesday,1) AS wed, ROUND(pct_thursday,1) AS thu,
        ROUND(pct_friday,1) AS fri, ROUND(pct_saturday,1) AS sat
    FROM `{PROJECT}.marts.mart_store_time_patterns` WHERE total_transactions >= 1000
""")

# ═══════════════════════════════════════════════════════════════
# SERIALIZE
# ═══════════════════════════════════════════════════════════════
print('\nSerializing...')
ov = dict(zip(overview['k'], overview['v'])) if overview is not None else {}

data_json = json.dumps({
    'overview': ov,
    'benchmarks': json.loads(to_json(benchmarks)),
    'profiles': json.loads(to_json(profiles)),
    'summary': json.loads(to_json(summary)),
    'revenue': json.loads(to_json(revenue)),
    'churn': json.loads(to_json(churn)),
    'churn_reasons': json.loads(to_json(churn_reasons)),
    'clv': json.loads(to_json(clv)),
    'momentum': json.loads(to_json(momentum)),
    'retention': json.loads(to_json(retention)),
    'behavioral': json.loads(to_json(behavioral)),
    'categories': json.loads(to_json(categories)),
    'pitches': json.loads(to_json(pitches)),
    'affinity': json.loads(to_json(affinity)),
    'propensity': json.loads(to_json(propensity)),
    'audiences': json.loads(to_json(audiences)),
    'demo': json.loads(to_json(demo)),
    'geo': json.loads(to_json(geo)),
    'trends': json.loads(to_json(trends)),
    'loyalty': json.loads(to_json(loyalty)),
    'timepatterns': json.loads(to_json(timepatterns)),
}, default=str)

size_mb = len(data_json) / 1024 / 1024
print(f'  Data size: {size_mb:.1f} MB')

now = datetime.now().strftime('%d %B %Y')
cats = sorted(benchmarks['CATEGORY_TWO'].unique().tolist()) if benchmarks is not None else []

# ═══════════════════════════════════════════════════════════════
# HTML
# ═══════════════════════════════════════════════════════════════
print('Building dashboard...')

html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NAV Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'DM Sans',sans-serif;background:#f8fafc;color:#1a202c}}
#hdr{{background:linear-gradient(135deg,#0f172a,#1e3a5f);color:#fff;padding:16px 24px;display:flex;align-items:center;gap:16px;flex-wrap:wrap}}
#hdr h1{{font-size:1.3rem;font-weight:600}}
#hdr .meta{{font-size:.75rem;opacity:.5;margin-left:auto}}
.tabs{{display:flex;background:#fff;border-bottom:1px solid #e2e8f0;padding:0 16px;overflow-x:auto}}
.tab{{padding:10px 18px;font-size:.85rem;color:#64748b;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;font-weight:500}}
.tab:hover{{color:#1e3a5f;background:#f8fafc}}
.tab.a{{color:#1e3a5f;border-bottom-color:#1e3a5f}}
.filters{{background:#fff;padding:10px 24px;border-bottom:1px solid #e2e8f0;display:flex;gap:12px;flex-wrap:wrap;align-items:center}}
.filters label{{font-size:.78rem;color:#64748b;font-weight:500}}
.filters select{{padding:5px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.82rem;font-family:inherit;min-width:160px}}
.tog{{display:inline-flex;border:1px solid #d1d5db;border-radius:6px;overflow:hidden}}
.tog span{{padding:5px 14px;font-size:.78rem;cursor:pointer;color:#64748b}}
.tog span.on{{background:#1e3a5f;color:#fff}}
.pg{{display:none;padding:20px 24px;max-width:1300px;margin:0 auto}}.pg.a{{display:block}}
.row{{display:grid;gap:12px;margin-bottom:14px}}
.r2{{grid-template-columns:1fr 1fr}}.r3{{grid-template-columns:1fr 1fr 1fr}}.r4{{grid-template-columns:1fr 1fr 1fr 1fr}}.r5{{grid-template-columns:repeat(5,1fr)}}
@media(max-width:768px){{.r2,.r3,.r4,.r5{{grid-template-columns:1fr}}}}
.card{{background:#fff;border-radius:10px;padding:14px;border:1px solid #f1f5f9}}
.card .l{{font-size:.72rem;color:#94a3b8;margin-bottom:2px}}
.card .v{{font-size:1.4rem;font-weight:600;color:#0f172a}}
.card .s{{font-size:.72rem;margin-top:2px}}
.sec{{background:#fff;border-radius:12px;padding:18px;margin-bottom:14px;border:1px solid #f1f5f9}}
.sec h3{{font-size:.95rem;font-weight:600;color:#0f172a;margin-bottom:10px}}
.sec .desc{{font-size:.8rem;color:#94a3b8;margin-bottom:12px}}
.chbox{{position:relative;height:280px}}
table{{width:100%;border-collapse:collapse;font-size:.8rem;margin-top:8px}}
th{{background:#0f172a;color:#fff;padding:7px 10px;text-align:left;font-size:.72rem;text-transform:uppercase;letter-spacing:.3px}}
td{{padding:6px 10px;border-bottom:1px solid #f1f5f9}}
tr:hover{{background:#f8fafc}}
.badge{{display:inline-block;padding:1px 8px;border-radius:12px;font-size:.7rem;font-weight:600}}
.b-g{{background:#dcfce7;color:#166534}}.b-r{{background:#fee2e2;color:#991b1b}}.b-y{{background:#fef3c7;color:#92400e}}.b-b{{background:#dbeafe;color:#1e40af}}.b-p{{background:#f3e5f5;color:#7b1fa2}}.b-gr{{background:#f1f5f9;color:#475569}}
.ins{{border-radius:8px;padding:12px 16px;margin:10px 0;font-size:.85rem;font-weight:500}}
.ins-r{{background:#fef2f2;border-left:4px solid #ef4444}}
.ins-y{{background:#fffbeb;border-left:4px solid #f59e0b}}
.ins-g{{background:#f0fdf4;border-left:4px solid #22c55e}}
.ftr{{text-align:center;padding:24px;color:#94a3b8;font-size:.75rem}}
.empty{{color:#94a3b8;font-style:italic;padding:20px;text-align:center}}
</style>
</head><body>

<div id="hdr">
<h1>NAV Analytics Dashboard</h1>
<span class="meta">{PROJECT} · {now} · Confidential</span>
</div>

<div class="tabs" id="tabbar">
<div class="tab a" onclick="showPage(0)">Overview</div>
<div class="tab" onclick="showPage(1)">Client Pitch</div>
<div class="tab" onclick="showPage(2)">Segments</div>
<div class="tab" onclick="showPage(3)">Churn & CLV</div>
<div class="tab" onclick="showPage(4)">Categories</div>
<div class="tab" onclick="showPage(5)">Audiences</div>
</div>

<div class="filters" id="filterbar">
<label>Category</label>
<select id="fCat" onchange="onFilter()">
{''.join(f'<option value="{c}">{c}</option>' for c in cats)}
</select>
<label>Client</label>
<select id="fClient" onchange="onFilter()"></select>
<label>Competitors</label>
<select id="fTopN" onchange="onFilter()">
<option value="5">Top 5</option><option value="8" selected>Top 8</option><option value="15">Top 15</option><option value="999">All</option>
</select>
<div class="tog" id="anonToggle">
<span class="on" onclick="setAnon(false)">Internal</span>
<span onclick="setAnon(true)">External</span>
</div>
</div>

<!-- PAGE 0: OVERVIEW -->
<div class="pg a" id="pg0">
<div class="row r5" id="ovKpis"></div>
<div class="row r2">
<div class="sec"><h3>Customers by segment</h3><div class="chbox"><canvas id="chSegPie"></canvas></div></div>
<div class="sec"><h3>Revenue concentration</h3><div class="chbox"><canvas id="chRevBar"></canvas></div></div>
</div>
<div class="row r3" id="ovHighlights"></div>
<div class="sec"><h3>Churn risk distribution</h3><div class="chbox"><canvas id="chChurnPie"></canvas></div></div>
</div>

<!-- PAGE 1: CLIENT PITCH -->
<div class="pg" id="pg1">
<div class="row r4" id="cpKpis"></div>
<div class="row r2">
<div class="sec"><h3>Market share — competitors</h3><div class="chbox"><canvas id="chCompShare"></canvas></div></div>
<div class="sec"><h3>Spend per customer — competitors</h3><div class="chbox"><canvas id="chCompSpc"></canvas></div></div>
</div>
<div class="row r2">
<div class="sec"><h3>Share of wallet bands</h3><div class="chbox"><canvas id="chSow"></canvas></div></div>
<div class="sec"><h3>Store loyalty — % with 50%+ wallet share</h3><div class="chbox"><canvas id="chLoyalty"></canvas></div></div>
</div>
<div class="sec"><h3>Monthly trend</h3><div class="chbox"><canvas id="chTrend"></canvas></div></div>
<div class="sec"><h3>Shopping time by store</h3><div id="timeTable"></div></div>
<div class="row r3">
<div class="sec"><h3>Age distribution</h3><div class="chbox"><canvas id="chAge"></canvas></div></div>
<div class="sec"><h3>Gender</h3><div class="chbox"><canvas id="chGender"></canvas></div></div>
<div class="sec"><h3>Spend by province</h3><div class="chbox"><canvas id="chGeo"></canvas></div></div>
</div>
<div class="sec"><h3>Income distribution</h3><div class="chbox"><canvas id="chIncome"></canvas></div></div>
</div>

<!-- PAGE 2: SEGMENTS -->
<div class="pg" id="pg2">
<div class="sec"><h3>Segment profiles</h3><div id="segTable"></div></div>
<div class="row r2">
<div class="sec"><h3>Cohort retention</h3><div class="chbox"><canvas id="chRetention"></canvas></div></div>
<div class="sec"><h3>Spend momentum</h3><div id="momTable"></div></div>
</div>
<div class="sec"><h3>Time-of-day shopping by segment</h3><div class="chbox"><canvas id="chBehTime"></canvas></div></div>
</div>

<!-- PAGE 3: CHURN & CLV -->
<div class="pg" id="pg3">
<div class="row r3" id="churnKpis"></div>
<div class="row r2">
<div class="sec"><h3>Top churn drivers</h3><div class="chbox"><canvas id="chChurnDrivers"></canvas></div></div>
<div class="sec"><h3>Risk level breakdown</h3><div id="churnTable"></div></div>
</div>
<div class="sec"><h3>Customer lifetime value tiers</h3><div id="clvTable"></div></div>
</div>

<!-- PAGE 4: CATEGORIES -->
<div class="pg" id="pg4">
<div class="sec"><h3>Category health scorecard</h3><div id="catTable"></div></div>
<div class="row r2">
<div class="sec"><h3>Cross-sell affinity</h3><div id="affTable"></div></div>
<div class="sec"><h3>Category propensity</h3><div id="propTable"></div></div>
</div>
<div class="sec"><h3>Pitch opportunities</h3><div id="pitchTable"></div></div>
</div>

<!-- PAGE 5: AUDIENCES -->
<div class="pg" id="pg5">
<div class="row r3" id="audKpis"></div>
<div class="sec"><h3>Audience catalog</h3>
<div style="margin-bottom:10px">
<label style="font-size:.78rem;color:#64748b">Type: </label>
<select id="fAudType" onchange="renderAudiences()" style="padding:4px 8px;border:1px solid #d1d5db;border-radius:4px;font-size:.8rem">
<option value="">All</option>
<option>Demographic</option><option>Lifestyle</option><option>Behavioral</option><option>Seasonal</option><option>Geographic</option><option>Cross-category</option>
</select>
</div>
<div id="audTable"></div></div>
</div>

<div class="ftr">NAV Analytics Dashboard · {PROJECT} · Built by Prosper Sikhwari · {datetime.now().strftime('%B %Y')}</div>

<script>
const D = DASHBOARD_DATA_PLACEHOLDER;
const C = ['#0f172a','#1e3a5f','#2E75B6','#4CAF50','#FF9800','#f44336','#9C27B0','#00BCD4','#607D8B','#795548'];
const RC = ['#f44336','#FF9800','#fbc02d','#4CAF50','#2196f3'];
Chart.defaults.font.family = "'DM Sans',sans-serif";
Chart.defaults.plugins.legend.labels.usePointStyle = true;

let anon = false;
let charts = {{}};
let currentPage = 0;

// ─── Helpers ───
const fmt = v => {{ if(v==null||isNaN(v)) return 'N/A'; v=Number(v); if(Math.abs(v)>=1e9) return 'R'+(v/1e9).toFixed(1)+'B'; if(Math.abs(v)>=1e6) return 'R'+(v/1e6).toFixed(1)+'M'; if(Math.abs(v)>=1e3) return 'R'+(v/1e3).toFixed(0)+'k'; return 'R'+v.toLocaleString(); }};
const pct = v => v==null ? 'N/A' : v.toFixed(1)+'%';
const num = v => v==null ? 'N/A' : Number(v).toLocaleString();
const badge = (v,type) => `<span class="badge b-${{type}}">${{v}}</span>`;
const healthBadge = s => {{ if(!s) return ''; const m = {{'Growing':'g','Stable':'b','Declining':'r','Monitor':'gr'}}; return badge(s, m[s]||'gr'); }};

function destroyChart(id) {{ if(charts[id]) {{ charts[id].destroy(); delete charts[id]; }} }}

function makeChart(id, config) {{
    destroyChart(id);
    const el = document.getElementById(id);
    if(!el) return;
    charts[id] = new Chart(el, config);
}}

function card(label, value, extra) {{
    return `<div class="card"><div class="l">${{label}}</div><div class="v">${{value}}</div>${{extra?`<div class="s">${{extra}}</div>`:''}}</div>`;
}}

function tableHtml(headers, rows) {{
    let h = '<table><tr>' + headers.map(h=>`<th>${{h}}</th>`).join('') + '</tr>';
    rows.forEach(r => {{ h += '<tr>' + r.map(c=>`<td>${{c}}</td>`).join('') + '</tr>'; }});
    return h + '</table>';
}}

// ─── Page switching ───
function showPage(n) {{
    currentPage = n;
    document.querySelectorAll('.pg').forEach((p,i) => p.classList.toggle('a', i===n));
    document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('a', i===n));
    // Show/hide pitch filters
    const fb = document.getElementById('filterbar');
    fb.style.display = (n===0 || n===1) ? 'flex' : (n >= 2 ? 'none' : 'flex');
    if(n===1) renderPitch();
}}

// ─── Filter logic ───
function onFilter() {{
    const cat = document.getElementById('fCat').value;
    // Update client dropdown
    const clients = [...new Set(D.benchmarks.filter(b=>b.CATEGORY_TWO===cat).map(b=>b.DESTINATION))];
    const sel = document.getElementById('fClient');
    const prev = sel.value;
    sel.innerHTML = clients.map(c=>`<option>${{c}}</option>`).join('');
    if(clients.includes(prev)) sel.value = prev;
    if(currentPage===1) renderPitch();
}}

function setAnon(v) {{
    anon = v;
    document.querySelectorAll('#anonToggle span').forEach((s,i) => s.classList.toggle('on', i===(v?1:0)));
    if(currentPage===1) renderPitch();
}}

function destName(dest, client, rank) {{
    if(!anon) return dest;
    if(dest === client) return dest + ' ★';
    return 'Competitor #' + rank;
}}

// ─── RENDER: Overview ───
function renderOverview() {{
    const ov = D.overview;
    document.getElementById('ovKpis').innerHTML =
        card('Transactions', num(ov.txns)) +
        card('Customers', num(ov.custs)) +
        card('Segmented', num(ov.segs)) +
        card('Churn scored', num(ov.churn)) +
        card('Destinations', num(ov.dests));

    // Highlights
    const hi = D.churn || [];
    const critical = hi.filter(c=>c.churn_risk_level==='Critical'||c.churn_risk_level==='High');
    const arCusts = critical.reduce((s,c)=>s+c.custs,0);
    const arSpend = critical.reduce((s,c)=>s+c.spend,0);
    document.getElementById('ovHighlights').innerHTML =
        card('Champions → revenue', D.revenue && D.revenue.find(r=>r.segment_name==='Champions') ? D.revenue.find(r=>r.segment_name==='Champions').pct_rev+'%' : 'N/A', 'from '+((D.revenue && D.revenue.find(r=>r.segment_name==='Champions'))||{{}}).pct_cust+'% of customers') +
        card('At-risk spend', fmt(arSpend), `<span style="color:#dc2626">${{num(arCusts)}} critical + high</span>`) +
        card('10% recovery', fmt(arSpend*0.1));

    // Segment pie
    if(D.profiles) {{
        makeChart('chSegPie', {{type:'doughnut',data:{{labels:D.profiles.map(p=>p.segment_name),datasets:[{{data:D.profiles.map(p=>p.customer_count),backgroundColor:C,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:false}}}}}}}});
    }}

    // Revenue bar
    if(D.revenue) {{
        makeChart('chRevBar', {{type:'bar',data:{{labels:D.revenue.map(r=>r.segment_name),datasets:[{{label:'% customers',data:D.revenue.map(r=>r.pct_cust),backgroundColor:'#94a3b8',borderRadius:4}},{{label:'% revenue',data:D.revenue.map(r=>r.pct_rev),backgroundColor:'#0f172a',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:false}}}}}}}});
    }}

    // Churn pie
    if(D.churn) {{
        makeChart('chChurnPie', {{type:'doughnut',data:{{labels:D.churn.map(c=>c.churn_risk_level),datasets:[{{data:D.churn.map(c=>c.custs),backgroundColor:RC,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false}}}});
    }}
}}

// ─── RENDER: Client Pitch ───
function renderPitch() {{
    const cat = document.getElementById('fCat').value;
    const client = document.getElementById('fClient').value;
    const topN = parseInt(document.getElementById('fTopN').value);
    if(!cat || !client) return;

    // Filter benchmarks
    let comps = D.benchmarks.filter(b=>b.CATEGORY_TWO===cat).sort((a,b)=>b.total_spend-a.total_spend).slice(0, topN);
    const ck = comps.find(c=>c.DESTINATION===client);

    // KPIs
    document.getElementById('cpKpis').innerHTML = ck ? (
        card('Customers', num(ck.customers)) +
        card('Total Spend', fmt(ck.total_spend)) +
        card('Market Share', pct(ck.market_share_pct)) +
        card('Penetration', pct(ck.penetration_pct))
    ) : '<div class="empty">Client not found in this category</div>';

    // Competitor charts
    const labels = comps.map((c,i) => destName(c.DESTINATION, client, i+1));
    const colors = comps.map(c => c.DESTINATION===client ? '#d97706' : '#0f172a');

    makeChart('chCompShare', {{type:'bar',data:{{labels,datasets:[{{data:comps.map(c=>c.market_share_pct),backgroundColor:colors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    makeChart('chCompSpc', {{type:'bar',data:{{labels,datasets:[{{data:comps.map(c=>c.spend_per_customer),backgroundColor:colors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>fmt(v)}}}}}}}}}});

    // Loyalty
    const loy = (D.loyalty||[]).filter(l=>l.CATEGORY_TWO===cat).sort((a,b)=>b.pct_loyal_50-a.pct_loyal_50).slice(0,topN);
    if(loy.length) {{
        const loyLabels = loy.map(l => destName(l.DESTINATION, client, loy.indexOf(l)+1));
        const loyColors = loy.map(l => l.DESTINATION===client ? '#d97706' : '#1e3a5f');
        makeChart('chLoyalty', {{type:'bar',data:{{labels:loyLabels,datasets:[{{data:loy.map(l=>l.pct_loyal_50),backgroundColor:loyColors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    }}

    // SOW bands
    if(ck && D.loyalty) {{
        const cl = D.loyalty.find(l=>l.CATEGORY_TWO===cat && l.DESTINATION===client);
        if(cl) {{
            makeChart('chSow', {{type:'bar',data:{{labels:['1 store','2 stores','3-4','5-7','8+'],datasets:[{{data:[cl.band_1_store,cl.band_2_stores,cl.band_3_4_stores,cl.band_5_7_stores,cl.band_8_plus],backgroundColor:['#0f172a','#1e3a5f','#2E75B6','#94a3b8','#d1d5db'],borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},title:{{display:true,text:client+' customer loyalty bands'}}}}}}}});
        }}
    }}

    // Trend
    const catTrends = (D.trends||[]).filter(t=>t.CATEGORY_TWO===cat);
    const months = [...new Set(catTrends.map(t=>t.month))].sort();
    const clientTrend = months.map(m => {{ const r = catTrends.find(t=>t.month===m&&t.DESTINATION===client); return r?r.spend/1e6:0; }});
    const catTotal = months.map(m => catTrends.filter(t=>t.month===m).reduce((s,t)=>s+t.spend,0)/1e6);
    makeChart('chTrend', {{type:'line',data:{{labels:months.map(m=>m.substring(0,7)),datasets:[
        {{label:cat+' total',data:catTotal,borderColor:'#94a3b8',borderWidth:1.5,borderDash:[5,3],tension:.3,pointRadius:2}},
        {{label:client,data:clientTrend,borderColor:'#d97706',borderWidth:2.5,backgroundColor:'rgba(217,119,6,.1)',fill:true,tension:.3,pointRadius:3}}
    ]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

    // Time patterns table
    const tp = (D.timepatterns||[]).filter(t=>t.CATEGORY_TWO===cat).slice(0,topN);
    if(tp.length) {{
        document.getElementById('timeTable').innerHTML = tableHtml(
            ['Store','Morning','Midday','Afternoon','Evening','Weekend'],
            tp.map(t=>[destName(t.DESTINATION,client,tp.indexOf(t)+1), pct(t.morning), pct(t.midmorning), pct(t.afternoon), pct(t.evening), pct(t.weekend)])
        );
    }}

    // Demographics
    const dm = (D.demo||[]).filter(d=>d.CATEGORY_TWO===cat);
    const ages = {{}};
    dm.forEach(d => {{ if(d.age_group) ages[d.age_group] = (ages[d.age_group]||0) + d.customers; }});
    const ageLabels = Object.keys(ages).sort();
    makeChart('chAge', {{type:'bar',data:{{labels:ageLabels,datasets:[{{data:ageLabels.map(a=>ages[a]),backgroundColor:'#0f172a',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}}}}}});

    const genders = {{}};
    dm.forEach(d => {{ if(d.gender_label && d.gender_label!=='Unknown') genders[d.gender_label] = (genders[d.gender_label]||0) + d.customers; }});
    makeChart('chGender', {{type:'doughnut',data:{{labels:Object.keys(genders),datasets:[{{data:Object.values(genders),backgroundColor:['#0f172a','#E91E63','#607D8B'],borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false}}}});

    const incomes = {{}};
    dm.forEach(d => {{ if(d.income_group && d.income_group!=='Unknown') incomes[d.income_group] = (incomes[d.income_group]||0) + d.spend; }});
    const incLabels = Object.keys(incomes).sort();
    makeChart('chIncome', {{type:'bar',data:{{labels:incLabels,datasets:[{{data:incLabels.map(i=>incomes[i]/1e6),backgroundColor:'#2E75B6',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

    // Geo
    const gd = (D.geo||[]).filter(g=>g.CATEGORY_TWO===cat).sort((a,b)=>b.spend-a.spend).slice(0,9);
    makeChart('chGeo', {{type:'bar',data:{{labels:gd.map(g=>g.PROVINCE),datasets:[{{data:gd.map(g=>g.spend/1e6),backgroundColor:'#0f172a',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});
}}

// ─── RENDER: Segments ───
function renderSegments() {{
    if(D.profiles) {{
        const rows = D.profiles.map(p => {{
            const s = (D.summary||[]).find(s=>s.segment_name===p.segment_name) || {{}};
            return [
                `<strong>${{p.segment_name}}</strong>`,
                num(p.customer_count), fmt(p.avg_total_spend), Math.round(p.avg_transactions),
                Math.round(p.avg_recency_days)+'d', Math.round(p.avg_merchants),
                p.top_age_group||'', p.top_income_group||'',
                s.recommended_action || ''
            ];
        }});
        document.getElementById('segTable').innerHTML = tableHtml(
            ['Segment','Customers','Avg Spend','Avg Txns','Recency','Merchants','Top Age','Top Income','Action'], rows
        );
    }}

    // Retention
    if(D.retention) {{
        makeChart('chRetention', {{type:'line',data:{{labels:D.retention.map(r=>r.month),datasets:[{{label:'Retention %',data:D.retention.map(r=>r.ret),borderColor:'#0f172a',backgroundColor:'rgba(15,23,42,.1)',borderWidth:2.5,fill:true,tension:.3,pointRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{beginAtZero:true,max:100,title:{{display:true,text:'%'}}}},x:{{title:{{display:true,text:'Months since first transaction'}}}}}}}}}});
    }}

    // Momentum
    if(D.momentum) {{
        const mColors = {{'Declining':'r','Slowing':'y','Steady':'b','Accelerating':'g','New':'gr'}};
        document.getElementById('momTable').innerHTML = tableHtml(
            ['Status','Customers','Avg 12m Spend','Change','Urgency'],
            D.momentum.map(m=>[badge(m.momentum_status, mColors[m.momentum_status]||'gr'), num(m.custs), fmt(m.spend), (m.chg>0?'+':'')+m.chg+'%', m.urg])
        );
    }}

    // Behavioral
    if(D.behavioral) {{
        makeChart('chBehTime', {{type:'bar',data:{{labels:D.behavioral.map(b=>b.segment_name),datasets:[
            {{label:'Morning',data:D.behavioral.map(b=>b.pct_morning),backgroundColor:'#FF9800',borderRadius:3}},
            {{label:'Afternoon',data:D.behavioral.map(b=>b.pct_afternoon),backgroundColor:'#0f172a',borderRadius:3}},
            {{label:'Evening',data:D.behavioral.map(b=>b.pct_evening),backgroundColor:'#4CAF50',borderRadius:3}}
        ]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{title:{{display:true,text:'%'}}}}}}}}}});
    }}
}}

// ─── RENDER: Churn & CLV ───
function renderChurn() {{
    const hi = (D.churn||[]).filter(c=>c.churn_risk_level==='Critical'||c.churn_risk_level==='High');
    const arC = hi.reduce((s,c)=>s+c.custs,0);
    const arS = hi.reduce((s,c)=>s+c.spend,0);
    document.getElementById('churnKpis').innerHTML =
        card('Critical + High', num(arC), '<span style="color:#dc2626">'+pct(arC/((D.churn||[]).reduce((s,c)=>s+c.custs,0)||1)*100)+' of base</span>') +
        card('Spend at risk', fmt(arS)) +
        card('10% recovery', fmt(arS*.1));

    // Drivers
    if(D.churn_reasons) {{
        makeChart('chChurnDrivers', {{type:'bar',data:{{labels:D.churn_reasons.map(r=>r.reason_1),datasets:[{{data:D.churn_reasons.map(r=>r.custs),backgroundColor:RC.concat(C),borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    }}

    // Risk table
    if(D.churn) {{
        document.getElementById('churnTable').innerHTML = tableHtml(
            ['Level','Customers','Avg Prob','Spend at Risk','Avg Days'],
            D.churn.map(c=>[`<strong>${{c.churn_risk_level}}</strong>`, num(c.custs), pct(c.avg_prob), fmt(c.spend), Math.round(c.avg_days)])
        );
    }}

    // CLV
    if(D.clv) {{
        document.getElementById('clvTable').innerHTML = tableHtml(
            ['Tier','Customers','Avg CLV','Avg Historical','Total Predicted'],
            D.clv.map(c=>[`<strong>${{c.clv_tier}}</strong>`, num(c.custs), fmt(c.avg_clv), fmt(c.avg_hist), fmt(c.total_clv)])
        );
    }}
}}

// ─── RENDER: Categories ───
function renderCategories() {{
    if(D.categories) {{
        document.getElementById('catTable').innerHTML = tableHtml(
            ['Category','Customers','Spend','Growth','Churn','% Champ','% Dormant','Leader','Health'],
            D.categories.map(c=>[
                `<strong>${{c.CATEGORY_TWO}}</strong>`, num(c.total_customers), fmt(c.total_spend),
                `<span style="color:${{c.growth_pct>0?'#16a34a':'#dc2626'}}">${{c.growth_pct>0?'+':''}}${{c.growth_pct}}%</span>`,
                pct(c.avg_churn_pct), pct(c.pct_champions), pct(c.pct_dormant),
                c.top_destination_name, healthBadge(c.health_status)
            ])
        );
    }}

    if(D.affinity) {{
        document.getElementById('affTable').innerHTML = tableHtml(
            ['Category A','Category B','Shared','Lift','A→B','B→A'],
            D.affinity.map(a=>[a.category_a, a.category_b, num(a.shared_customers), `<strong>${{a.lift}}x</strong>`, a.pct_a_b+'%', a.pct_b_a+'%'])
        );
    }}

    if(D.propensity) {{
        document.getElementById('propTable').innerHTML = tableHtml(
            ['Segment','Category','Propensity','Unadopted','Revenue','Adoption'],
            D.propensity.map(p=>[`<strong>${{p.segment_name}}</strong>`, p.CATEGORY_TWO, p.propensity_level, num(p.unadopted_customers), fmt(p.pot), p.adopt+'%'])
        );
    }}

    if(D.pitches) {{
        document.getElementById('pitchTable').innerHTML = tableHtml(
            ['Destination','Category','Customers','Share','Addressable','Gap','Score','Action'],
            D.pitches.map(p=>[`<strong>${{p.DESTINATION}}</strong>`, p.CATEGORY_TWO, num(p.customers), pct(p.market_share_pct), fmt(p.addressable), pct(p.gap_to_leader_pct), `<strong>${{p.score}}</strong>`, p.recommended_action?p.recommended_action.split(' - ')[0]:''])
        );
    }}
}}

// ─── RENDER: Audiences ───
function renderAudiences() {{
    const typeFilter = document.getElementById('fAudType')?.value || '';
    let auds = D.audiences || [];
    if(typeFilter) auds = auds.filter(a=>a.audience_type===typeFilter);

    document.getElementById('audKpis').innerHTML =
        card('Total audiences', auds.length) +
        card('Total reachable', num(auds.reduce((s,a)=>s+a.audience_size,0))) +
        card('Avg size', num(Math.round(auds.reduce((s,a)=>s+a.audience_size,0)/(auds.length||1))));

    const typeColors = {{'Demographic':'b','Lifestyle':'g','Behavioral':'r','Seasonal':'y','Geographic':'gr','Cross-category':'p'}};
    document.getElementById('audTable').innerHTML = tableHtml(
        ['Audience','Type','Size','Avg Spend','Avg Age','% Female','Province','Description'],
        auds.map(a=>[
            `<strong>${{a.audience_name}}</strong>`,
            badge(a.audience_type, typeColors[a.audience_type]||'gr'),
            num(a.audience_size), fmt(a.avg_spend),
            a.avg_age ? Math.round(a.avg_age) : '',
            a.pct_female ? pct(a.pct_female) : '',
            a.top_province || '',
            `<span style="font-size:.75rem;color:#64748b">${{a.description||''}}</span>`
        ])
    );
}}

// ─── INIT ───
function init() {{
    onFilter();
    renderOverview();
    renderSegments();
    renderChurn();
    renderCategories();
    renderAudiences();
    renderPitch();
}}

init();
</script>
</body></html>"""

# Inject data
html = html.replace('DASHBOARD_DATA_PLACEHOLDER', data_json)

with open(OUT, 'w') as f:
    f.write(html)

size_html = os.path.getsize(OUT) / 1024 / 1024
print(f'\n✓ {OUT} ({size_html:.1f} MB)')
print(f'  6 pages, live filtering, {len(cats)} categories')
print(f'  Open in Chrome: file://{os.path.abspath(OUT)}')
print(f'\nTo refresh with new data: python scripts/generate_dashboard.py')
