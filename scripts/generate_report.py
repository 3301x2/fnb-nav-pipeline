"""
generate_report.py
pulls key insights from BigQuery and generates a beautiful HTML + PDF report
uses Chart.js for visuals, covers all 8 notebooks worth of insights

usage:
    python scripts/generate_report.py
    BQ_PROJECT=fmn-production python scripts/generate_report.py

output:
    insights_report.html  (interactive, open in browser)
    insights_report.pdf   (if playwright is installed)

requires: pip install google-cloud-bigquery db-dtypes pandas
optional: pip install playwright && playwright install chromium  (for PDF)
"""

import os
import json
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
LOCATION = 'africa-south1'
client = bigquery.Client(project=PROJECT, location=LOCATION)
HTML_OUT = 'insights_report.html'
PDF_OUT = 'insights_report.pdf'

def q(sql):
    return client.query(sql).to_dataframe()

def fmt_r(val):
    if val >= 1e9: return f'R{val/1e9:.1f}B'
    if val >= 1e6: return f'R{val/1e6:.1f}M'
    if val >= 1e3: return f'R{val/1e3:.0f}k'
    return f'R{val:.0f}'

def safe_query(sql, fallback=None):
    try:
        return q(sql)
    except Exception as e:
        print(f'    query failed: {e}')
        return fallback

def to_json(df, col):
    if df is None: return '[]'
    return json.dumps([float(x) if pd.notna(x) else 0 for x in df[col]])

def to_json_str(df, col):
    if df is None: return '[]'
    return json.dumps([str(x) for x in df[col]])

# -- pull all data --

print(f'pulling data from {PROJECT}...')

print('  pipeline overview')
overview = safe_query(f"""
    SELECT 'Transactions processed' AS metric, COUNT(*) AS value FROM `{PROJECT}.staging.stg_transactions`
    UNION ALL SELECT 'Customers profiled', COUNT(*) FROM `{PROJECT}.staging.stg_customers`
    UNION ALL SELECT 'Customers segmented', COUNT(*) FROM `{PROJECT}.marts.mart_cluster_output`
    UNION ALL SELECT 'Customers scored (churn)', COUNT(*) FROM `{PROJECT}.marts.mart_churn_risk`
    UNION ALL SELECT 'Destinations benchmarked', COUNT(*) FROM `{PROJECT}.marts.mart_destination_benchmarks`
""")

print('  segments')
segments = safe_query(f"""
    SELECT segment_name, customer_count, pct_of_total,
           ROUND(avg_total_spend, 0) AS avg_total_spend,
           ROUND(total_segment_spend, 0) AS total_segment_spend
    FROM `{PROJECT}.marts.mart_cluster_profiles`
    ORDER BY avg_total_spend DESC
""")

print('  revenue concentration')
revenue = safe_query(f"""
    SELECT segment_name,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_customers,
           ROUND(SUM(val_trns) * 100.0 / SUM(SUM(val_trns)) OVER(), 1) AS pct_revenue
    FROM `{PROJECT}.marts.mart_cluster_output`
    GROUP BY segment_name
    ORDER BY pct_revenue DESC
""")

print('  churn risk')
churn = safe_query(f"""
    SELECT churn_risk_level,
           COUNT(*) AS customers,
           ROUND(SUM(total_spend), 0) AS total_spend
    FROM `{PROJECT}.marts.mart_churn_risk`
    GROUP BY churn_risk_level
    ORDER BY CASE churn_risk_level
        WHEN 'Critical' THEN 1 WHEN 'High' THEN 2
        WHEN 'Medium' THEN 3 WHEN 'Low' THEN 4 ELSE 5 END
""")

print('  behavioral')
behavioral = safe_query(f"""
    SELECT segment_name, pct_morning, pct_afternoon, pct_evening, pct_late_night,
           pct_weekend, avg_categories, avg_merchants, avg_txn_value
    FROM `{PROJECT}.marts.mart_behavioral_summary`
    ORDER BY avg_txns_per_customer DESC
""")

print('  monthly trends (top category)')
trends = safe_query(f"""
    WITH top_cat AS (
        SELECT CATEGORY_TWO FROM `{PROJECT}.marts.mart_destination_benchmarks`
        GROUP BY CATEGORY_TWO ORDER BY SUM(total_spend) DESC LIMIT 1
    )
    SELECT CAST(month AS STRING) AS month_str, SUM(total_spend) AS total_spend
    FROM `{PROJECT}.marts.mart_monthly_trends`
    WHERE CATEGORY_TWO = (SELECT CATEGORY_TWO FROM top_cat)
    GROUP BY month ORDER BY month
""")

print('  demographics')
demo_age = safe_query(f"""
    SELECT age_group, SUM(customers) AS customers, ROUND(SUM(total_spend), 0) AS total_spend
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE age_group IS NOT NULL
    GROUP BY age_group ORDER BY age_group
""")

demo_gender = safe_query(f"""
    SELECT gender_label, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_demographic_summary`
    WHERE gender_label IS NOT NULL AND gender_label != 'Unknown'
    GROUP BY gender_label
""")

print('  geo')
geo = safe_query(f"""
    SELECT PROVINCE, SUM(total_spend) AS total_spend, SUM(customers) AS customers
    FROM `{PROJECT}.marts.mart_geo_summary`
    WHERE PROVINCE IS NOT NULL
    GROUP BY PROVINCE ORDER BY total_spend DESC LIMIT 10
""")

print('  category health')
categories = safe_query(f"""
    SELECT CATEGORY_TWO, ROUND(total_spend, 0) AS total_spend,
           growth_pct, avg_churn_pct, health_status,
           top_destination_name, top_dest_market_share
    FROM `{PROJECT}.marts.mart_category_scorecard`
    WHERE growth_pct IS NOT NULL
    ORDER BY total_spend DESC LIMIT 20
""")

print('  pitch opportunities')
pitches = safe_query(f"""
    SELECT DESTINATION, CATEGORY_TWO, market_share_pct,
           ROUND(addressable_market, 0) AS addressable_market,
           ROUND(pitch_score, 1) AS pitch_score, recommended_action
    FROM `{PROJECT}.marts.mart_pitch_opportunities`
    ORDER BY pitch_score DESC LIMIT 15
""")

print('  cohort retention')
retention = safe_query(f"""
    SELECT months_since_first, ROUND(AVG(retention_pct), 1) AS avg_retention
    FROM `{PROJECT}.marts.mart_cohort_retention`
    WHERE cohort_size >= 1000 AND months_since_first IN (1, 2, 3, 6, 9, 12)
    GROUP BY months_since_first ORDER BY months_since_first
""")

print('  cross-sell')
affinity = safe_query(f"""
    SELECT category_a, category_b, shared_customers, lift, pct_a_also_shops_b
    FROM `{PROJECT}.marts.mart_category_affinity`
    WHERE lift > 1.2 ORDER BY shared_customers DESC LIMIT 12
""")

# -- build chart data --

seg_labels = to_json_str(segments, 'segment_name')
seg_counts = to_json(segments, 'customer_count')
seg_spend = to_json(segments, 'avg_total_spend')

rev_labels = to_json_str(revenue, 'segment_name')
rev_custs = to_json(revenue, 'pct_customers')
rev_revenue = to_json(revenue, 'pct_revenue')

churn_labels = to_json_str(churn, 'churn_risk_level')
churn_custs = to_json(churn, 'customers')
# format spend as millions for the chart so axis labels are readable
churn_spend_m = json.dumps([round(float(x)/1e6, 0) for x in churn['total_spend']]) if churn is not None else '[]'

ret_labels = to_json_str(retention, 'months_since_first') if retention is not None else '[]'
ret_values = to_json(retention, 'avg_retention') if retention is not None else '[]'

# behavioral chart data
beh_labels = to_json_str(behavioral, 'segment_name') if behavioral is not None else '[]'
beh_morning = to_json(behavioral, 'pct_morning') if behavioral is not None else '[]'
beh_afternoon = to_json(behavioral, 'pct_afternoon') if behavioral is not None else '[]'
beh_evening = to_json(behavioral, 'pct_evening') if behavioral is not None else '[]'

# trends
trend_labels = to_json_str(trends, 'month_str') if trends is not None else '[]'
trend_spend_m = json.dumps([round(float(x)/1e6, 0) for x in trends['total_spend']]) if trends is not None else '[]'

# demographics
age_labels = to_json_str(demo_age, 'age_group') if demo_age is not None else '[]'
age_custs = to_json(demo_age, 'customers') if demo_age is not None else '[]'

gender_labels = to_json_str(demo_gender, 'gender_label') if demo_gender is not None else '[]'
gender_custs = to_json(demo_gender, 'customers') if demo_gender is not None else '[]'

# geo
geo_labels = to_json_str(geo, 'PROVINCE') if geo is not None else '[]'
geo_spend_m = json.dumps([round(float(x)/1e6, 0) for x in geo['total_spend']]) if geo is not None else '[]'

# -- build HTML tables --

overview_html = ''
if overview is not None:
    for _, row in overview.iterrows():
        overview_html += f'<div class="metric-card"><div class="metric-value">{int(row["value"]):,}</div><div class="metric-label">{row["metric"]}</div></div>\n'

seg_table = ''
if segments is not None:
    for _, row in segments.iterrows():
        seg_table += f'<tr><td><strong>{row["segment_name"]}</strong></td><td>{row["customer_count"]:,}</td><td>{row["pct_of_total"]}%</td><td>{fmt_r(row["avg_total_spend"])}</td><td>{fmt_r(row["total_segment_spend"])}</td></tr>\n'

churn_headline = ''
if churn is not None:
    ch = churn[churn['churn_risk_level'].isin(['Critical', 'High'])]
    churn_headline = f'{ch["customers"].sum():,} customers ({ch["customers"].sum()*100/churn["customers"].sum():.1f}%) are Critical or High risk, representing {fmt_r(ch["total_spend"].sum())} in spend'

champ_line = ''
if revenue is not None:
    c = revenue[revenue['segment_name'] == 'Champions']
    if not c.empty:
        champ_line = f'Champions are {c.iloc[0]["pct_customers"]}% of customers but drive {c.iloc[0]["pct_revenue"]}% of revenue'

cat_table = ''
if categories is not None:
    for _, row in categories.iterrows():
        hc = row['health_status'].lower()
        g = f'+{row["growth_pct"]:.1f}%' if row['growth_pct'] > 0 else f'{row["growth_pct"]:.1f}%'
        cat_table += f'<tr><td><strong>{row["CATEGORY_TWO"]}</strong></td><td>{fmt_r(row["total_spend"])}</td><td>{g}</td><td>{row["avg_churn_pct"]:.1f}%</td><td>{row["top_destination_name"]}</td><td><span class="badge badge-{hc}">{row["health_status"]}</span></td></tr>\n'

pitch_table = ''
if pitches is not None:
    for _, row in pitches.iterrows():
        ac = row['recommended_action'].split(' - ')[0].lower()
        pitch_table += f'<tr><td><strong>{row["DESTINATION"]}</strong></td><td>{row["CATEGORY_TWO"]}</td><td>{row["market_share_pct"]:.1f}%</td><td>{fmt_r(row["addressable_market"])}</td><td>{row["pitch_score"]}</td><td><span class="badge badge-{ac}">{row["recommended_action"].split(" - ")[0]}</span></td></tr>\n'

affinity_table = ''
if affinity is not None:
    for _, row in affinity.iterrows():
        affinity_table += f'<tr><td>{row["category_a"]}</td><td>{row["category_b"]}</td><td>{int(row["shared_customers"]):,}</td><td><strong>{row["lift"]:.1f}x</strong></td><td>{row["pct_a_also_shops_b"]:.0f}%</td></tr>\n'

beh_table = ''
if behavioral is not None:
    for _, row in behavioral.iterrows():
        beh_table += f'<tr><td><strong>{row["segment_name"]}</strong></td><td>{row["pct_morning"]:.1f}%</td><td>{row["pct_afternoon"]:.1f}%</td><td>{row["pct_evening"]:.1f}%</td><td>{row["pct_weekend"]:.1f}%</td><td>{row["avg_merchants"]:.0f}</td><td>R{row["avg_txn_value"]:,.0f}</td></tr>\n'

top_cat_name = ''
if trends is not None and categories is not None and not categories.empty:
    top_cat_name = categories.iloc[0]['CATEGORY_TWO']

now = datetime.now().strftime('%d %B %Y')

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FNB NAV — Insights Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Inter', sans-serif; background: #f8f9fa; color: #333; }}
    .header {{ background: linear-gradient(135deg, #1a365d 0%, #2E75B6 100%); color: white; padding: 50px 40px; text-align: center; }}
    .header h1 {{ font-size: 2.4rem; font-weight: 700; margin-bottom: 8px; }}
    .header p {{ font-size: 1.1rem; opacity: 0.85; }}
    .header .date {{ margin-top: 15px; font-size: 0.9rem; opacity: 0.7; }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 30px 20px; }}
    .section {{ background: white; border-radius: 12px; padding: 35px; margin-bottom: 30px; box-shadow: 0 2px 12px rgba(0,0,0,0.06); page-break-inside: avoid; }}
    .section h2 {{ font-size: 1.5rem; color: #1a365d; margin-bottom: 8px; border-bottom: 3px solid #2E75B6; padding-bottom: 10px; display: inline-block; }}
    .section .sub {{ color: #666; margin-bottom: 25px; font-size: 0.95rem; }}
    .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 18px; margin-top: 20px; }}
    .metric-card {{ background: linear-gradient(135deg, #f0f7ff 0%, #e8f4fd 100%); border-radius: 10px; padding: 22px; text-align: center; border-left: 4px solid #2E75B6; }}
    .metric-value {{ font-size: 1.6rem; font-weight: 700; color: #1a365d; }}
    .metric-label {{ font-size: 0.82rem; color: #666; margin-top: 5px; }}
    .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 25px; margin-top: 20px; }}
    .chart-box {{ position: relative; height: 320px; }}
    .chart-full {{ position: relative; height: 320px; grid-column: span 2; }}
    @media (max-width: 768px) {{ .chart-row {{ grid-template-columns: 1fr; }} .chart-full {{ grid-column: span 1; }} }}
    .headline {{ background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%); border-left: 4px solid #FF9800; border-radius: 8px; padding: 16px 22px; margin: 20px 0; font-size: 1rem; font-weight: 500; }}
    .headline.danger {{ background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%); border-left-color: #f44336; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 0.88rem; }}
    th {{ background: #1a365d; color: white; padding: 11px 14px; text-align: left; font-weight: 600; }}
    td {{ padding: 9px 14px; border-bottom: 1px solid #eee; }}
    tr:hover {{ background: #f8f9fa; }}
    .badge {{ display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 0.78rem; font-weight: 600; }}
    .badge-defend {{ background: #e3f2fd; color: #1565c0; }}
    .badge-grow {{ background: #e8f5e9; color: #2e7d32; }}
    .badge-attack {{ background: #fff3e0; color: #e65100; }}
    .badge-opportunity {{ background: #f3e5f5; color: #7b1fa2; }}
    .badge-protect {{ background: #ffebee; color: #c62828; }}
    .badge-monitor {{ background: #f5f5f5; color: #616161; }}
    .badge-growing {{ background: #e8f5e9; color: #2e7d32; }}
    .badge-stable {{ background: #e3f2fd; color: #1565c0; }}
    .badge-slowing {{ background: #fff3e0; color: #e65100; }}
    .badge-declining {{ background: #ffebee; color: #c62828; }}
    .footer {{ text-align: center; padding: 30px; color: #999; font-size: 0.85rem; }}
    .page-num {{ text-align: right; color: #ccc; font-size: 0.75rem; margin-top: 10px; }}
</style>
</head>
<body>

<div class="header">
    <h1>FNB NAV Data Platform</h1>
    <p>Analytics Insights Report</p>
    <div class="date">Generated {now} &middot; Project: {PROJECT} &middot; Confidential</div>
</div>

<div class="container">

    <div class="section">
        <h2>Pipeline Overview</h2>
        <p class="sub">Scale of data processed by the analytics pipeline</p>
        <div class="metrics-grid">{overview_html}</div>
    </div>

    <div class="section">
        <h2>Customer Segments</h2>
        <p class="sub">K-means ML model groups customers into 5 behavioural segments based on spending patterns</p>
        <div class="headline">{champ_line}</div>
        <div class="chart-row">
            <div class="chart-box"><canvas id="segPie"></canvas></div>
            <div class="chart-box"><canvas id="segSpend"></canvas></div>
        </div>
        <table><tr><th>Segment</th><th>Customers</th><th>% of total</th><th>Avg spend</th><th>Total spend</th></tr>{seg_table}</table>
    </div>

    <div class="section">
        <h2>Revenue Concentration</h2>
        <p class="sub">Small group of customers drives outsized revenue</p>
        <div class="chart-row"><div class="chart-full"><canvas id="revChart"></canvas></div></div>
    </div>

    <div class="section">
        <h2>Churn Risk</h2>
        <p class="sub">ML model scores every customer with a churn probability (0-100%)</p>
        <div class="headline danger">{churn_headline}</div>
        <div class="chart-row">
            <div class="chart-box"><canvas id="churnPie"></canvas></div>
            <div class="chart-box"><canvas id="churnSpend"></canvas></div>
        </div>
    </div>

    <div class="section">
        <h2>Shopping Behaviour by Segment</h2>
        <p class="sub">When do different segments shop and how diverse are their habits?</p>
        <div class="chart-row"><div class="chart-full"><canvas id="behChart"></canvas></div></div>
        <table><tr><th>Segment</th><th>Morning</th><th>Afternoon</th><th>Evening</th><th>Weekend</th><th>Merchants</th><th>Avg txn</th></tr>{beh_table}</table>
    </div>

    <div class="section">
        <h2>Monthly Spend Trend</h2>
        <p class="sub">Total monthly spend for {top_cat_name} (top category by spend)</p>
        <div class="chart-row"><div class="chart-full"><canvas id="trendChart"></canvas></div></div>
    </div>

    <div class="section">
        <h2>Customer Demographics</h2>
        <p class="sub">Age and gender breakdown across all categories</p>
        <div class="chart-row">
            <div class="chart-box"><canvas id="ageChart"></canvas></div>
            <div class="chart-box"><canvas id="genderChart"></canvas></div>
        </div>
    </div>

    <div class="section">
        <h2>Geographic Spend</h2>
        <p class="sub">Top 10 provinces by total spend</p>
        <div class="chart-row"><div class="chart-full"><canvas id="geoChart"></canvas></div></div>
    </div>

    <div class="section">
        <h2>Customer Retention</h2>
        <p class="sub">Of customers who first transacted in a given month, what % are still active after N months?</p>
        <div class="chart-row"><div class="chart-full"><canvas id="retChart"></canvas></div></div>
    </div>

    <div class="section">
        <h2>Category Health</h2>
        <p class="sub">Top 20 categories — growth trend, churn exposure, market leader</p>
        <table><tr><th>Category</th><th>Total spend</th><th>Growth</th><th>Churn risk</th><th>Leader</th><th>Health</th></tr>{cat_table}</table>
    </div>

    <div class="section">
        <h2>Top Pitch Opportunities</h2>
        <p class="sub">Ranked by composite score: market size, growth gap, churn risk, spend efficiency</p>
        <table><tr><th>Destination</th><th>Category</th><th>Share</th><th>Addressable</th><th>Score</th><th>Action</th></tr>{pitch_table}</table>
    </div>

    <div class="section">
        <h2>Cross-Sell Opportunities</h2>
        <p class="sub">Categories commonly shopped together. Lift > 1 means more likely than random chance.</p>
        <table><tr><th>Category A</th><th>Category B</th><th>Shared customers</th><th>Lift</th><th>% A also in B</th></tr>{affinity_table}</table>
    </div>

</div>

<div class="footer">FNB NAV Data Platform &middot; Built by Prosper Sikhwari &middot; {datetime.now().strftime('%B %Y')}</div>

<script>
Chart.defaults.font.family = 'Inter, sans-serif';
Chart.defaults.plugins.legend.labels.usePointStyle = true;
const C = ['#2E75B6','#4CAF50','#FF9800','#607D8B','#f44336','#9C27B0','#00BCD4'];
const RC = ['#f44336','#FF9800','#fbc02d','#4CAF50','#2196f3'];

new Chart('segPie', {{ type:'doughnut', data:{{ labels:{seg_labels}, datasets:[{{ data:{seg_counts}, backgroundColor:C, borderWidth:2, borderColor:'#fff' }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customer distribution', font:{{ size:14, weight:600 }} }} }} }} }});

new Chart('segSpend', {{ type:'bar', data:{{ labels:{seg_labels}, datasets:[{{ label:'Avg spend (R)', data:{seg_spend}, backgroundColor:C, borderRadius:6 }}] }}, options:{{ responsive:true, maintainAspectRatio:false, indexAxis:'y', plugins:{{ title:{{ display:true, text:'Average spend per segment', font:{{ size:14, weight:600 }} }}, legend:{{ display:false }} }}, scales:{{ x:{{ ticks:{{ callback: function(v) {{ if(v>=1e6) return 'R'+(v/1e6).toFixed(0)+'M'; if(v>=1e3) return 'R'+(v/1e3).toFixed(0)+'k'; return 'R'+v; }} }} }} }} }} }});

new Chart('revChart', {{ type:'bar', data:{{ labels:{rev_labels}, datasets:[ {{ label:'% of customers', data:{rev_custs}, backgroundColor:'#607D8B', borderRadius:6 }}, {{ label:'% of revenue', data:{rev_revenue}, backgroundColor:'#2E75B6', borderRadius:6 }} ] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customers vs revenue by segment', font:{{ size:14, weight:600 }} }} }}, scales:{{ y:{{ beginAtZero:true, title:{{ display:true, text:'%' }} }} }} }} }});

new Chart('churnPie', {{ type:'doughnut', data:{{ labels:{churn_labels}, datasets:[{{ data:{churn_custs}, backgroundColor:RC, borderWidth:2, borderColor:'#fff' }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customers by risk level', font:{{ size:14, weight:600 }} }} }} }} }});

new Chart('churnSpend', {{ type:'bar', data:{{ labels:{churn_labels}, datasets:[{{ label:'Spend at risk (R millions)', data:{churn_spend_m}, backgroundColor:RC, borderRadius:6 }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Spend at risk by level (R millions)', font:{{ size:14, weight:600 }} }}, legend:{{ display:false }} }}, scales:{{ y:{{ ticks:{{ callback: function(v) {{ return 'R'+v.toLocaleString()+'M'; }} }} }} }} }} }});

new Chart('behChart', {{ type:'bar', data:{{ labels:{beh_labels}, datasets:[ {{ label:'Morning %', data:{beh_morning}, backgroundColor:'#FF9800', borderRadius:4 }}, {{ label:'Afternoon %', data:{beh_afternoon}, backgroundColor:'#2E75B6', borderRadius:4 }}, {{ label:'Evening %', data:{beh_evening}, backgroundColor:'#4CAF50', borderRadius:4 }} ] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Time-of-day shopping patterns by segment', font:{{ size:14, weight:600 }} }} }}, scales:{{ y:{{ beginAtZero:true, title:{{ display:true, text:'%' }} }} }} }} }});

new Chart('trendChart', {{ type:'line', data:{{ labels:{trend_labels}, datasets:[{{ label:'Total spend (R millions)', data:{trend_spend_m}, borderColor:'#2E75B6', backgroundColor:'rgba(46,117,182,0.1)', borderWidth:3, fill:true, tension:0.3, pointRadius:4, pointBackgroundColor:'#2E75B6' }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Monthly spend trend — {top_cat_name}', font:{{ size:14, weight:600 }} }} }}, scales:{{ y:{{ ticks:{{ callback: function(v) {{ return 'R'+v+'M'; }} }} }} }} }} }});

new Chart('ageChart', {{ type:'bar', data:{{ labels:{age_labels}, datasets:[{{ label:'Customers', data:{age_custs}, backgroundColor:'#2E75B6', borderRadius:6 }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customers by age group', font:{{ size:14, weight:600 }} }}, legend:{{ display:false }} }}, scales:{{ y:{{ ticks:{{ callback: function(v) {{ if(v>=1e6) return (v/1e6).toFixed(1)+'M'; if(v>=1e3) return (v/1e3).toFixed(0)+'k'; return v; }} }} }} }} }} }});

new Chart('genderChart', {{ type:'doughnut', data:{{ labels:{gender_labels}, datasets:[{{ data:{gender_custs}, backgroundColor:['#2E75B6','#E91E63','#607D8B'], borderWidth:2, borderColor:'#fff' }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customers by gender', font:{{ size:14, weight:600 }} }} }} }} }});

new Chart('geoChart', {{ type:'bar', data:{{ labels:{geo_labels}, datasets:[{{ label:'Spend (R millions)', data:{geo_spend_m}, backgroundColor:'#2E75B6', borderRadius:6 }}] }}, options:{{ responsive:true, maintainAspectRatio:false, indexAxis:'y', plugins:{{ title:{{ display:true, text:'Spend by province (R millions)', font:{{ size:14, weight:600 }} }}, legend:{{ display:false }} }}, scales:{{ x:{{ ticks:{{ callback: function(v) {{ return 'R'+v.toLocaleString()+'M'; }} }} }} }} }} }});

new Chart('retChart', {{ type:'line', data:{{ labels:{ret_labels}, datasets:[{{ label:'Avg retention %', data:{ret_values}, borderColor:'#2E75B6', backgroundColor:'rgba(46,117,182,0.1)', borderWidth:3, fill:true, tension:0.3, pointRadius:6, pointBackgroundColor:'#2E75B6' }}] }}, options:{{ responsive:true, maintainAspectRatio:false, plugins:{{ title:{{ display:true, text:'Customer retention over time', font:{{ size:14, weight:600 }} }} }}, scales:{{ x:{{ title:{{ display:true, text:'Months since first transaction' }} }}, y:{{ beginAtZero:true, max:100, title:{{ display:true, text:'Retention %' }} }} }} }} }});
</script>

</body>
</html>"""

with open(HTML_OUT, 'w') as f:
    f.write(html)

print(f'\nsaved: {HTML_OUT}')

# -- generate PDF using playwright if available --
try:
    from playwright.sync_api import sync_playwright
    print('generating PDF...')

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1200, 'height': 900})
        page.goto(f'file://{os.path.abspath(HTML_OUT)}', wait_until='networkidle')
        import time
        time.sleep(3)  # let charts render
        page.pdf(path=PDF_OUT, format='A4', print_background=True,
                 margin={'top': '10mm', 'bottom': '10mm', 'left': '10mm', 'right': '10mm'})
        browser.close()

    print(f'saved: {PDF_OUT}')
    print(f'\nboth files ready to share')

except ImportError:
    print(f'\nto also generate PDF: pip install playwright && playwright install chromium')
    print(f'then rerun this script')
except Exception as e:
    print(f'\nPDF generation failed: {e}')
    print(f'HTML report is still available: {HTML_OUT}')
