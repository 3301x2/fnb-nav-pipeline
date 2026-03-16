"""
generate_report.py
pulls key insights from BigQuery and generates a beautiful HTML report
uses Chart.js for visuals, opens in any browser

usage:
    python scripts/generate_report.py
    BQ_PROJECT=fmn-production python scripts/generate_report.py

output: insights_report.html in the repo root

requires: pip install google-cloud-bigquery db-dtypes pandas
"""

import os
import json
from datetime import datetime
from google.cloud import bigquery
import pandas as pd

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
LOCATION = 'africa-south1'
client = bigquery.Client(project=PROJECT, location=LOCATION)
OUTPUT = 'insights_report.html'

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

print(f'pulling data from {PROJECT}...')

# -- pull all data upfront --

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

print('  category health')
categories = safe_query(f"""
    SELECT CATEGORY_TWO, ROUND(total_spend, 0) AS total_spend,
           growth_pct, avg_churn_pct, health_status,
           top_destination_name, top_dest_market_share
    FROM `{PROJECT}.marts.mart_category_scorecard`
    WHERE growth_pct IS NOT NULL
    ORDER BY total_spend DESC
    LIMIT 20
""")

print('  pitch opportunities')
pitches = safe_query(f"""
    SELECT DESTINATION, CATEGORY_TWO, market_share_pct,
           ROUND(addressable_market, 0) AS addressable_market,
           ROUND(pitch_score, 1) AS pitch_score,
           recommended_action
    FROM `{PROJECT}.marts.mart_pitch_opportunities`
    ORDER BY pitch_score DESC
    LIMIT 15
""")

print('  cohort retention')
retention = safe_query(f"""
    SELECT months_since_first,
           ROUND(AVG(retention_pct), 1) AS avg_retention
    FROM `{PROJECT}.marts.mart_cohort_retention`
    WHERE cohort_size >= 1000
      AND months_since_first IN (1, 2, 3, 6, 9, 12)
    GROUP BY months_since_first
    ORDER BY months_since_first
""")

print('  cross-sell')
affinity = safe_query(f"""
    SELECT category_a, category_b, shared_customers, lift,
           pct_a_also_shops_b
    FROM `{PROJECT}.marts.mart_category_affinity`
    WHERE lift > 1.2
    ORDER BY shared_customers DESC
    LIMIT 12
""")

# -- build the HTML --

def to_json(df, col):
    if df is None: return '[]'
    return json.dumps(df[col].tolist())

def to_json_str(df, col):
    if df is None: return '[]'
    return json.dumps([str(x) for x in df[col].tolist()])

seg_labels = to_json_str(segments, 'segment_name') if segments is not None else '[]'
seg_counts = to_json(segments, 'customer_count') if segments is not None else '[]'
seg_spend = to_json(segments, 'avg_total_spend') if segments is not None else '[]'

rev_labels = to_json_str(revenue, 'segment_name') if revenue is not None else '[]'
rev_custs = to_json(revenue, 'pct_customers') if revenue is not None else '[]'
rev_revenue = to_json(revenue, 'pct_revenue') if revenue is not None else '[]'

churn_labels = to_json_str(churn, 'churn_risk_level') if churn is not None else '[]'
churn_custs = to_json(churn, 'customers') if churn is not None else '[]'
churn_spend = to_json(churn, 'total_spend') if churn is not None else '[]'

ret_labels = to_json_str(retention, 'months_since_first') if retention is not None else '[]'
ret_values = to_json(retention, 'avg_retention') if retention is not None else '[]'

# overview metrics
overview_html = ''
if overview is not None:
    for _, row in overview.iterrows():
        val = int(row['value'])
        overview_html += f'<div class="metric-card"><div class="metric-value">{val:,}</div><div class="metric-label">{row["metric"]}</div></div>\n'

# segments table
seg_table = ''
if segments is not None:
    for _, row in segments.iterrows():
        seg_table += f'<tr><td><strong>{row["segment_name"]}</strong></td><td>{row["customer_count"]:,}</td><td>{row["pct_of_total"]}%</td><td>{fmt_r(row["avg_total_spend"])}</td><td>{fmt_r(row["total_segment_spend"])}</td></tr>\n'

# churn headline
churn_headline = ''
if churn is not None:
    crit_high = churn[churn['churn_risk_level'].isin(['Critical', 'High'])]
    at_risk_custs = crit_high['customers'].sum()
    at_risk_spend = crit_high['total_spend'].sum()
    total_custs = churn['customers'].sum()
    churn_headline = f'{at_risk_custs:,} customers ({at_risk_custs*100/total_custs:.1f}%) are Critical or High risk, representing {fmt_r(at_risk_spend)} in spend'

# pitch table
pitch_table = ''
if pitches is not None:
    for _, row in pitches.iterrows():
        action_class = row['recommended_action'].split(' - ')[0].lower().replace(' ', '-')
        pitch_table += f'<tr><td><strong>{row["DESTINATION"]}</strong></td><td>{row["CATEGORY_TWO"]}</td><td>{row["market_share_pct"]:.1f}%</td><td>{fmt_r(row["addressable_market"])}</td><td>{row["pitch_score"]}</td><td><span class="badge badge-{action_class}">{row["recommended_action"].split(" - ")[0]}</span></td></tr>\n'

# category health table
cat_table = ''
if categories is not None:
    for _, row in categories.iterrows():
        health_class = row['health_status'].lower()
        growth = f'+{row["growth_pct"]:.1f}%' if row['growth_pct'] > 0 else f'{row["growth_pct"]:.1f}%'
        cat_table += f'<tr><td><strong>{row["CATEGORY_TWO"]}</strong></td><td>{fmt_r(row["total_spend"])}</td><td>{growth}</td><td>{row["avg_churn_pct"]:.1f}%</td><td>{row["top_destination_name"]}</td><td><span class="badge badge-{health_class}">{row["health_status"]}</span></td></tr>\n'

# affinity table
affinity_table = ''
if affinity is not None:
    for _, row in affinity.iterrows():
        affinity_table += f'<tr><td>{row["category_a"]}</td><td>{row["category_b"]}</td><td>{int(row["shared_customers"]):,}</td><td><strong>{row["lift"]:.1f}x</strong></td><td>{row["pct_a_also_shops_b"]:.0f}%</td></tr>\n'

# champion headline
champ_line = ''
if revenue is not None:
    champ = revenue[revenue['segment_name'] == 'Champions']
    if not champ.empty:
        c = champ.iloc[0]
        champ_line = f'Champions are {c["pct_customers"]}% of customers but drive {c["pct_revenue"]}% of revenue'

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
    body {{ font-family: 'Inter', -apple-system, sans-serif; background: #f8f9fa; color: #333; }}

    .header {{
        background: linear-gradient(135deg, #1a365d 0%, #2E75B6 100%);
        color: white; padding: 50px 40px; text-align: center;
    }}
    .header h1 {{ font-size: 2.5rem; font-weight: 700; margin-bottom: 8px; }}
    .header p {{ font-size: 1.1rem; opacity: 0.85; }}
    .header .date {{ margin-top: 15px; font-size: 0.9rem; opacity: 0.7; }}

    .container {{ max-width: 1200px; margin: 0 auto; padding: 30px 20px; }}

    .section {{
        background: white; border-radius: 12px; padding: 35px;
        margin-bottom: 30px; box-shadow: 0 2px 12px rgba(0,0,0,0.06);
    }}
    .section h2 {{
        font-size: 1.5rem; color: #1a365d; margin-bottom: 8px;
        border-bottom: 3px solid #2E75B6; padding-bottom: 10px; display: inline-block;
    }}
    .section .subtitle {{ color: #666; margin-bottom: 25px; font-size: 0.95rem; }}

    .metrics-grid {{
        display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px; margin-top: 20px;
    }}
    .metric-card {{
        background: linear-gradient(135deg, #f0f7ff 0%, #e8f4fd 100%);
        border-radius: 10px; padding: 25px; text-align: center;
        border-left: 4px solid #2E75B6;
    }}
    .metric-value {{ font-size: 1.8rem; font-weight: 700; color: #1a365d; }}
    .metric-label {{ font-size: 0.85rem; color: #666; margin-top: 5px; }}

    .chart-row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-top: 20px; }}
    .chart-box {{ position: relative; height: 350px; }}
    @media (max-width: 768px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}

    .headline {{
        background: linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%);
        border-left: 4px solid #FF9800; border-radius: 8px;
        padding: 18px 25px; margin: 20px 0; font-size: 1.05rem; font-weight: 500;
    }}
    .headline.danger {{
        background: linear-gradient(135deg, #ffebee 0%, #ffcdd2 100%);
        border-left-color: #f44336;
    }}

    table {{ width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 0.9rem; }}
    th {{ background: #1a365d; color: white; padding: 12px 15px; text-align: left; font-weight: 600; }}
    td {{ padding: 10px 15px; border-bottom: 1px solid #eee; }}
    tr:hover {{ background: #f8f9fa; }}

    .badge {{
        display: inline-block; padding: 4px 12px; border-radius: 20px;
        font-size: 0.8rem; font-weight: 600;
    }}
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

    .footer {{
        text-align: center; padding: 30px; color: #999; font-size: 0.85rem;
    }}

    @media print {{
        .section {{ break-inside: avoid; page-break-inside: avoid; }}
        body {{ background: white; }}
    }}
</style>
</head>
<body>

<div class="header">
    <h1>FNB NAV Data Platform</h1>
    <p>Analytics Insights Report</p>
    <div class="date">Generated {datetime.now().strftime('%d %B %Y')} &middot; Project: {PROJECT} &middot; Confidential</div>
</div>

<div class="container">

    <div class="section">
        <h2>Pipeline Overview</h2>
        <p class="subtitle">Scale of data processed by the analytics pipeline</p>
        <div class="metrics-grid">
            {overview_html}
        </div>
    </div>

    <div class="section">
        <h2>Customer Segments</h2>
        <p class="subtitle">K-means ML model groups customers into 5 behavioural segments based on spending patterns</p>
        <div class="headline">{champ_line}</div>
        <div class="chart-row">
            <div class="chart-box"><canvas id="segPie"></canvas></div>
            <div class="chart-box"><canvas id="segSpend"></canvas></div>
        </div>
        <table>
            <tr><th>Segment</th><th>Customers</th><th>% of total</th><th>Avg spend</th><th>Total spend</th></tr>
            {seg_table}
        </table>
    </div>

    <div class="section">
        <h2>Revenue Concentration</h2>
        <p class="subtitle">Who drives the revenue? Small group of customers, outsized impact.</p>
        <div class="chart-row">
            <div class="chart-box" style="grid-column: span 2;"><canvas id="revChart"></canvas></div>
        </div>
    </div>

    <div class="section">
        <h2>Churn Risk</h2>
        <p class="subtitle">ML model scores every customer with a churn probability (0-100%)</p>
        <div class="headline danger">{churn_headline}</div>
        <div class="chart-row">
            <div class="chart-box"><canvas id="churnPie"></canvas></div>
            <div class="chart-box"><canvas id="churnSpend"></canvas></div>
        </div>
    </div>

    <div class="section">
        <h2>Customer Retention</h2>
        <p class="subtitle">Of customers who first transacted in a given month, what % are still active after N months?</p>
        <div class="chart-row">
            <div class="chart-box" style="grid-column: span 2;"><canvas id="retChart"></canvas></div>
        </div>
    </div>

    <div class="section">
        <h2>Category Health</h2>
        <p class="subtitle">Top 20 categories by spend with growth trend, churn exposure, and market leader</p>
        <table>
            <tr><th>Category</th><th>Total spend</th><th>Growth</th><th>Churn risk</th><th>Leader</th><th>Health</th></tr>
            {cat_table}
        </table>
    </div>

    <div class="section">
        <h2>Top Pitch Opportunities</h2>
        <p class="subtitle">Ranked by composite score: market size, growth gap, churn risk, spend efficiency</p>
        <table>
            <tr><th>Destination</th><th>Category</th><th>Market share</th><th>Addressable</th><th>Score</th><th>Action</th></tr>
            {pitch_table}
        </table>
    </div>

    <div class="section">
        <h2>Cross-Sell Opportunities</h2>
        <p class="subtitle">Categories commonly shopped together. Lift > 1 = more likely than random chance.</p>
        <table>
            <tr><th>Category A</th><th>Category B</th><th>Shared customers</th><th>Lift</th><th>% A also in B</th></tr>
            {affinity_table}
        </table>
    </div>

</div>

<div class="footer">
    FNB NAV Data Platform &middot; Built by Prosper Sikhwari &middot; {datetime.now().strftime('%B %Y')}
</div>

<script>
Chart.defaults.font.family = 'Inter, -apple-system, sans-serif';
Chart.defaults.plugins.legend.labels.usePointStyle = true;

const COLORS = ['#2E75B6', '#4CAF50', '#FF9800', '#607D8B', '#f44336', '#9C27B0', '#00BCD4'];
const RISK_COLORS = ['#f44336', '#FF9800', '#fbc02d', '#4CAF50', '#2196f3'];

new Chart('segPie', {{
    type: 'doughnut',
    data: {{
        labels: {seg_labels},
        datasets: [{{ data: {seg_counts}, backgroundColor: COLORS, borderWidth: 2, borderColor: '#fff' }}]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ title: {{ display: true, text: 'Customer distribution', font: {{ size: 14, weight: 600 }} }} }}
    }}
}});

new Chart('segSpend', {{
    type: 'bar',
    data: {{
        labels: {seg_labels},
        datasets: [{{ label: 'Avg spend (R)', data: {seg_spend}, backgroundColor: COLORS, borderRadius: 6 }}]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false, indexAxis: 'y',
        plugins: {{ title: {{ display: true, text: 'Average spend per segment', font: {{ size: 14, weight: 600 }} }}, legend: {{ display: false }} }}
    }}
}});

new Chart('revChart', {{
    type: 'bar',
    data: {{
        labels: {rev_labels},
        datasets: [
            {{ label: '% of customers', data: {rev_custs}, backgroundColor: '#607D8B', borderRadius: 6 }},
            {{ label: '% of revenue', data: {rev_revenue}, backgroundColor: '#2E75B6', borderRadius: 6 }}
        ]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ title: {{ display: true, text: 'Customers vs revenue by segment', font: {{ size: 14, weight: 600 }} }} }},
        scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: '%' }} }} }}
    }}
}});

new Chart('churnPie', {{
    type: 'doughnut',
    data: {{
        labels: {churn_labels},
        datasets: [{{ data: {churn_custs}, backgroundColor: RISK_COLORS, borderWidth: 2, borderColor: '#fff' }}]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ title: {{ display: true, text: 'Customers by risk level', font: {{ size: 14, weight: 600 }} }} }}
    }}
}});

new Chart('churnSpend', {{
    type: 'bar',
    data: {{
        labels: {churn_labels},
        datasets: [{{ label: 'Spend at risk (R)', data: {churn_spend}, backgroundColor: RISK_COLORS, borderRadius: 6 }}]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ title: {{ display: true, text: 'Spend at risk by level', font: {{ size: 14, weight: 600 }} }}, legend: {{ display: false }} }}
    }}
}});

new Chart('retChart', {{
    type: 'line',
    data: {{
        labels: {ret_labels},
        datasets: [{{
            label: 'Avg retention %',
            data: {ret_values},
            borderColor: '#2E75B6', backgroundColor: 'rgba(46,117,182,0.1)',
            borderWidth: 3, fill: true, tension: 0.3,
            pointRadius: 6, pointBackgroundColor: '#2E75B6'
        }}]
    }},
    options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{ title: {{ display: true, text: 'Customer retention over time', font: {{ size: 14, weight: 600 }} }} }},
        scales: {{
            x: {{ title: {{ display: true, text: 'Months since first transaction' }} }},
            y: {{ beginAtZero: true, max: 100, title: {{ display: true, text: 'Retention %' }} }}
        }}
    }}
}});
</script>

</body>
</html>"""

with open(OUTPUT, 'w') as f:
    f.write(html)

print(f'\nsaved: {OUTPUT}')
print('open it in any browser, or print to PDF with Ctrl+P / Cmd+P')
