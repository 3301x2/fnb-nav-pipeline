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

import os, json, sys, base64
from datetime import datetime
from pathlib import Path

CACHED = '--cached' in sys.argv or '--cache' in sys.argv
PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
OUT = 'nav_dashboard.html'
CACHE_FILE = 'nav_dashboard_cache.json'

if not CACHED:
    from google.cloud import bigquery
    import pandas as pd
    bq = bigquery.Client(project=PROJECT, location='africa-south1')

# ── Load brand config ──
SCRIPT_DIR = Path(__file__).parent
BRAND_FILE = SCRIPT_DIR.parent / 'assets' / 'brand.json'

brand = {
    'brand_name': 'NAV Analytics',
    'tagline': 'Data & Media Network',
    'colors': {
        'header_bg': '#0f172a',
        'header_bg_gradient': '#1e3a5f',
        'accent': '#1e3a5f',
        'chart_primary': '#0f172a',
        'chart_secondary': '#2E75B6',
        'chart_palette': ['#0f172a','#1e3a5f','#2E75B6','#4CAF50','#FF9800','#f44336','#9C27B0','#00BCD4'],
        'tab_active': '#1e3a5f',
        'badge_highlight': '#1e3a5f'
    },
    'confidential': True
}

if BRAND_FILE.exists():
    with open(BRAND_FILE) as f:
        user_brand = json.load(f)
        brand.update(user_brand)
        if 'colors' in user_brand:
            brand['colors'] = {**brand['colors'], **user_brand['colors']}
    print(f'  Brand: {brand["brand_name"]} (loaded from {BRAND_FILE})')
else:
    print(f'  Brand: default (no assets/brand.json found)')

# Embed logo as base64 (resize if huge)
logo_b64 = ''
LOGO_FILE = SCRIPT_DIR.parent / 'assets' / 'logo.png'
if LOGO_FILE.exists():
    try:
        from PIL import Image
        import io
        img = Image.open(LOGO_FILE)
        if max(img.size) > 800:
            img.thumbnail((800, 800), Image.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format='PNG', optimize=True)
            logo_b64 = base64.b64encode(buf.getvalue()).decode()
            print(f'  Logo: {LOGO_FILE.name} resized to {img.size} ({len(logo_b64)//1024}KB)')
        else:
            with open(LOGO_FILE, 'rb') as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            print(f'  Logo: {LOGO_FILE.name} ({len(logo_b64)//1024}KB)')
    except ImportError:
        with open(LOGO_FILE, 'rb') as f:
            logo_b64 = base64.b64encode(f.read()).decode()
        print(f'  Logo: {LOGO_FILE.name} ({len(logo_b64)//1024}KB) — install Pillow to auto-resize')
else:
    print(f'  Logo: none (put logo.png in assets/ folder)')

# Embed bottom banner as base64
low_b64 = ''
LOW_FILE = SCRIPT_DIR.parent / 'assets' / 'low.png'
if LOW_FILE.exists():
    with open(LOW_FILE, 'rb') as f:
        low_b64 = base64.b64encode(f.read()).decode()
    print(f'  Banner: {LOW_FILE.name} ({len(low_b64)//1024}KB base64)')
else:
    print(f'  Banner: none (put low.png in assets/ folder)')

# Embed background logo as base64 (faint)
bg_logo_b64 = ''
BG_LOGO_FILE = SCRIPT_DIR.parent / 'assets' / 'logo3.png'
if BG_LOGO_FILE.exists():
    try:
        from PIL import Image, ImageEnhance
        import io
        img = Image.open(BG_LOGO_FILE).convert('RGBA')
        # Make it very faint — 8% opacity
        alpha = img.split()[3]
        alpha = alpha.point(lambda p: int(p * 0.08))
        img.putalpha(alpha)
        buf = io.BytesIO()
        img.save(buf, format='PNG', optimize=True)
        bg_logo_b64 = base64.b64encode(buf.getvalue()).decode()
        print(f'  BG Logo: {BG_LOGO_FILE.name} faded to 8% ({len(bg_logo_b64)//1024}KB)')
    except ImportError:
        with open(BG_LOGO_FILE, 'rb') as f:
            bg_logo_b64 = base64.b64encode(f.read()).decode()
        print(f'  BG Logo: {BG_LOGO_FILE.name} ({len(bg_logo_b64)//1024}KB) — install Pillow for fading')
else:
    print(f'  BG Logo: none (put logo3.png in assets/ folder)')

BC = brand['colors']

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
# PULL ALL DATA (or load from cache)
# ═══════════════════════════════════════════════════════════════
if CACHED and os.path.exists(CACHE_FILE):
    print(f'Loading from cache: {CACHE_FILE} (no BQ queries, no cost)')
    with open(CACHE_FILE) as f:
        data_json = f.read()
    cats = sorted(set(b['CATEGORY_TWO'] for b in json.loads(data_json).get('benchmarks', [])))
    print(f'  {len(data_json)//1024}KB, {len(cats)} categories')
    # Override safe so query lines below are harmless no-ops
    def safe(sql): return None
    def q(sql): return None
elif CACHED:
    print(f'ERROR: no cache file found. Run once without --cached first.')
    sys.exit(1)
else:
    data_json = None

print(f'Pulling data from {PROJECT}...' if not CACHED else '  Skipping queries (cached)')

print('  date range')
date_range = safe(f"""
    SELECT MIN(EFF_DATE) AS data_from, MAX(EFF_DATE) AS data_to,
        DATE_DIFF(MAX(EFF_DATE), MIN(EFF_DATE), DAY) AS days_span,
        FORMAT_DATE('%d %b %Y', MIN(EFF_DATE)) AS from_str,
        FORMAT_DATE('%d %b %Y', MAX(EFF_DATE)) AS to_str,
        FORMAT_DATE('%d %b %Y', DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH)) AS analysis_from_str
    FROM `{PROJECT}.staging.stg_transactions`
    WHERE EFF_DATE >= DATE_SUB(
        (SELECT MAX(EFF_DATE) FROM `{PROJECT}.staging.stg_transactions`),
        INTERVAL 12 MONTH)
""")

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

print('  revenue concentration (FNB-wide baseline)')
revenue = safe(f"""
    SELECT segment_name,
        ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct_cust,
        ROUND(SUM(val_trns)*100.0/SUM(SUM(val_trns)) OVER(),1) AS pct_rev
    FROM `{PROJECT}.marts.mart_cluster_output` GROUP BY 1
""")

print('  per-client segment mix (the fix — different numbers per client)')
client_segment_mix = safe(f"""
    SELECT DESTINATION, CATEGORY_TWO, segment_name,
        segment_customers, segment_spend,
        client_total_customers, client_total_spend,
        pct_of_client_customers AS pct_cust,
        pct_of_client_spend     AS pct_rev,
        fnb_pct_of_customers,
        index_vs_fnb
    FROM `{PROJECT}.marts.mart_client_segment_mix`
""")

print('  audience × client overlap (which audiences are MY customers in)')
aud_client_overlap = safe(f"""
    SELECT DESTINATION, CATEGORY_TWO, audience_id, audience_name, audience_type,
        overlap_customers, client_total_customers, pct_of_client
    FROM `{PROJECT}.marts.mart_audience_client_overlap`
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

print('  audience details (top categories per audience)')
aud_categories = safe(f"""
    SELECT am.audience_id, cs.CATEGORY_TWO,
        COUNT(DISTINCT am.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_audience_members` am
    JOIN `{PROJECT}.analytics.int_customer_category_spend` cs ON am.UNIQUE_ID = cs.UNIQUE_ID
    GROUP BY 1,2
    HAVING customers >= 50
    ORDER BY 1, spend DESC
""")

print('  audience details (top merchants per audience)')
aud_merchants = safe(f"""
    SELECT am.audience_id, cs.DESTINATION,
        COUNT(DISTINCT am.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend),0) AS spend
    FROM `{PROJECT}.marts.mart_audience_members` am
    JOIN `{PROJECT}.analytics.int_customer_category_spend` cs ON am.UNIQUE_ID = cs.UNIQUE_ID
    GROUP BY 1,2
    HAVING customers >= 50
    ORDER BY 1, spend DESC
""")

print('  audience details (age breakdown per audience)')
aud_age = safe(f"""
    SELECT am.audience_id, c.age_group, COUNT(*) AS customers
    FROM `{PROJECT}.marts.mart_audience_members` am
    JOIN `{PROJECT}.staging.stg_customers` c ON am.UNIQUE_ID = c.UNIQUE_ID
    WHERE c.age_group IS NOT NULL
    GROUP BY 1,2 ORDER BY 1,2
""")

print('  audience details (income breakdown per audience)')
aud_income = safe(f"""
    SELECT am.audience_id, c.income_group, COUNT(*) AS customers
    FROM `{PROJECT}.marts.mart_audience_members` am
    JOIN `{PROJECT}.staging.stg_customers` c ON am.UNIQUE_ID = c.UNIQUE_ID
    WHERE c.income_group IS NOT NULL AND c.income_group != 'Unknown'
    GROUP BY 1,2 ORDER BY 1,2
""")

print('  audience details (gender breakdown per audience)')
aud_gender = safe(f"""
    SELECT am.audience_id, c.gender_label, COUNT(*) AS customers
    FROM `{PROJECT}.marts.mart_audience_members` am
    JOIN `{PROJECT}.staging.stg_customers` c ON am.UNIQUE_ID = c.UNIQUE_ID
    WHERE c.gender_label IS NOT NULL AND c.gender_label != 'Unknown'
    GROUP BY 1,2 ORDER BY 1,2
""")

print('  audience details (province — using catalog top_province)')
# Province per audience is already in the catalog as top_province.
# For a fuller breakdown we'd need to scan stg_transactions which is expensive (~R30+).
# So we build a simple breakdown from the geographic audiences we already know,
# and show top_province for the rest.
aud_province = safe(f"""
    SELECT am.audience_id, am.audience_name, am.audience_type,
        CASE
            WHEN am.audience_id = 'G01' THEN 'GAUTENG'
            WHEN am.audience_id = 'G02' THEN 'WESTERN CAPE'
            WHEN am.audience_id = 'G03' THEN 'KWAZULU-NATAL'
            WHEN am.audience_id = 'G04' THEN 'EASTERN CAPE'
            ELSE am.top_province
        END AS PROVINCE,
        am.audience_size AS customers,
        ROUND(am.avg_spend * am.audience_size, 0) AS spend
    FROM `{PROJECT}.marts.mart_audience_catalog` am
""")

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
if data_json is None:
    print('\nSerializing...')
    ov = dict(zip(overview['k'], overview['v'])) if overview is not None else {}

    data_json = json.dumps({
        'overview': ov,
        'date_range': json.loads(to_json(date_range)),
        'brand': brand,
        'benchmarks': json.loads(to_json(benchmarks)),
        'profiles': json.loads(to_json(profiles)),
        'summary': json.loads(to_json(summary)),
        'revenue': json.loads(to_json(revenue)),
        'client_segment_mix': json.loads(to_json(client_segment_mix)),
        'aud_client_overlap': json.loads(to_json(aud_client_overlap)),
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
        'aud_categories': json.loads(to_json(aud_categories)),
        'aud_merchants': json.loads(to_json(aud_merchants)),
        'aud_age': json.loads(to_json(aud_age)),
        'aud_income': json.loads(to_json(aud_income)),
        'aud_gender': json.loads(to_json(aud_gender)),
        'aud_province': json.loads(to_json(aud_province)),
        'demo': json.loads(to_json(demo)),
        'geo': json.loads(to_json(geo)),
        'trends': json.loads(to_json(trends)),
        'loyalty': json.loads(to_json(loyalty)),
        'timepatterns': json.loads(to_json(timepatterns)),
    }, default=str)

    size_mb = len(data_json) / 1024 / 1024
    print(f'  Data size: {size_mb:.1f} MB')

    with open(CACHE_FILE, 'w') as f:
        f.write(data_json)
    print(f'  Cached to: {CACHE_FILE}')
    print(f'  Next time use: python3 scripts/generate_dashboard.py --cached')

    cats = sorted(benchmarks['CATEGORY_TWO'].unique().tolist()) if benchmarks is not None else []

now = datetime.now().strftime('%d %B %Y')

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
body{{font-family:'DM Sans',sans-serif;background-color:#f8fafc;{"" if not bg_logo_b64 else "background-image:url(data:image/png;base64," + bg_logo_b64 + ");background-repeat:space;background-size:500px;"}color:#1a202c}}
#hdr{{background:linear-gradient(135deg,{BC['header_bg']},{BC['header_bg_gradient']});color:#fff;padding:0 24px;display:flex;align-items:center;gap:12px;height:42px;overflow:hidden}}
#hdr h1{{font-size:1.05rem;font-weight:600;white-space:nowrap}}
#hdr .meta{{font-size:.68rem;opacity:.7;margin-left:auto;text-align:right;line-height:1.3;white-space:nowrap}}
.tabs{{display:flex;background:#fff;border-bottom:1px solid #e2e8f0;padding:0 16px;overflow-x:auto}}
.tab{{padding:7px 14px;font-size:.8rem;color:#64748b;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap;font-weight:500}}
.tab:hover{{color:#1e3a5f;background:#f8fafc}}
.tab.a{{color:{BC['tab_active']};border-bottom-color:{BC['tab_active']}}}
.filters{{background:#fff;padding:10px 24px;border-bottom:1px solid #e2e8f0;display:flex;gap:12px;flex-wrap:wrap;align-items:center}}
.filters label{{font-size:.78rem;color:#64748b;font-weight:500}}
.filters select{{padding:5px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.82rem;font-family:inherit;min-width:160px}}
.tog{{display:inline-flex;border:1px solid #d1d5db;border-radius:6px;overflow:hidden}}
.tog span{{padding:5px 14px;font-size:.78rem;cursor:pointer;color:#64748b}}
.tog span.on{{background:{BC['accent']};color:#fff}}
.pg{{display:none;padding:20px 24px;max-width:1300px;margin:0 auto}}.pg.a{{display:block}}
.aud-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:14px;margin-top:12px}}
.aud-card{{background:#fff;border-radius:12px;border:1px solid #f1f5f9;overflow:hidden;transition:box-shadow .2s,transform .15s}}
.aud-card:hover{{box-shadow:0 4px 20px rgba(0,0,0,.08);transform:translateY(-2px)}}
.aud-top{{height:8px}}
.aud-body{{padding:16px 18px 14px}}
.aud-type{{font-size:.68rem;font-weight:600;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}}
.aud-name{{font-size:1rem;font-weight:600;color:#0f172a;margin-bottom:4px}}
.aud-desc{{font-size:.78rem;color:#64748b;line-height:1.5;margin-bottom:12px;min-height:36px}}
.aud-stats{{display:flex;gap:14px;margin-bottom:12px;flex-wrap:wrap}}
.aud-stat{{text-align:center}}
.aud-stat .n{{font-size:1.1rem;font-weight:600;color:#0f172a}}
.aud-stat .l{{font-size:.65rem;color:#94a3b8}}
.aud-channels{{display:flex;gap:6px;align-items:center;margin-bottom:10px}}
.aud-ch{{background:#f1f5f9;border-radius:4px;padding:3px 8px;font-size:.68rem;font-weight:600;color:#475569}}
.aud-ch.meta{{background:#1877f2;color:#fff}}.aud-ch.goog{{background:#ea4335;color:#fff}}.aud-ch.tik{{background:#000;color:#fff}}
.aud-tags{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:10px}}
.aud-tag{{padding:2px 8px;border-radius:10px;font-size:.68rem;font-weight:500}}
.aud-demog{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;padding:10px 0;border-top:1px solid #f1f5f9;margin-top:8px}}
.aud-demog .di{{text-align:center}}
.aud-demog .di .dv{{font-size:.85rem;font-weight:600;color:#0f172a}}
.aud-demog .di .dl{{font-size:.62rem;color:#94a3b8}}
.aud-use{{background:#f8fafc;border-top:1px solid #f1f5f9;padding:10px 18px;font-size:.75rem;color:#64748b}}
.aud-use strong{{color:#1e3a5f}}
.aud-filters{{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}}
.aud-filters label{{font-size:.78rem;color:#64748b;font-weight:500}}
.aud-filters select{{padding:5px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.82rem;font-family:inherit}}
.aud-filters input{{padding:5px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.82rem;font-family:inherit;width:180px}}
.aud-count{{font-size:.82rem;color:#94a3b8;margin-left:auto}}
.modal-overlay{{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.5);z-index:100;justify-content:center;align-items:flex-start;padding:40px 20px;overflow-y:auto}}
.modal-overlay.show{{display:flex}}
.modal{{background:#fff;border-radius:16px;max-width:900px;width:100%;max-height:calc(100vh - 80px);overflow-y:auto;box-shadow:0 20px 60px rgba(0,0,0,.2)}}
.modal-hdr{{position:sticky;top:0;background:#fff;padding:20px 24px 16px;border-bottom:1px solid #f1f5f9;display:flex;justify-content:space-between;align-items:flex-start;z-index:1;border-radius:16px 16px 0 0}}
.modal-hdr h2{{font-size:1.2rem;font-weight:700;color:#0f172a;margin:0}}
.modal-hdr .mtype{{font-size:.72rem;font-weight:600;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}}
.modal-hdr .mdesc{{font-size:.82rem;color:#64748b;margin-top:4px;line-height:1.5}}
.modal-close{{background:none;border:none;font-size:1.5rem;cursor:pointer;color:#94a3b8;padding:0 4px;line-height:1}}
.modal-close:hover{{color:#0f172a}}
.modal-body{{padding:20px 24px 24px}}
.modal-stats{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:20px}}
.modal-stat{{background:#f8fafc;border-radius:8px;padding:12px;text-align:center}}
.modal-stat .n{{font-size:1.2rem;font-weight:600;color:#0f172a}}
.modal-stat .l{{font-size:.68rem;color:#94a3b8;margin-top:2px}}
.modal-section{{margin-bottom:20px}}
.modal-section h3{{font-size:.85rem;font-weight:600;color:#0f172a;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #f1f5f9}}
.modal-bars{{display:flex;flex-direction:column;gap:6px}}
.modal-bar{{display:flex;align-items:center;gap:8px}}
.modal-bar .lbl{{font-size:.78rem;color:#64748b;width:120px;text-align:right;flex-shrink:0}}
.modal-bar .track{{flex:1;height:20px;background:#f1f5f9;border-radius:4px;overflow:hidden}}
.modal-bar .fill{{height:100%;border-radius:4px;transition:width .3s}}
.modal-bar .val{{font-size:.78rem;color:#0f172a;font-weight:500;width:60px}}
.modal-row{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
@media(max-width:768px){{.modal-row{{grid-template-columns:1fr}}.modal-stats{{grid-template-columns:repeat(3,1fr)}}}}
.modal-channels{{display:flex;gap:8px;margin:12px 0}}
.modal-ch{{padding:6px 16px;border-radius:6px;font-size:.8rem;font-weight:600;color:#fff}}
.aud-card{{cursor:pointer}}
.pg-banner{{width:100%;margin-top:20px;display:block}}
.pg-banner img{{width:100%;height:auto;display:block}}
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
<h1>{brand['brand_name']} {'· ' + brand.get('tagline','') if brand.get('tagline') else ''}</h1>
<span class="meta">{PROJECT} · Generated {now}<br>{'Confidential · ' if brand.get('confidential') else ''}Data period shown on Overview</span>
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
<option value="">All categories</option>
{''.join(f'<option value="{c}">{c}</option>' for c in cats)}
</select>
<label>Client</label>
<select id="fClient" onchange="renderPitch();renderOverview();renderAudiences();"></select>
<label>Competitors</label>
<select id="fTopN" onchange="renderPitch()">
<option value="5">Top 5</option><option value="8" selected>Top 8</option><option value="15">Top 15</option><option value="999">All</option>
</select>
<div class="tog" id="anonToggle">
<span class="on" onclick="setAnon(false)">Internal</span>
<span onclick="setAnon(true)">External</span>
</div>
</div>

<!-- PAGE 0: OVERVIEW -->
<div class="pg a" id="pg0">
<div id="dateRange" style="background:#fff;border-radius:10px;padding:12px 18px;margin-bottom:14px;border:1px solid #f1f5f9;display:flex;gap:24px;align-items:center;flex-wrap:wrap"></div>
<div class="row r5" id="ovKpis"></div>
<div class="row r2">
<div class="sec"><h3>Customers by segment <span id="segPieScope" style="font-size:.72rem;color:#64748b;font-weight:400"></span></h3><div class="chbox"><canvas id="chSegPie"></canvas></div></div>
<div class="sec"><h3>Revenue concentration <span id="revBarScope" style="font-size:.72rem;color:#64748b;font-weight:400"></span></h3><div class="chbox"><canvas id="chRevBar"></canvas></div></div>
</div>
<div class="row r3" id="ovHighlights"></div>
<div class="sec"><h3>Churn risk distribution</h3><div class="chbox"><canvas id="chChurnPie"></canvas></div></div>
</div>

<!-- PAGE 1: CLIENT PITCH -->
<div class="pg" id="pg1">
<div id="cpDate" style="font-size:.78rem;color:#94a3b8;margin-bottom:10px"></div>
<div class="row r4" id="cpKpis"></div>
<div class="row r4" id="cpKpis2"></div>
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
<div style="margin-bottom:4px"><h2 style="font-size:1.3rem;font-weight:700;color:#0f172a;margin:0">Audience Marketplace <span style="font-size:.7rem;font-weight:500;color:#64748b;background:#f1f5f9;padding:3px 8px;border-radius:10px;vertical-align:middle;margin-left:6px">FNB-wide · not filtered by client</span></h2><p style="font-size:.82rem;color:#64748b;margin:4px 0 16px">Pre-packaged audiences ready for activation via LiveRamp → Meta, Google, TikTok. Audience sizes below are across all FNB customers. For <em>this client's</em> overlap with each audience, see the "Top audiences among this client's customers" chart at the bottom of the page.</p></div>
<div class="row r4" id="audKpis"></div>
<div class="aud-filters">
<label>Type</label>
<select id="fAudType" onchange="renderAudiences()">
<option value="">All types</option>
<option value="Demographic">Demographic</option><option value="Lifestyle">Lifestyle</option><option value="Behavioral">Behavioral</option><option value="Seasonal">Seasonal</option><option value="Geographic">Geographic</option><option value="Cross-category">Cross-category</option>
</select>
<label>Min size</label>
<select id="fAudMin" onchange="renderAudiences()">
<option value="0">Any</option><option value="100000">100k+</option><option value="500000">500k+</option><option value="1000000">1M+</option>
</select>
<label>Search</label>
<input id="fAudSearch" type="text" placeholder="Search audiences..." oninput="renderAudiences()">
<span class="aud-count" id="audCount"></span>
</div>
<div class="aud-grid" id="audGrid"></div>

<!-- Per-client overlap — honest reframe: which of these audiences is MY client's customer base in? -->
<div class="sec" style="margin-top:24px"><h3>Top audiences among this client's customers <span id="audOverlapScope" style="font-size:.72rem;color:#64748b;font-weight:400"></span></h3><div id="audOverlapTable"></div></div>
</div>

<!-- AUDIENCE DETAIL MODAL -->
<div class="modal-overlay" id="audModal" onclick="if(event.target===this)closeModal()">
<div class="modal">
<div class="modal-hdr">
<div>
<div class="mtype" id="modalType"></div>
<h2 id="modalName"></h2>
<div class="mdesc" id="modalDesc"></div>
</div>
<button class="modal-close" onclick="closeModal()">&times;</button>
</div>
<div class="modal-body">
<div class="modal-stats" id="modalStats"></div>
<div class="modal-channels" id="modalChannels">
<span class="modal-ch" style="background:#1877f2">Meta</span>
<span class="modal-ch" style="background:#ea4335">Google</span>
<span class="modal-ch" style="background:#000">TikTok</span>
</div>
<div class="modal-row">
<div class="modal-section"><h3>Top categories by spend</h3><div class="modal-bars" id="modalCats"></div></div>
<div class="modal-section"><h3>Top merchants by spend</h3><div class="modal-bars" id="modalMerch"></div></div>
</div>
<div class="modal-row">
<div class="modal-section"><h3>Age distribution</h3><div class="modal-bars" id="modalAge"></div></div>
<div class="modal-section"><h3>Income distribution</h3><div class="modal-bars" id="modalIncome"></div></div>
</div>
<div class="modal-row">
<div class="modal-section"><h3>Gender split</h3><div class="modal-bars" id="modalGender"></div></div>
<div class="modal-section"><h3>Provincial distribution</h3><div class="modal-bars" id="modalProv"></div></div>
</div>
</div>
</div>
</div>

<div class="ftr">{brand['brand_name']} · {PROJECT} · Built by Prosper Sikhwari · {datetime.now().strftime('%B %Y')}</div>

<script>
const D = DASHBOARD_DATA_PLACEHOLDER;
const C = D.brand && D.brand.colors && D.brand.colors.chart_palette ? D.brand.colors.chart_palette : ['#0f172a','#1e3a5f','#2E75B6','#4CAF50','#FF9800','#f44336','#9C27B0','#00BCD4','#607D8B','#795548'];
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
    // Only show filters on Client Pitch
    document.getElementById('filterbar').style.display = (n===1) ? 'flex' : 'none';
    if(n===1) renderPitch();
}}

// ─── Filter logic ───
function onFilter() {{
    const cat = document.getElementById('fCat').value;
    // Get clients sorted by total_spend descending (biggest first)
    // If no category selected (All), show all destinations
    const catBench = cat
        ? D.benchmarks.filter(b=>b.CATEGORY_TWO===cat).sort((a,b)=>b.total_spend-a.total_spend)
        : D.benchmarks.sort((a,b)=>b.total_spend-a.total_spend);
    const clients = catBench.map(b=>b.DESTINATION);
    // Remove duplicates while preserving order
    const uniqueClients = [...new Set(clients)];
    const sel = document.getElementById('fClient');
    const prev = sel.value;
    sel.innerHTML = uniqueClients.map(c=>`<option value="${{c}}">${{c}}</option>`).join('');
    // Keep previous selection if it exists in new category, otherwise pick the #1 by spend
    if(uniqueClients.includes(prev)) {{
        sel.value = prev;
    }} else if(uniqueClients.length > 0) {{
        sel.value = uniqueClients[0];
    }}
    renderPitch();
    renderOverview();    // Overview's segment charts are now client-aware
    renderAudiences();   // Audience overlap section is client × category scoped
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
    // Date range bar
    const dr = (D.date_range && D.date_range.length) ? D.date_range[0] : null;
    if(dr) {{
        document.getElementById('dateRange').innerHTML =
            `<div style="font-size:.82rem"><strong style="color:#0f172a">Analysis period</strong></div>` +
            `<div style="font-size:.82rem;color:#64748b"><strong>${{dr.analysis_from_str || '—'}}</strong> → <strong>${{dr.to_str || '—'}}</strong> (12 months)</div>` +
            `<div style="font-size:.82rem;color:#64748b">Full data: ${{dr.from_str || '—'}} → ${{dr.to_str || '—'}} (${{dr.days_span ? Math.round(dr.days_span/365*10)/10 : '?'}} years)</div>` +
            `<div style="margin-left:auto;font-size:.72rem;color:#94a3b8">Dashboard refreshed: ${{new Date().toLocaleDateString('en-ZA')}}</div>`;
    }} else {{
        document.getElementById('dateRange').innerHTML = '<div style="font-size:.82rem;color:#94a3b8">Date range: loading...</div>';
    }}

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

    // Pick per-client mix if a client × category is selected AND data exists for it;
    // otherwise fall back to FNB-wide (D.profiles / D.revenue).
    const selCat = document.getElementById('fCat') ? document.getElementById('fCat').value : '';
    const selCli = document.getElementById('fClient') ? document.getElementById('fClient').value : '';
    const csm = (D.client_segment_mix || []).filter(r => r.DESTINATION === selCli && r.CATEGORY_TWO === selCat);
    const usingClientMix = csm.length > 0;

    const scopeLabel = usingClientMix
        ? `— ${{selCli}} in ${{selCat}}`
        : `— FNB-wide (select a client + category for a client-specific view)`;
    const segPieScope = document.getElementById('segPieScope');
    const revBarScope = document.getElementById('revBarScope');
    if (segPieScope) segPieScope.textContent = scopeLabel;
    if (revBarScope) revBarScope.textContent = scopeLabel;

    // Segment pie
    const pieRows = usingClientMix
        ? csm.slice().sort((a,b)=>{{const o=['Champions','Loyal High Value','Steady Mid-Tier','At Risk','Dormant'];return o.indexOf(a.segment_name)-o.indexOf(b.segment_name);}})
        : (D.profiles || []);
    const pieLabels = usingClientMix ? pieRows.map(r=>r.segment_name) : pieRows.map(p=>p.segment_name);
    const pieData   = usingClientMix ? pieRows.map(r=>r.segment_customers) : pieRows.map(p=>p.customer_count);
    if(pieLabels.length) {{
        makeChart('chSegPie', {{type:'doughnut',data:{{labels:pieLabels,datasets:[{{data:pieData,backgroundColor:C,borderWidth:2,borderColor:'#fff'}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:false}}}}}}}});
    }}

    // Revenue bar (% customers vs % revenue for each segment)
    const barRows = usingClientMix
        ? csm.slice().sort((a,b)=>b.pct_rev-a.pct_rev)
        : (D.revenue || []);
    if(barRows.length) {{
        makeChart('chRevBar', {{type:'bar',data:{{labels:barRows.map(r=>r.segment_name),datasets:[{{label:'% customers',data:barRows.map(r=>r.pct_cust),backgroundColor:'#94a3b8',borderRadius:4}},{{label:'% revenue',data:barRows.map(r=>r.pct_rev),backgroundColor:'#0f172a',borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{title:{{display:false}}}}}}}});
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
    if(!client) return;

    // Filter helper — if cat is empty (All), don't filter by category
    const byCat = arr => cat ? arr.filter(b=>b.CATEGORY_TWO===cat) : arr;

    // For "All categories" view, aggregate by destination across categories
    let allComps;
    if(!cat) {{
        // Group by destination, sum spend across categories
        const destMap = {{}};
        D.benchmarks.forEach(b => {{
            if(!destMap[b.DESTINATION]) destMap[b.DESTINATION] = {{DESTINATION:b.DESTINATION, CATEGORY_TWO:'All', customers:0, total_spend:0, transactions:0, market_share_pct:0, penetration_pct:0, avg_txn_value:0, spend_per_customer:0, avg_share_of_wallet:0, spend_rank:0, _count:0}};
            const d = destMap[b.DESTINATION];
            d.customers += (b.customers||0);
            d.total_spend += (b.total_spend||0);
            d.transactions += (b.transactions||0);
            d._count++;
        }});
        allComps = Object.values(destMap).sort((a,b)=>b.total_spend-a.total_spend);
        // Calculate rank and averages
        allComps.forEach((d,i) => {{
            d.spend_rank = i+1;
            d.avg_txn_value = d.transactions ? Math.round(d.total_spend/d.transactions) : 0;
            d.spend_per_customer = d.customers ? Math.round(d.total_spend/d.customers) : 0;
            const totalSpend = allComps.reduce((s,x)=>s+x.total_spend,0);
            d.market_share_pct = totalSpend ? Math.round(d.total_spend/totalSpend*1000)/10 : 0;
            const totalCust = allComps.reduce((s,x)=>s+x.customers,0);
            d.penetration_pct = totalCust ? Math.round(d.customers/totalCust*1000)/10 : 0;
        }});
    }} else {{
        allComps = D.benchmarks.filter(b=>b.CATEGORY_TWO===cat).sort((a,b)=>b.total_spend-a.total_spend);
    }}
    // Top N for competitor charts
    let comps = allComps.slice(0, topN);
    // Make sure selected client is always in the competitor list even if outside topN
    if(!comps.find(c=>c.DESTINATION===client)) {{
        const ck = allComps.find(c=>c.DESTINATION===client);
        if(ck) comps.push(ck);
    }}
    // Client data (from full list)
    const ck = allComps.find(c=>c.DESTINATION===client);

    // Date context
    const dr = (D.date_range && D.date_range.length) ? D.date_range[0] : null;
    document.getElementById('cpDate').innerHTML = dr ?
        `📊 Data: <strong>${{dr.analysis_from_str}}</strong> → <strong>${{dr.to_str}}</strong> (12 months) · Last refreshed: ${{new Date().toLocaleDateString('en-ZA')}}` :
        '';

    // KPIs — 2 rows of 4
    if(ck) {{
        document.getElementById('cpKpis').innerHTML =
            card('Customers', num(ck.customers)) +
            card('Total Spend', fmt(ck.total_spend)) +
            card('Market Share', pct(ck.market_share_pct)) +
            card('Penetration', pct(ck.penetration_pct));
        document.getElementById('cpKpis2').innerHTML =
            card('Avg Transaction', fmt(ck.avg_txn_value)) +
            card('Spend/Customer', fmt(ck.spend_per_customer)) +
            card('Rank in Category', '#'+ck.spend_rank) +
            card('Share of Wallet', pct(ck.avg_share_of_wallet));
    }} else {{
        document.getElementById('cpKpis').innerHTML = '<div class="empty">Select a client from the dropdown above</div>';
        document.getElementById('cpKpis2').innerHTML = '';
        return;
    }}

    // Brand accent color for charts
    const accent = (D.brand && D.brand.colors && D.brand.colors.chart_primary) || '#0f172a';

    // Competitor charts
    const labels = comps.map((c,i) => destName(c.DESTINATION, client, i+1));
    const colors = comps.map(c => c.DESTINATION===client ? '#d97706' : accent);

    makeChart('chCompShare', {{type:'bar',data:{{labels,datasets:[{{data:comps.map(c=>c.market_share_pct),backgroundColor:colors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    makeChart('chCompSpc', {{type:'bar',data:{{labels,datasets:[{{data:comps.map(c=>c.spend_per_customer),backgroundColor:colors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{callback:v=>fmt(v)}}}}}}}}}});

    // Loyalty
    const loy = byCat(D.loyalty||[]).sort((a,b)=>b.pct_loyal_50-a.pct_loyal_50).slice(0,topN);
    if(loy.length) {{
        const loyLabels = loy.map(l => destName(l.DESTINATION, client, loy.indexOf(l)+1));
        const loyColors = loy.map(l => l.DESTINATION===client ? '#d97706' : '#1e3a5f');
        makeChart('chLoyalty', {{type:'bar',data:{{labels:loyLabels,datasets:[{{data:loy.map(l=>l.pct_loyal_50),backgroundColor:loyColors,borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{{legend:{{display:false}}}}}}}});
    }}

    // SOW bands
    if(ck && D.loyalty) {{
        const cl = byCat(D.loyalty||[]).find(l=>l.DESTINATION===client);
        if(cl) {{
            makeChart('chSow', {{type:'bar',data:{{labels:['1 store','2 stores','3-4','5-7','8+'],datasets:[{{data:[cl.band_1_store,cl.band_2_stores,cl.band_3_4_stores,cl.band_5_7_stores,cl.band_8_plus],backgroundColor:['#0f172a','#1e3a5f','#2E75B6','#94a3b8','#d1d5db'],borderRadius:4}}]}},options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}},title:{{display:true,text:client+' customer loyalty bands'}}}}}}}});
        }}
    }}

    // Trend
    const catTrends = byCat(D.trends||[]);
    const months = [...new Set(catTrends.map(t=>t.month))].sort();
    const clientTrend = months.map(m => {{ const r = catTrends.find(t=>t.month===m&&t.DESTINATION===client); return r?r.spend/1e6:0; }});
    const catTotal = months.map(m => catTrends.filter(t=>t.month===m).reduce((s,t)=>s+t.spend,0)/1e6);
    makeChart('chTrend', {{type:'line',data:{{labels:months.map(m=>m.substring(0,7)),datasets:[
        {{label:cat+' total',data:catTotal,borderColor:'#94a3b8',borderWidth:1.5,borderDash:[5,3],tension:.3,pointRadius:2}},
        {{label:client,data:clientTrend,borderColor:'#d97706',borderWidth:2.5,backgroundColor:'rgba(217,119,6,.1)',fill:true,tension:.3,pointRadius:3}}
    ]}},options:{{responsive:true,maintainAspectRatio:false,scales:{{y:{{ticks:{{callback:v=>'R'+v+'M'}}}}}}}}}});

    // Time patterns table
    const tp = byCat(D.timepatterns||[]).slice(0,topN);
    if(tp.length) {{
        document.getElementById('timeTable').innerHTML = tableHtml(
            ['Store','Morning','Midday','Afternoon','Evening','Weekend'],
            tp.map(t=>[destName(t.DESTINATION,client,tp.indexOf(t)+1), pct(t.morning), pct(t.midmorning), pct(t.afternoon), pct(t.evening), pct(t.weekend)])
        );
    }}

    // Demographics
    const dm = byCat(D.demo||[]);
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
    const gd = byCat(D.geo||[]).sort((a,b)=>b.spend-a.spend).slice(0,9);
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

// ─── RENDER: Audiences (Marketplace) ───
function renderAudiences() {{
    const typeFilter = document.getElementById('fAudType')?.value || '';
    const minSize = parseInt(document.getElementById('fAudMin')?.value || '0');
    const search = (document.getElementById('fAudSearch')?.value || '').toLowerCase();
    let auds = D.audiences || [];
    if(typeFilter) auds = auds.filter(a=>a.audience_type===typeFilter);
    if(minSize) auds = auds.filter(a=>a.audience_size>=minSize);
    if(search) auds = auds.filter(a=>(a.audience_name||'').toLowerCase().includes(search)||(a.description||'').toLowerCase().includes(search));

    const total = auds.reduce((s,a)=>s+a.audience_size,0);
    document.getElementById('audKpis').innerHTML =
        card('Available audiences', auds.length) +
        card('Total reachable', num(total)) +
        card('Avg audience size', num(Math.round(total/(auds.length||1)))) +
        card('Activation channels', 'Meta · Google · TikTok');

    document.getElementById('audCount').textContent = `Showing ${{auds.length}} audience${{auds.length!==1?'s':''}}`;

    const typeConfig = {{
        'Demographic': {{color:'#1e40af',bg:'#dbeafe',top:'#2563eb',icon:'👤',usePre:'Target by'}},
        'Lifestyle': {{color:'#166534',bg:'#dcfce7',top:'#22c55e',icon:'✨',usePre:'Reach'}},
        'Behavioral': {{color:'#991b1b',bg:'#fee2e2',top:'#ef4444',icon:'📊',usePre:'Re-engage'}},
        'Seasonal': {{color:'#92400e',bg:'#fef3c7',top:'#f59e0b',icon:'📅',usePre:'Activate during'}},
        'Geographic': {{color:'#475569',bg:'#f1f5f9',top:'#64748b',icon:'📍',usePre:'Reach in'}},
        'Cross-category': {{color:'#7b1fa2',bg:'#f3e5f5',top:'#9c27b0',icon:'🔗',usePre:'Cross-sell to'}}
    }};

    const useCases = {{
        'Demographic': ['Awareness campaigns','Broad reach targeting','Demographic layering','Persona-based campaigns'],
        'Lifestyle': ['Interest-based targeting','Content marketing','Brand alignment','Affinity campaigns'],
        'Behavioral': ['Retargeting at-risk','Win-back campaigns','Upsell & cross-sell','Loyalty programs'],
        'Seasonal': ['Event-driven campaigns','Flash sales','Holiday promotions','Calendar targeting'],
        'Geographic': ['Local store campaigns','Regional launches','Geo-targeted promos','Expansion targeting'],
        'Cross-category': ['Bundle promotions','Cross-brand campaigns','Lifestyle packages','Multi-category upsell']
    }};

    const fmtSize = v => {{ if(v>=1e6) return (v/1e6).toFixed(1)+'M'; if(v>=1e3) return Math.round(v/1e3)+'K'; return v; }};

    let html = '';
    auds.forEach((a,i) => {{
        const tc = typeConfig[a.audience_type] || typeConfig['Geographic'];
        const uses = useCases[a.audience_type] || [];
        const useCase = uses[i % uses.length] || '';

        html += `
        <div class="aud-card" onclick="openModal('${{a.audience_id}}')">
            <div class="aud-top" style="background:${{tc.top}}"></div>
            <div class="aud-body">
                <div class="aud-type" style="color:${{tc.color}}">${{tc.icon}} ${{a.audience_type}}</div>
                <div class="aud-name">${{a.audience_name}}</div>
                <div class="aud-desc">${{a.description || 'Behaviorally defined audience segment.'}}</div>
                <div class="aud-stats">
                    <div class="aud-stat"><div class="n">${{fmtSize(a.audience_size)}}</div><div class="l">Reach</div></div>
                    <div class="aud-stat"><div class="n">${{a.avg_spend ? fmt(a.avg_spend) : 'N/A'}}</div><div class="l">Avg Spend</div></div>
                    ${{a.avg_age ? `<div class="aud-stat"><div class="n">${{Math.round(a.avg_age)}}</div><div class="l">Avg Age</div></div>` : ''}}
                    ${{a.pct_female ? `<div class="aud-stat"><div class="n">${{Math.round(a.pct_female)}}%</div><div class="l">Female</div></div>` : ''}}
                </div>
                <div class="aud-channels">
                    <span class="aud-ch meta">Meta</span>
                    <span class="aud-ch goog">Google</span>
                    <span class="aud-ch tik">TikTok</span>
                </div>
                <div class="aud-tags">
                    <span class="aud-tag" style="background:${{tc.bg}};color:${{tc.color}}">${{a.audience_type}}</span>
                    ${{a.top_province ? `<span class="aud-tag" style="background:#f1f5f9;color:#475569">${{a.top_province}}</span>` : ''}}
                    ${{a.top_segment ? `<span class="aud-tag" style="background:#f1f5f9;color:#475569">${{a.top_segment}}</span>` : ''}}
                </div>
                <div class="aud-demog">
                    <div class="di"><div class="dv">${{a.top_age_group||'—'}}</div><div class="dl">Top Age</div></div>
                    <div class="di"><div class="dv">${{a.avg_income ? fmt(a.avg_income) : '—'}}</div><div class="dl">Avg Income</div></div>
                    <div class="di"><div class="dv">${{a.top_province||'—'}}</div><div class="dl">Top Province</div></div>
                </div>
            </div>
            <div class="aud-use"><strong>Best for:</strong> ${{useCase}}</div>
        </div>`;
    }});

    document.getElementById('audGrid').innerHTML = html || '<div class="empty">No audiences match your filters</div>';

    // Per-client overlap section — answers "which of these audiences are MY client's customers in?"
    const oCat = document.getElementById('fCat') ? document.getElementById('fCat').value : '';
    const oCli = document.getElementById('fClient') ? document.getElementById('fClient').value : '';
    const overlap = (D.aud_client_overlap || []).filter(r => r.DESTINATION === oCli && r.CATEGORY_TWO === oCat);
    const scopeEl = document.getElementById('audOverlapScope');
    const tableEl = document.getElementById('audOverlapTable');
    if (tableEl) {{
        if (overlap.length === 0) {{
            if (scopeEl) scopeEl.textContent = oCli && oCat ? `— no overlap data for ${{oCli}} in ${{oCat}} (below 1,000-customer threshold)` : `— select a client and category above`;
            tableEl.innerHTML = '<div class="empty" style="padding:20px;color:#94a3b8;font-size:.85rem">Pick a client and category in the header filters to see which audiences their customers over-index on.</div>';
        }} else {{
            if (scopeEl) scopeEl.textContent = `— ${{oCli}} in ${{oCat}}`;
            const top = overlap.slice().sort((a,b)=>b.pct_of_client-a.pct_of_client).slice(0, 10);
            const rows = top.map(r => [
                `<strong>${{r.audience_name}}</strong>`,
                r.audience_type,
                num(r.overlap_customers),
                r.pct_of_client.toFixed(1) + '%',
            ]);
            tableEl.innerHTML = tableHtml(['Audience','Type','Customers in overlap','% of this client base'], rows);
        }}
    }}
}}

// ─── MODAL: Audience detail ───
function openModal(audId) {{
    const a = (D.audiences||[]).find(x=>x.audience_id===audId);
    if(!a) return;

    const typeConfig = {{
        'Demographic': {{color:'#1e40af',bg:'#dbeafe'}},
        'Lifestyle': {{color:'#166534',bg:'#dcfce7'}},
        'Behavioral': {{color:'#991b1b',bg:'#fee2e2'}},
        'Seasonal': {{color:'#92400e',bg:'#fef3c7'}},
        'Geographic': {{color:'#475569',bg:'#f1f5f9'}},
        'Cross-category': {{color:'#7b1fa2',bg:'#f3e5f5'}}
    }};
    const tc = typeConfig[a.audience_type] || typeConfig['Geographic'];
    const fmtSize = v => {{ if(v>=1e6) return (v/1e6).toFixed(1)+'M+'; if(v>=1e3) return Math.round(v/1e3)+'K+'; return v; }};

    document.getElementById('modalType').textContent = a.audience_type;
    document.getElementById('modalType').style.color = tc.color;
    document.getElementById('modalName').textContent = a.audience_name;
    document.getElementById('modalDesc').textContent = a.description || '';

    document.getElementById('modalStats').innerHTML =
        `<div class="modal-stat"><div class="n">${{fmtSize(a.audience_size)}}</div><div class="l">Audience size</div></div>` +
        `<div class="modal-stat"><div class="n">${{a.avg_spend ? fmt(a.avg_spend) : 'N/A'}}</div><div class="l">Avg spend</div></div>` +
        `<div class="modal-stat"><div class="n">${{a.avg_age ? Math.round(a.avg_age) : '—'}}</div><div class="l">Avg age</div></div>` +
        `<div class="modal-stat"><div class="n">${{a.pct_female ? Math.round(a.pct_female)+'%' : '—'}}</div><div class="l">Female</div></div>` +
        `<div class="modal-stat"><div class="n">${{a.avg_income ? fmt(a.avg_income) : '—'}}</div><div class="l">Avg income</div></div>`;

    // Top categories
    const cats = (D.aud_categories||[]).filter(c=>c.audience_id===audId).sort((a,b)=>b.spend-a.spend).slice(0,8);
    const catMax = cats.length ? cats[0].spend : 1;
    document.getElementById('modalCats').innerHTML = cats.length ?
        cats.map(c => barRow(c.CATEGORY_TWO, c.spend, catMax, fmt(c.spend), tc.color)).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No category data</div>';

    // Top merchants
    const merch = (D.aud_merchants||[]).filter(m=>m.audience_id===audId).sort((a,b)=>b.spend-a.spend).slice(0,8);
    const mMax = merch.length ? merch[0].spend : 1;
    document.getElementById('modalMerch').innerHTML = merch.length ?
        merch.map(m => barRow(m.DESTINATION, m.spend, mMax, fmt(m.spend), '#1e3a5f')).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No merchant data</div>';

    // Age
    const ages = (D.aud_age||[]).filter(x=>x.audience_id===audId);
    const ageMax = ages.length ? Math.max(...ages.map(x=>x.customers)) : 1;
    document.getElementById('modalAge').innerHTML = ages.length ?
        ages.map(x => barRow(x.age_group, x.customers, ageMax, num(x.customers), '#2E75B6')).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No age data</div>';

    // Income
    const inc = (D.aud_income||[]).filter(x=>x.audience_id===audId);
    const incMax = inc.length ? Math.max(...inc.map(x=>x.customers)) : 1;
    document.getElementById('modalIncome').innerHTML = inc.length ?
        inc.map(x => barRow(x.income_group, x.customers, incMax, num(x.customers), '#4CAF50')).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No income data</div>';

    // Gender
    const gen = (D.aud_gender||[]).filter(x=>x.audience_id===audId);
    const genMax = gen.length ? Math.max(...gen.map(x=>x.customers)) : 1;
    document.getElementById('modalGender').innerHTML = gen.length ?
        gen.map(x => barRow(x.gender_label, x.customers, genMax, num(x.customers), x.gender_label==='Female'?'#E91E63':'#0f172a')).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No gender data</div>';

    // Province
    const prov = (D.aud_province||[]).filter(x=>x.audience_id===audId).sort((a,b)=>b.spend-a.spend).slice(0,6);
    const provMax = prov.length ? prov[0].spend : 1;
    document.getElementById('modalProv').innerHTML = prov.length ?
        prov.map(x => barRow(x.PROVINCE, x.spend, provMax, fmt(x.spend), '#64748b')).join('') :
        '<div style="color:#94a3b8;font-size:.82rem">No province data</div>';

    document.getElementById('audModal').classList.add('show');
    document.body.style.overflow = 'hidden';
}}

function closeModal() {{
    document.getElementById('audModal').classList.remove('show');
    document.body.style.overflow = '';
}}

function barRow(label, value, max, display, color) {{
    const pct = Math.round(value / max * 100);
    return `<div class="modal-bar">
        <div class="lbl">${{label}}</div>
        <div class="track"><div class="fill" style="width:${{pct}}%;background:${{color}}"></div></div>
        <div class="val">${{display}}</div>
    </div>`;
}}

// Close on Escape key
document.addEventListener('keydown', e => {{ if(e.key==='Escape') closeModal(); }});

// ─── INIT ───
const LOW_B64 = '{low_b64}';

function init() {{
    // Hide filters on Overview (they only show on Client Pitch)
    document.getElementById('filterbar').style.display = 'none';

    // Add bottom banner to every page
    if(LOW_B64) {{
        document.querySelectorAll('.pg').forEach(pg => {{
            const banner = document.createElement('div');
            banner.className = 'pg-banner';
            banner.innerHTML = `<img src="data:image/png;base64,${{LOW_B64}}" alt="">`;
            pg.appendChild(banner);
        }});
    }}

    // Populate client dropdown for first category
    onFilter();
    // Render all static pages
    renderOverview();
    renderSegments();
    renderChurn();
    renderCategories();
    renderAudiences();
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
