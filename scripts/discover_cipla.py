#!/usr/bin/env python3
"""
discover_cipla.py — Run this and screenshot the output.
Discovers pharmacy landscape, seasonal patterns, and rugby-related spend.
"""
import os
from google.cloud import bigquery

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
bq = bigquery.Client(project=PROJECT, location='africa-south1')

def run(label, sql):
    print(f'\n{"="*70}')
    print(f'  {label}')
    print(f'{"="*70}')
    try:
        df = bq.query(sql).to_dataframe()
        print(df.to_string(index=False))
    except Exception as e:
        print(f'  ERROR: {e}')

# ═══════════════════════════════════════════════════════════════
# PHARMACY LANDSCAPE
# ═══════════════════════════════════════════════════════════════

run("1. All pharmacy/health categories", f"""
    SELECT DISTINCT CATEGORY_TWO
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(CATEGORY_TWO) LIKE '%PHARM%'
        OR UPPER(CATEGORY_TWO) LIKE '%HEALTH%'
        OR UPPER(CATEGORY_TWO) LIKE '%BEAUTY%'
        OR UPPER(CATEGORY_TWO) LIKE '%CARE%'
        OR UPPER(CATEGORY_TWO) LIKE '%MEDICAL%'
        OR UPPER(CATEGORY_TWO) LIKE '%HOSPITAL%'
        OR UPPER(CATEGORY_TWO) LIKE '%DOCTOR%'
        OR UPPER(CATEGORY_TWO) LIKE '%WELLNESS%'
    ORDER BY 1
""")

run("2. Top 15 pharmacy/health destinations by spend", f"""
    SELECT DESTINATION, CATEGORY_TWO, customers,
        ROUND(total_spend/1e9, 2) AS spend_bn,
        ROUND(market_share_pct, 1) AS share,
        spend_rank
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(CATEGORY_TWO) LIKE '%PHARM%'
        OR UPPER(CATEGORY_TWO) LIKE '%HEALTH%' AND UPPER(CATEGORY_TWO) NOT LIKE '%PET%' AND UPPER(CATEGORY_TWO) NOT LIKE '%INSUR%'
    ORDER BY total_spend DESC
    LIMIT 15
""")

run("3. Clicks vs Dis-Chem vs others in Pharmacies", f"""
    SELECT DESTINATION, customers,
        ROUND(total_spend/1e9, 2) AS spend_bn,
        ROUND(market_share_pct, 1) AS share,
        ROUND(spend_per_customer, 0) AS per_cust,
        ROUND(avg_share_of_wallet, 1) AS sow,
        spend_rank
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE CATEGORY_TWO = 'Pharmacies and Wellbeing'
    ORDER BY spend_rank
    LIMIT 10
""")

# ═══════════════════════════════════════════════════════════════
# SEASONAL PATTERNS
# ═══════════════════════════════════════════════════════════════

run("4. Monthly pharmacy spend (seasonality)", f"""
    SELECT FORMAT_DATE('%Y-%m', EFF_DATE) AS month,
        COUNT(DISTINCT UNIQUE_ID) AS customers,
        ROUND(SUM(trns_amt)/1e9, 2) AS spend_bn,
        COUNT(*) AS txns
    FROM `{PROJECT}.staging.stg_transactions`
    WHERE CATEGORY_TWO = 'Pharmacies and Wellbeing'
        AND EFF_DATE >= DATE_SUB(
            (SELECT MAX(EFF_DATE) FROM `{PROJECT}.staging.stg_transactions`),
            INTERVAL 12 MONTH)
    GROUP BY 1
    ORDER BY 1
""")

# ═══════════════════════════════════════════════════════════════
# PHARMACY SHOPPER DEMOGRAPHICS
# ═══════════════════════════════════════════════════════════════

run("5. Pharmacy shoppers by age group", f"""
    SELECT c.age_group, COUNT(DISTINCT cs.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend)/1e9, 2) AS spend_bn
    FROM `{PROJECT}.analytics.int_customer_category_spend` cs
    JOIN `{PROJECT}.staging.stg_customers` c ON cs.UNIQUE_ID = c.UNIQUE_ID
    WHERE cs.CATEGORY_TWO = 'Pharmacies and Wellbeing'
        AND c.age_group IS NOT NULL AND c.age_group != 'Unknown'
    GROUP BY 1 ORDER BY 1
""")

run("6. Pharmacy shoppers by income group", f"""
    SELECT c.income_group, COUNT(DISTINCT cs.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend)/1e9, 2) AS spend_bn
    FROM `{PROJECT}.analytics.int_customer_category_spend` cs
    JOIN `{PROJECT}.staging.stg_customers` c ON cs.UNIQUE_ID = c.UNIQUE_ID
    WHERE cs.CATEGORY_TWO = 'Pharmacies and Wellbeing'
        AND c.income_group IS NOT NULL AND c.income_group != 'Unknown'
    GROUP BY 1 ORDER BY 1
""")

run("7. Pharmacy shoppers by gender", f"""
    SELECT c.gender_label, COUNT(DISTINCT cs.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend)/1e9, 2) AS spend_bn
    FROM `{PROJECT}.analytics.int_customer_category_spend` cs
    JOIN `{PROJECT}.staging.stg_customers` c ON cs.UNIQUE_ID = c.UNIQUE_ID
    WHERE cs.CATEGORY_TWO = 'Pharmacies and Wellbeing'
        AND c.gender_label IS NOT NULL
    GROUP BY 1 ORDER BY 2 DESC
""")

# ═══════════════════════════════════════════════════════════════
# CROSS-CATEGORY SHOPPING
# ═══════════════════════════════════════════════════════════════

run("8. What else do pharmacy shoppers buy? (top categories)", f"""
    WITH pharma_shoppers AS (
        SELECT DISTINCT UNIQUE_ID
        FROM `{PROJECT}.analytics.int_customer_category_spend`
        WHERE CATEGORY_TWO = 'Pharmacies and Wellbeing'
    )
    SELECT cs.CATEGORY_TWO, COUNT(DISTINCT cs.UNIQUE_ID) AS customers,
        ROUND(SUM(cs.dest_spend)/1e9, 2) AS spend_bn
    FROM pharma_shoppers p
    JOIN `{PROJECT}.analytics.int_customer_category_spend` cs ON p.UNIQUE_ID = cs.UNIQUE_ID
    WHERE cs.CATEGORY_TWO != 'Pharmacies and Wellbeing'
    GROUP BY 1 ORDER BY spend_bn DESC
    LIMIT 12
""")

# ═══════════════════════════════════════════════════════════════
# RUGBY / SPORTS SPEND
# ═══════════════════════════════════════════════════════════════

run("9. All categories — looking for sports/rugby/betting/events", f"""
    SELECT DISTINCT CATEGORY_TWO
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(CATEGORY_TWO) LIKE '%SPORT%'
        OR UPPER(CATEGORY_TWO) LIKE '%BET%'
        OR UPPER(CATEGORY_TWO) LIKE '%EVENT%'
        OR UPPER(CATEGORY_TWO) LIKE '%TICKET%'
        OR UPPER(CATEGORY_TWO) LIKE '%ENTERTAIN%'
        OR UPPER(CATEGORY_TWO) LIKE '%RECREATION%'
        OR UPPER(CATEGORY_TWO) LIKE '%GAME%'
        OR UPPER(CATEGORY_TWO) LIKE '%LEIS%'
        OR UPPER(CATEGORY_TWO) LIKE '%PUB%'
        OR UPPER(CATEGORY_TWO) LIKE '%BAR%'
        OR UPPER(CATEGORY_TWO) LIKE '%RESTAURANT%'
    ORDER BY 1
""")

run("10. Top destinations in sports/betting/entertainment", f"""
    SELECT DESTINATION, CATEGORY_TWO, customers,
        ROUND(total_spend/1e9, 2) AS spend_bn
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(CATEGORY_TWO) LIKE '%SPORT%'
        OR UPPER(CATEGORY_TWO) LIKE '%BET%'
        OR UPPER(CATEGORY_TWO) LIKE '%EVENT%'
        OR UPPER(CATEGORY_TWO) LIKE '%ENTERTAIN%'
        OR UPPER(CATEGORY_TWO) LIKE '%RECREATION%'
    ORDER BY total_spend DESC
    LIMIT 15
""")

# ═══════════════════════════════════════════════════════════════
# AUDIENCE SIZING
# ═══════════════════════════════════════════════════════════════

run("11. Pharmacy shoppers by segment", f"""
    WITH pharma AS (
        SELECT DISTINCT cs.UNIQUE_ID
        FROM `{PROJECT}.analytics.int_customer_category_spend` cs
        WHERE cs.CATEGORY_TWO = 'Pharmacies and Wellbeing'
    )
    SELECT co.segment_name, COUNT(*) AS n
    FROM pharma p
    JOIN `{PROJECT}.marts.mart_cluster_output` co ON p.UNIQUE_ID = co.UNIQUE_ID
    GROUP BY 1 ORDER BY n DESC
""")

run("12. Total unique pharmacy shoppers (any pharmacy store)", f"""
    SELECT COUNT(DISTINCT UNIQUE_ID) AS total_pharmacy_shoppers,
        ROUND(SUM(dest_spend)/1e9, 2) AS total_spend_bn,
        ROUND(AVG(dest_spend), 0) AS avg_spend
    FROM `{PROJECT}.analytics.int_customer_category_spend`
    WHERE CATEGORY_TWO = 'Pharmacies and Wellbeing'
""")

run("13. Overlap: people who shop at BOTH Clicks AND Dis-Chem", f"""
    SELECT COUNT(DISTINCT a.UNIQUE_ID) AS overlap
    FROM (SELECT DISTINCT UNIQUE_ID FROM `{PROJECT}.analytics.int_customer_category_spend` WHERE DESTINATION = 'CLICKS') a
    JOIN (SELECT DISTINCT UNIQUE_ID FROM `{PROJECT}.analytics.int_customer_category_spend` WHERE UPPER(DESTINATION) LIKE '%DIS%CHEM%' OR UPPER(DESTINATION) LIKE '%DISCHEM%') b
    ON a.UNIQUE_ID = b.UNIQUE_ID
""")

print('\n\n' + '='*70)
print('  DONE — screenshot all output and send to Claude')
print('='*70)
