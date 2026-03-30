#!/usr/bin/env python3
"""
discover_clicks.py — Run this and screenshot the output for me.
"""
import os
from google.cloud import bigquery

PROJECT = os.environ.get('BQ_PROJECT', 'fmn-sandbox')
bq = bigquery.Client(project=PROJECT, location='africa-south1')

def run(label, sql):
    print(f'\n{"="*60}')
    print(f'  {label}')
    print(f'{"="*60}')
    try:
        df = bq.query(sql).to_dataframe()
        print(df.to_string(index=False))
    except Exception as e:
        print(f'  ERROR: {e}')

run("1. What is Clicks called in the data?", f"""
    SELECT DESTINATION, CATEGORY_TWO, customers, ROUND(total_spend,0) AS spend, spend_rank
    FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(DESTINATION) LIKE '%CLICK%'
    ORDER BY spend DESC LIMIT 10
""")

run("2. What health/beauty/pharmacy categories exist?", f"""
    SELECT DISTINCT CATEGORY_TWO FROM `{PROJECT}.marts.mart_destination_benchmarks`
    WHERE UPPER(CATEGORY_TWO) LIKE '%HEALTH%' OR UPPER(CATEGORY_TWO) LIKE '%PHARM%'
        OR UPPER(CATEGORY_TWO) LIKE '%BEAUTY%' OR UPPER(CATEGORY_TWO) LIKE '%CARE%'
        OR UPPER(CATEGORY_TWO) LIKE '%COSM%' OR UPPER(CATEGORY_TWO) LIKE '%PERSON%'
    ORDER BY 1
""")

run("3. What demographic columns exist in stg_customers?", f"""
    SELECT column_name FROM `{PROJECT}.staging.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'stg_customers' ORDER BY ordinal_position
""")

run("4. What gender values exist?", f"""
    SELECT gender_label, COUNT(*) AS n FROM `{PROJECT}.staging.stg_customers`
    GROUP BY 1 ORDER BY 2 DESC
""")

run("5. What age_group values exist?", f"""
    SELECT age_group, COUNT(*) AS n FROM `{PROJECT}.staging.stg_customers`
    GROUP BY 1 ORDER BY 1
""")

run("6. What income_group values exist?", f"""
    SELECT income_group, COUNT(*) AS n FROM `{PROJECT}.staging.stg_customers`
    GROUP BY 1 ORDER BY 1
""")

run("7. Income range (min/max/avg)?", f"""
    SELECT MIN(estimated_income) AS min_inc, MAX(estimated_income) AS max_inc,
        ROUND(AVG(estimated_income),0) AS avg_inc
    FROM `{PROJECT}.staging.stg_customers`
    WHERE estimated_income > 0
""")

run("8. How many people shop at Clicks?", f"""
    SELECT COUNT(DISTINCT UNIQUE_ID) AS clicks_shoppers
    FROM `{PROJECT}.analytics.int_customer_category_spend`
    WHERE UPPER(DESTINATION) LIKE '%CLICK%'
""")

run("9. Clicks shoppers by gender?", f"""
    SELECT c.gender_label, COUNT(DISTINCT cs.UNIQUE_ID) AS n
    FROM `{PROJECT}.analytics.int_customer_category_spend` cs
    JOIN `{PROJECT}.staging.stg_customers` c ON cs.UNIQUE_ID = c.UNIQUE_ID
    WHERE UPPER(cs.DESTINATION) LIKE '%CLICK%'
    GROUP BY 1 ORDER BY 2 DESC
""")

run("10. Clicks shoppers by age group?", f"""
    SELECT c.age_group, COUNT(DISTINCT cs.UNIQUE_ID) AS n
    FROM `{PROJECT}.analytics.int_customer_category_spend` cs
    JOIN `{PROJECT}.staging.stg_customers` c ON cs.UNIQUE_ID = c.UNIQUE_ID
    WHERE UPPER(cs.DESTINATION) LIKE '%CLICK%'
    GROUP BY 1 ORDER BY 1
""")

print('\n\nDONE — screenshot this and send to Claude')
