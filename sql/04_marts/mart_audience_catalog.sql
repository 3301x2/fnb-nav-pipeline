-- mart_audience_catalog.sql + mart_audience_members.sql
-- Pre-packaged audience segments for advertisers.
-- Reads from: existing mart tables + stg_customers + int_customer_category_spend + stg_transactions (partitioned)
-- The only expensive part is seasonal audiences which scan a few months of stg_transactions.

-- ═══════════════════════════════════════════════════════════════
-- STEP 1: Build audience members (customer × audience mapping)
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_audience_members` AS

-- ─── DEMOGRAPHIC AUDIENCES ───────────────────────────────────

SELECT 'D01' AS audience_id, 'Affluent professionals' AS audience_name, 'Demographic' AS audience_type, UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE estimated_income >= 56000 AND age BETWEEN 25 AND 55

UNION ALL
SELECT 'D02', 'Emerging middle class', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE estimated_income BETWEEN 23000 AND 56000

UNION ALL
SELECT 'D03', 'Young adults 18-25', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE age BETWEEN 18 AND 25

UNION ALL
SELECT 'D04', 'Senior spenders 55+', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE age >= 55

UNION ALL
SELECT 'D05', 'High income female', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE estimated_income >= 56000 AND gender_label = 'Female'

UNION ALL
SELECT 'D06', 'High income male', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE estimated_income >= 56000 AND gender_label = 'Male'

UNION ALL
SELECT 'D07', 'Young families 26-40', 'Demographic', UNIQUE_ID
FROM `__PROJECT__.staging.stg_customers`
WHERE age BETWEEN 26 AND 40 AND estimated_income >= 23000

UNION ALL

-- ─── LIFESTYLE / AFFINITY AUDIENCES ──────────────────────────
-- Based on category spend concentration

SELECT 'L01', 'Food lovers', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Groceries', 'Restaurants', 'Food & Beverage'), dest_spend, 0)) AS food_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING food_spend / NULLIF(total_spend, 0) >= 0.4 AND total_spend >= 5000
)

UNION ALL
SELECT 'L02', 'Fashion forward', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Clothing & Apparel', 'Footwear', 'Accessories'), dest_spend, 0)) AS fashion_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING fashion_spend / NULLIF(total_spend, 0) >= 0.25 AND fashion_spend >= 3000
)

UNION ALL
SELECT 'L03', 'Auto enthusiasts', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Fuel & Energy', 'Automotive', 'Auto Accessories'), dest_spend, 0)) AS auto_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING auto_spend / NULLIF(total_spend, 0) >= 0.3 AND auto_spend >= 5000
)

UNION ALL
SELECT 'L04', 'Health & wellness', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Health', 'Pharmacy', 'Fitness', 'Wellness'), dest_spend, 0)) AS health_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING health_spend / NULLIF(total_spend, 0) >= 0.15 AND health_spend >= 2000
)

UNION ALL
SELECT 'L05', 'Home improvers', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Hardware', 'Furniture', 'Home & Garden', 'Home Decor'), dest_spend, 0)) AS home_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING home_spend / NULLIF(total_spend, 0) >= 0.2 AND home_spend >= 3000
)

UNION ALL
SELECT 'L06', 'Tech savvy', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Electronics', 'Telecoms', 'Technology'), dest_spend, 0)) AS tech_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING tech_spend / NULLIF(total_spend, 0) >= 0.2 AND tech_spend >= 2000
)

UNION ALL
SELECT 'L07', 'Travel & leisure', 'Lifestyle', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(CATEGORY_TWO IN ('Travel', 'Accommodation', 'Airlines', 'Entertainment'), dest_spend, 0)) AS travel_spend,
        SUM(dest_spend) AS total_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID
    HAVING travel_spend / NULLIF(total_spend, 0) >= 0.15 AND travel_spend >= 3000
)

UNION ALL

-- ─── BEHAVIORAL AUDIENCES ────────────────────────────────────

SELECT 'B01', 'High value champions', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.marts.mart_cluster_output`
WHERE segment_name = 'Champions'

UNION ALL
SELECT 'B02', 'Loyal high value', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.marts.mart_cluster_output`
WHERE segment_name = 'Loyal High Value'

UNION ALL
SELECT 'B03', 'At-risk high spenders', 'Behavioral', a.UNIQUE_ID
FROM `__PROJECT__.marts.mart_churn_risk` a
JOIN `__PROJECT__.analytics.int_rfm_features` b ON a.UNIQUE_ID = b.UNIQUE_ID
WHERE a.churn_risk_level IN ('Critical', 'High')
    AND b.val_trns >= 50000

UNION ALL
SELECT 'B04', 'Accelerating spenders', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.marts.mart_spend_momentum`
WHERE momentum_status = 'Accelerating'
    AND total_spend_12m >= 10000

UNION ALL
SELECT 'B05', 'Declining high value', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.marts.mart_spend_momentum`
WHERE momentum_status = 'Declining'
    AND total_spend_12m >= 50000

UNION ALL
SELECT 'B06', 'Frequent transactors', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.analytics.int_rfm_features`
WHERE nr_trns >= 500

UNION ALL
SELECT 'B07', 'Multi-category shoppers', 'Behavioral', UNIQUE_ID
FROM `__PROJECT__.analytics.int_rfm_features`
WHERE active_nav_categories >= 8

UNION ALL

-- ─── GEOGRAPHIC AUDIENCES ────────────────────────────────────
-- Province comes from stg_transactions, not stg_customers.
-- Single scan, then split by province.

SELECT
    CASE main_province
        WHEN 'GAUTENG' THEN 'G01'
        WHEN 'WESTERN CAPE' THEN 'G02'
        WHEN 'KWAZULU-NATAL' THEN 'G03'
        WHEN 'EASTERN CAPE' THEN 'G04'
    END AS audience_id,
    CASE main_province
        WHEN 'GAUTENG' THEN 'Gauteng metro'
        WHEN 'WESTERN CAPE' THEN 'Western Cape'
        WHEN 'KWAZULU-NATAL' THEN 'KwaZulu-Natal'
        WHEN 'EASTERN CAPE' THEN 'Eastern Cape'
    END AS audience_name,
    'Geographic' AS audience_type,
    UNIQUE_ID
FROM (
    SELECT UNIQUE_ID, APPROX_TOP_COUNT(PROVINCE, 1)[OFFSET(0)].value AS main_province
    FROM `__PROJECT__.staging.stg_transactions`
    WHERE PROVINCE IS NOT NULL
        AND EFF_DATE >= DATE_SUB(
            (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
            INTERVAL 12 MONTH)
    GROUP BY UNIQUE_ID
)
WHERE main_province IN ('GAUTENG', 'WESTERN CAPE', 'KWAZULU-NATAL', 'EASTERN CAPE')

UNION ALL

-- ─── SEASONAL AUDIENCES ──────────────────────────────────────
-- These touch stg_transactions but use month partition

SELECT 'S01', 'Black Friday power shoppers', 'Seasonal', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(EXTRACT(MONTH FROM EFF_DATE) = 11, trns_amt, 0)) AS nov_spend,
        AVG(trns_amt) AS avg_monthly
    FROM `__PROJECT__.staging.stg_transactions`
    WHERE EFF_DATE >= DATE_SUB(
        (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
        INTERVAL 14 MONTH
    )
    GROUP BY UNIQUE_ID
    HAVING nov_spend >= avg_monthly * 2 AND nov_spend >= 1000
)

UNION ALL
SELECT 'S02', 'Festive season splurgers', 'Seasonal', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(EXTRACT(MONTH FROM EFF_DATE) = 12, trns_amt, 0)) AS dec_spend,
        AVG(trns_amt) AS avg_monthly
    FROM `__PROJECT__.staging.stg_transactions`
    WHERE EFF_DATE >= DATE_SUB(
        (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
        INTERVAL 14 MONTH
    )
    GROUP BY UNIQUE_ID
    HAVING dec_spend >= avg_monthly * 1.5 AND dec_spend >= 1000
)

UNION ALL
SELECT 'S03', 'Back to school parents', 'Seasonal', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        SUM(IF(EXTRACT(MONTH FROM EFF_DATE) = 1, trns_amt, 0)) AS jan_spend,
        AVG(trns_amt) AS avg_monthly
    FROM `__PROJECT__.staging.stg_transactions`
    WHERE EFF_DATE >= DATE_SUB(
        (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
        INTERVAL 14 MONTH
    )
    GROUP BY UNIQUE_ID
    HAVING jan_spend >= avg_monthly * 1.5 AND jan_spend >= 500
)

UNION ALL
SELECT 'S04', 'Pay day shoppers', 'Seasonal', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID,
        COUNTIF(EXTRACT(DAY FROM EFF_DATE) BETWEEN 25 AND 31
             OR EXTRACT(DAY FROM EFF_DATE) BETWEEN 1 AND 3) AS payday_txns,
        COUNT(*) AS total_txns
    FROM `__PROJECT__.staging.stg_transactions`
    WHERE EFF_DATE >= DATE_SUB(
        (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
        INTERVAL 12 MONTH
    )
    GROUP BY UNIQUE_ID
    HAVING payday_txns * 1.0 / NULLIF(total_txns, 0) >= 0.5 AND total_txns >= 12
)

UNION ALL

-- ─── CROSS-CATEGORY AUDIENCES ────────────────────────────────

SELECT 'X01', 'Premium lifestyle bundle', 'Cross-category', UNIQUE_ID
FROM (
    SELECT UNIQUE_ID, COUNT(DISTINCT CATEGORY_TWO) AS premium_cats
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    WHERE CATEGORY_TWO IN ('Clothing & Apparel', 'Travel', 'Restaurants', 'Health', 'Electronics')
        AND dest_spend >= 2000
    GROUP BY UNIQUE_ID
    HAVING premium_cats >= 3
)

UNION ALL
SELECT 'X02', 'Grocery + clothing crossover', 'Cross-category', UNIQUE_ID
FROM (
    SELECT a.UNIQUE_ID
    FROM `__PROJECT__.analytics.int_customer_category_spend` a
    JOIN `__PROJECT__.analytics.int_customer_category_spend` b
        ON a.UNIQUE_ID = b.UNIQUE_ID
    WHERE a.CATEGORY_TWO = 'Groceries' AND a.dest_spend >= 5000
        AND b.CATEGORY_TWO = 'Clothing & Apparel' AND b.dest_spend >= 2000
    GROUP BY a.UNIQUE_ID
);


-- ═══════════════════════════════════════════════════════════════
-- STEP 2: Build audience catalog (the shelf)
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_audience_catalog` AS

SELECT
    am.audience_id,
    am.audience_name,
    am.audience_type,
    COUNT(DISTINCT am.UNIQUE_ID) AS audience_size,
    ROUND(AVG(r.val_trns), 0) AS avg_spend,
    ROUND(AVG(c.age), 1) AS avg_age,
    ROUND(COUNTIF(c.gender_label = 'Female') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_female,
    ROUND(AVG(c.estimated_income), 0) AS avg_income,
    CASE
        WHEN am.audience_id = 'G01' THEN 'GAUTENG'
        WHEN am.audience_id = 'G02' THEN 'WESTERN CAPE'
        WHEN am.audience_id = 'G03' THEN 'KWAZULU-NATAL'
        WHEN am.audience_id = 'G04' THEN 'EASTERN CAPE'
        ELSE 'Mixed'
    END AS top_province,
    APPROX_TOP_COUNT(c.age_group, 1)[OFFSET(0)].value AS top_age_group,
    APPROX_TOP_COUNT(c.income_group, 1)[OFFSET(0)].value AS top_income_group,
    APPROX_TOP_COUNT(co.segment_name, 1)[OFFSET(0)].value AS top_segment,
    ROUND(AVG(ch.churn_probability), 3) AS avg_churn_prob,

    -- Description (auto-generated)
    CASE am.audience_id
        WHEN 'D01' THEN 'High-income professionals aged 25-55. Strong purchasing power across categories.'
        WHEN 'D02' THEN 'Middle-income consumers (R23k-R56k). Growing spending power, price-conscious but aspirational.'
        WHEN 'D03' THEN 'Young adults 18-25. Digital-first, trend-driven, lower basket but high frequency potential.'
        WHEN 'D04' THEN 'Customers aged 55+. Higher average basket, brand loyal, health and wellness oriented.'
        WHEN 'D05' THEN 'High-income female consumers. Over-index in fashion, groceries, health.'
        WHEN 'D06' THEN 'High-income male consumers. Over-index in auto, electronics, fuel.'
        WHEN 'D07' THEN 'Young families aged 26-40 with middle+ income. Groceries, kids, household heavy.'
        WHEN 'L01' THEN 'Customers who spend 40%+ on food categories (groceries, restaurants, food delivery).'
        WHEN 'L02' THEN 'Customers who spend 25%+ on fashion (clothing, footwear, accessories).'
        WHEN 'L03' THEN 'Customers who spend 30%+ on automotive (fuel, parts, accessories).'
        WHEN 'L04' THEN 'Customers who spend 15%+ on health (pharmacy, fitness, wellness).'
        WHEN 'L05' THEN 'Customers who spend 20%+ on home improvement (hardware, furniture, decor).'
        WHEN 'L06' THEN 'Customers who spend 20%+ on technology (electronics, telecoms).'
        WHEN 'L07' THEN 'Customers who spend 15%+ on travel and leisure (flights, hotels, entertainment).'
        WHEN 'B01' THEN 'Top 8% of customers by spend and engagement. Highest value, most active.'
        WHEN 'B02' THEN 'Second tier — consistent high spenders with strong merchant diversity.'
        WHEN 'B03' THEN 'High historical spend but flagged as churn risk. Re-engagement priority.'
        WHEN 'B04' THEN 'Customers whose recent 6m spend exceeds prior 6m. Growing value.'
        WHEN 'B05' THEN 'High spenders whose spend is trending down. Retention intervention needed.'
        WHEN 'B06' THEN 'Customers with 500+ transactions in 12 months. Ultra-frequent buyers.'
        WHEN 'B07' THEN 'Customers active in 8+ categories. Broad lifestyle, high cross-sell potential.'
        WHEN 'G01' THEN 'All customers located in Gauteng province.'
        WHEN 'G02' THEN 'All customers located in Western Cape province.'
        WHEN 'G03' THEN 'All customers located in KwaZulu-Natal province.'
        WHEN 'G04' THEN 'All customers located in Eastern Cape province.'
        WHEN 'S01' THEN 'Customers who spent 2x+ their monthly average in November. Discount-driven.'
        WHEN 'S02' THEN 'Customers who spent 1.5x+ their monthly average in December. Festive spenders.'
        WHEN 'S03' THEN 'Customers with elevated January spend. Likely parents preparing for school.'
        WHEN 'S04' THEN 'Customers who concentrate 50%+ of transactions around pay day (25th-3rd).'
        WHEN 'X01' THEN 'Active in 3+ premium categories (fashion, travel, dining, health, electronics).'
        WHEN 'X02' THEN 'Significant spend in both groceries AND clothing. Strong cross-sell opportunity.'
        ELSE 'Custom audience segment.'
    END AS description

FROM `__PROJECT__.marts.mart_audience_members` am
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON am.UNIQUE_ID = c.UNIQUE_ID
LEFT JOIN `__PROJECT__.analytics.int_rfm_features` r ON am.UNIQUE_ID = r.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_cluster_output` co ON am.UNIQUE_ID = co.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_churn_risk` ch ON am.UNIQUE_ID = ch.UNIQUE_ID
GROUP BY am.audience_id, am.audience_name, am.audience_type
HAVING COUNT(DISTINCT am.UNIQUE_ID) >= 1000;
