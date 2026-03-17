-- mart_category_propensity.sql
-- for each customer, which new categories are they most likely to adopt?
-- based on what similar customers (same segment, demographics) already shop

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_category_propensity`
CLUSTER BY CATEGORY_TWO
AS

WITH date_bounds AS (
    SELECT DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- which categories does each customer already shop?
customer_cats AS (
    SELECT DISTINCT UNIQUE_ID, CATEGORY_TWO
    FROM `__PROJECT__.staging.stg_transactions`
    CROSS JOIN date_bounds d
    WHERE EFF_DATE >= d.start_date
      AND CATEGORY_TWO IS NOT NULL
),

-- segment-level adoption rates per category
segment_adoption AS (
    SELECT
        co.segment_name,
        cc.CATEGORY_TWO,
        COUNT(DISTINCT cc.UNIQUE_ID) AS segment_cat_customers,
        seg_total.segment_size,
        ROUND(SAFE_DIVIDE(COUNT(DISTINCT cc.UNIQUE_ID) * 100.0, seg_total.segment_size), 2) AS adoption_rate_pct
    FROM customer_cats cc
    JOIN `__PROJECT__.marts.mart_cluster_output` co ON cc.UNIQUE_ID = co.UNIQUE_ID
    JOIN (
        SELECT segment_name, COUNT(*) AS segment_size
        FROM `__PROJECT__.marts.mart_cluster_output`
        GROUP BY segment_name
    ) seg_total ON co.segment_name = seg_total.segment_name
    GROUP BY co.segment_name, cc.CATEGORY_TWO, seg_total.segment_size
),

-- category stats for sizing
category_stats AS (
    SELECT
        CATEGORY_TWO,
        COUNT(DISTINCT UNIQUE_ID) AS total_cat_customers,
        ROUND(SUM(dest_spend), 0) AS total_cat_spend,
        ROUND(AVG(dest_spend), 2) AS avg_spend_per_customer
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY CATEGORY_TWO
)

SELECT
    sa.segment_name,
    sa.CATEGORY_TWO,
    sa.segment_cat_customers,
    sa.segment_size,
    sa.adoption_rate_pct,
    cs.total_cat_customers,
    cs.total_cat_spend,
    cs.avg_spend_per_customer,

    -- unadopted customers: how many in this segment DONT shop this category yet
    sa.segment_size - sa.segment_cat_customers AS unadopted_customers,

    -- potential revenue: unadopted * avg spend if they adopt
    ROUND((sa.segment_size - sa.segment_cat_customers) * cs.avg_spend_per_customer, 0) AS potential_revenue,

    -- propensity signal: high adoption in segment = high propensity for unadopted
    CASE
        WHEN sa.adoption_rate_pct >= 60 THEN 'Very High'
        WHEN sa.adoption_rate_pct >= 40 THEN 'High'
        WHEN sa.adoption_rate_pct >= 20 THEN 'Medium'
        WHEN sa.adoption_rate_pct >= 10 THEN 'Low'
        ELSE 'Very Low'
    END AS propensity_level

FROM segment_adoption sa
JOIN category_stats cs ON sa.CATEGORY_TWO = cs.CATEGORY_TWO
WHERE sa.adoption_rate_pct < 90;  -- exclude categories everyone already shops
