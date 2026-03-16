-- mart_category_affinity.sql
-- shows which categories are commonly shopped together
-- eg customers who buy Clothing also buy Footwear 3.2x more than average

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_category_affinity` AS

WITH date_bounds AS (
    SELECT DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- customers and which categories they shop in
customer_categories AS (
    SELECT DISTINCT
        t.UNIQUE_ID,
        t.CATEGORY_TWO
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
      AND t.CATEGORY_TWO IS NOT NULL
),

-- total customers in the dataset
total_customers AS (
    SELECT COUNT(DISTINCT UNIQUE_ID) AS total FROM customer_categories
),

-- how many customers shop each category
category_customers AS (
    SELECT CATEGORY_TWO, COUNT(DISTINCT UNIQUE_ID) AS cat_customers
    FROM customer_categories
    GROUP BY CATEGORY_TWO
),

-- pairs: customers who shop both category A and category B
pairs AS (
    SELECT
        a.CATEGORY_TWO AS category_a,
        b.CATEGORY_TWO AS category_b,
        COUNT(DISTINCT a.UNIQUE_ID) AS shared_customers
    FROM customer_categories a
    JOIN customer_categories b ON a.UNIQUE_ID = b.UNIQUE_ID
    WHERE a.CATEGORY_TWO < b.CATEGORY_TWO
    GROUP BY a.CATEGORY_TWO, b.CATEGORY_TWO
    HAVING COUNT(DISTINCT a.UNIQUE_ID) >= 100
)

SELECT
    p.category_a,
    p.category_b,
    p.shared_customers,
    ca.cat_customers AS customers_in_a,
    cb.cat_customers AS customers_in_b,

    -- of people who shop A, what % also shop B
    ROUND(p.shared_customers * 100.0 / ca.cat_customers, 1) AS pct_a_also_shops_b,

    -- of people who shop B, what % also shop A
    ROUND(p.shared_customers * 100.0 / cb.cat_customers, 1) AS pct_b_also_shops_a,

    -- lift: how much more likely are they to shop both vs random chance
    ROUND(SAFE_DIVIDE(
        p.shared_customers * (SELECT total FROM total_customers),
        ca.cat_customers * cb.cat_customers
    ), 2) AS lift,

    -- jaccard similarity (overlap / union)
    ROUND(SAFE_DIVIDE(
        p.shared_customers,
        ca.cat_customers + cb.cat_customers - p.shared_customers
    ) * 100, 1) AS jaccard_pct

FROM pairs p
JOIN category_customers ca ON p.category_a = ca.CATEGORY_TWO
JOIN category_customers cb ON p.category_b = cb.CATEGORY_TWO
ORDER BY lift DESC;
