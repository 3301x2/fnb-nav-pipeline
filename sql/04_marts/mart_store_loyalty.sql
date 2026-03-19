-- mart_store_loyalty.sql
-- How many stores do customers visit within a category?
-- Which stores have the most loyal customers?
-- Reads from: int_customer_category_spend (cheap, already aggregated)

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_store_loyalty` AS

WITH customer_store_count AS (
    SELECT
        UNIQUE_ID,
        CATEGORY_TWO,
        COUNT(DISTINCT DESTINATION) AS stores_visited,
        SUM(dest_spend) AS total_category_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID, CATEGORY_TWO
),

customer_loyalty AS (
    SELECT
        cs.UNIQUE_ID,
        cs.CATEGORY_TWO,
        cs.DESTINATION,
        cs.dest_spend,
        ROUND(cs.share_of_wallet_pct, 1) AS loyalty_pct,
        csc.stores_visited,
        CASE
            WHEN csc.stores_visited = 1 THEN '1 store (Very loyal)'
            WHEN csc.stores_visited = 2 THEN '2 stores'
            WHEN csc.stores_visited BETWEEN 3 AND 4 THEN '3-4 stores'
            WHEN csc.stores_visited BETWEEN 5 AND 7 THEN '5-7 stores'
            ELSE '8+ stores (Variety seeker)'
        END AS loyalty_band
    FROM `__PROJECT__.analytics.int_customer_category_spend` cs
    JOIN customer_store_count csc
        ON cs.UNIQUE_ID = csc.UNIQUE_ID
        AND cs.CATEGORY_TWO = csc.CATEGORY_TWO
)

SELECT
    CATEGORY_TWO,
    DESTINATION,
    COUNT(DISTINCT UNIQUE_ID) AS customers,
    ROUND(AVG(loyalty_pct), 1) AS avg_loyalty_pct,
    ROUND(AVG(stores_visited), 1) AS avg_stores_visited,
    COUNTIF(loyalty_pct >= 50) AS loyal_customers_50pct,
    ROUND(COUNTIF(loyalty_pct >= 50) * 100.0 / COUNT(*), 1) AS pct_loyal_50,
    COUNTIF(loyalty_pct >= 80) AS loyal_customers_80pct,
    ROUND(COUNTIF(loyalty_pct >= 80) * 100.0 / COUNT(*), 1) AS pct_loyal_80,

    -- Loyalty band distribution
    COUNTIF(loyalty_band = '1 store (Very loyal)') AS band_1_store,
    COUNTIF(loyalty_band = '2 stores') AS band_2_stores,
    COUNTIF(loyalty_band = '3-4 stores') AS band_3_4_stores,
    COUNTIF(loyalty_band = '5-7 stores') AS band_5_7_stores,
    COUNTIF(loyalty_band = '8+ stores (Variety seeker)') AS band_8_plus,

    ROUND(SUM(dest_spend), 0) AS total_spend,
    ROUND(AVG(dest_spend), 0) AS avg_spend_per_customer
FROM customer_loyalty
GROUP BY CATEGORY_TWO, DESTINATION
HAVING COUNT(DISTINCT UNIQUE_ID) >= 100;
