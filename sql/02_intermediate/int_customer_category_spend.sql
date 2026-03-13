-- ════════════════════════════════════════════════════════════════
-- int_customer_category_spend.sql
-- ════════════════════════════════════════════════════════════════
-- For every customer, calculates their spend within each
-- CATEGORY_TWO — and their spend at each DESTINATION within
-- that category. This is the foundation for share-of-wallet
-- analysis at the dashboard level.
--
-- The dashboard picks a client (e.g. Adidas) and this table
-- already has: how much each customer spent at Adidas vs the
-- total Clothing & Apparel category. No re-running SQL needed.
--
-- Source: staging.stg_transactions
-- Target: analytics.int_customer_category_spend
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.analytics.int_customer_category_spend`
PARTITION BY DATE_TRUNC(last_txn_date, MONTH)
CLUSTER BY CATEGORY_TWO, DESTINATION
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE)                              AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `fmn-sandbox.staging.stg_transactions`
),

-- Per customer × destination within category
customer_destination AS (
    SELECT
        t.UNIQUE_ID,
        t.CATEGORY_TWO,
        t.DESTINATION,
        COUNT(*)                                   AS dest_txn_count,
        ROUND(SUM(t.trns_amt), 2)                 AS dest_spend,
        MAX(t.EFF_DATE)                            AS last_txn_date
    FROM `fmn-sandbox.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
      AND t.CATEGORY_TWO IS NOT NULL
      AND t.DESTINATION IS NOT NULL
    GROUP BY t.UNIQUE_ID, t.CATEGORY_TWO, t.DESTINATION
),

-- Per customer × category totals
customer_category AS (
    SELECT
        UNIQUE_ID,
        CATEGORY_TWO,
        SUM(dest_txn_count)                        AS category_txn_count,
        ROUND(SUM(dest_spend), 2)                  AS category_spend
    FROM customer_destination
    GROUP BY UNIQUE_ID, CATEGORY_TWO
)

SELECT
    cd.UNIQUE_ID,
    cd.CATEGORY_TWO,
    cd.DESTINATION,
    cd.dest_txn_count,
    cd.dest_spend,
    cd.last_txn_date,
    cc.category_txn_count,
    cc.category_spend,

    -- Share of wallet: what % of this customer's category spend
    -- goes to this specific destination?
    ROUND(SAFE_DIVIDE(cd.dest_spend, cc.category_spend) * 100, 1) AS share_of_wallet_pct

FROM customer_destination cd
JOIN customer_category cc
    ON cd.UNIQUE_ID = cc.UNIQUE_ID
    AND cd.CATEGORY_TWO = cc.CATEGORY_TWO;
