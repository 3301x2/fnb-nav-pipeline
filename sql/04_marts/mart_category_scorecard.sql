-- mart_category_scorecard.sql
-- one row per category with all the key metrics for a birds-eye portfolio view
-- growth trend, churn exposure, segment mix, top destinations

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_category_scorecard` AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 3 MONTH) AS recent_start,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 6 MONTH) AS prior_start
    FROM `__PROJECT__.staging.stg_transactions`
),

-- recent 3 months vs prior 3 months spend per category
trend AS (
    SELECT
        t.CATEGORY_TWO,
        SUM(CASE WHEN t.EFF_DATE >= d.recent_start THEN t.trns_amt ELSE 0 END) AS spend_recent_3m,
        SUM(CASE WHEN t.EFF_DATE >= d.prior_start AND t.EFF_DATE < d.recent_start THEN t.trns_amt ELSE 0 END) AS spend_prior_3m
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.CATEGORY_TWO IS NOT NULL
      AND t.EFF_DATE >= d.prior_start
    GROUP BY t.CATEGORY_TWO
),

-- benchmarks aggregated per category (true distinct customers, not sum of per-dest counts)
cat_benchmarks AS (
    SELECT
        b.CATEGORY_TWO,
        COUNT(DISTINCT b.DESTINATION) AS num_destinations,
        ct.cat_total_customers AS total_customers,
        SUM(b.total_spend) AS total_spend,
        ROUND(AVG(b.avg_txn_value), 2) AS avg_txn_value,
        ROUND(AVG(b.spend_per_customer), 0) AS avg_spend_per_customer
    FROM `__PROJECT__.marts.mart_destination_benchmarks` b
    JOIN (
        SELECT CATEGORY_TWO, COUNT(DISTINCT UNIQUE_ID) AS cat_total_customers
        FROM `__PROJECT__.analytics.int_customer_category_spend`
        GROUP BY CATEGORY_TWO
    ) ct ON b.CATEGORY_TWO = ct.CATEGORY_TWO
    GROUP BY b.CATEGORY_TWO, ct.cat_total_customers
),

-- top destination per category (by spend)
top_dest AS (
    SELECT CATEGORY_TWO, DESTINATION AS top_destination_name, total_spend AS top_dest_spend,
           market_share_pct AS top_dest_share
    FROM `__PROJECT__.marts.mart_destination_benchmarks`
    WHERE spend_rank = 1
),

-- churn exposure per category
cat_churn AS (
    SELECT
        cs.CATEGORY_TWO,
        COUNT(DISTINCT cs.UNIQUE_ID) AS scored_customers,
        ROUND(AVG(cr.churn_probability) * 100, 1) AS avg_churn_pct,
        COUNTIF(cr.churn_risk_level IN ('Critical', 'High')) AS high_risk_count,
        ROUND(COUNTIF(cr.churn_risk_level IN ('Critical', 'High')) * 100.0 / COUNT(DISTINCT cs.UNIQUE_ID), 1) AS high_risk_pct
    FROM (
        SELECT DISTINCT UNIQUE_ID, CATEGORY_TWO
        FROM `__PROJECT__.analytics.int_customer_category_spend`
        WHERE CATEGORY_TWO IS NOT NULL
    ) cs
    JOIN `__PROJECT__.marts.mart_churn_risk` cr ON cs.UNIQUE_ID = cr.UNIQUE_ID
    GROUP BY cs.CATEGORY_TWO
),

-- segment mix per category (what % of each category is Champions vs Dormant etc)
seg_mix AS (
    SELECT
        cs.CATEGORY_TWO,
        ROUND(COUNTIF(co.segment_name = 'Champions') * 100.0 / COUNT(*), 1) AS pct_champions,
        ROUND(COUNTIF(co.segment_name = 'Dormant') * 100.0 / COUNT(*), 1) AS pct_dormant
    FROM (
        SELECT DISTINCT UNIQUE_ID, CATEGORY_TWO
        FROM `__PROJECT__.analytics.int_customer_category_spend`
        WHERE CATEGORY_TWO IS NOT NULL
    ) cs
    JOIN `__PROJECT__.marts.mart_cluster_output` co ON cs.UNIQUE_ID = co.UNIQUE_ID
    GROUP BY cs.CATEGORY_TWO
)

SELECT
    cb.CATEGORY_TWO,
    cb.num_destinations,
    cb.total_customers,
    ROUND(cb.total_spend, 0) AS total_spend,
    cb.avg_txn_value,
    cb.avg_spend_per_customer,

    -- growth trend
    ROUND(t.spend_recent_3m, 0) AS spend_recent_3m,
    ROUND(t.spend_prior_3m, 0) AS spend_prior_3m,
    ROUND(SAFE_DIVIDE(t.spend_recent_3m - t.spend_prior_3m, NULLIF(t.spend_prior_3m, 0)) * 100, 1) AS growth_pct,

    -- top destination
    td.top_destination_name,
    td.top_dest_share AS top_dest_market_share,

    -- churn exposure
    COALESCE(cc.avg_churn_pct, 0) AS avg_churn_pct,
    COALESCE(cc.high_risk_count, 0) AS high_risk_customers,
    COALESCE(cc.high_risk_pct, 0) AS high_risk_pct,

    -- segment mix
    COALESCE(sm.pct_champions, 0) AS pct_champions,
    COALESCE(sm.pct_dormant, 0) AS pct_dormant,

    -- health indicator
    CASE
        WHEN SAFE_DIVIDE(t.spend_recent_3m - t.spend_prior_3m, NULLIF(t.spend_prior_3m, 0)) > 0.1 THEN 'Growing'
        WHEN SAFE_DIVIDE(t.spend_recent_3m - t.spend_prior_3m, NULLIF(t.spend_prior_3m, 0)) > -0.05 THEN 'Stable'
        WHEN SAFE_DIVIDE(t.spend_recent_3m - t.spend_prior_3m, NULLIF(t.spend_prior_3m, 0)) > -0.15 THEN 'Slowing'
        ELSE 'Declining'
    END AS health_status

FROM cat_benchmarks cb
LEFT JOIN trend t ON cb.CATEGORY_TWO = t.CATEGORY_TWO
LEFT JOIN top_dest td ON cb.CATEGORY_TWO = td.CATEGORY_TWO
LEFT JOIN cat_churn cc ON cb.CATEGORY_TWO = cc.CATEGORY_TWO
LEFT JOIN seg_mix sm ON cb.CATEGORY_TWO = sm.CATEGORY_TWO
ORDER BY cb.total_spend DESC;
