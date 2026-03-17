-- mart_spend_momentum.sql
-- tracks spend acceleration/deceleration per customer
-- a customer spending R5k/month but trending down 10% monthly is more urgent
-- than one spending R2k but trending up

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_spend_momentum`
CLUSTER BY momentum_status
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 6 MONTH) AS mid_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- monthly spend per customer for the last 12 months
monthly AS (
    SELECT
        t.UNIQUE_ID,
        DATE_TRUNC(t.EFF_DATE, MONTH) AS month,
        ROUND(SUM(t.trns_amt), 2) AS monthly_spend,
        COUNT(*) AS monthly_txns
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
    GROUP BY t.UNIQUE_ID, DATE_TRUNC(t.EFF_DATE, MONTH)
),

-- recent 6m vs prior 6m averages
halves AS (
    SELECT
        m.UNIQUE_ID,
        ROUND(AVG(CASE WHEN m.month >= d.mid_date THEN m.monthly_spend END), 2) AS avg_recent_6m,
        ROUND(AVG(CASE WHEN m.month < d.mid_date THEN m.monthly_spend END), 2) AS avg_prior_6m,
        ROUND(AVG(CASE WHEN m.month >= d.mid_date THEN m.monthly_txns END), 1) AS avg_recent_txns,
        ROUND(AVG(CASE WHEN m.month < d.mid_date THEN m.monthly_txns END), 1) AS avg_prior_txns,
        COUNT(DISTINCT m.month) AS active_months,
        ROUND(SUM(m.monthly_spend), 2) AS total_spend_12m
    FROM monthly m
    CROSS JOIN date_bounds d
    GROUP BY m.UNIQUE_ID
    HAVING COUNT(DISTINCT m.month) >= 3
)

SELECT
    h.UNIQUE_ID,
    h.total_spend_12m,
    h.active_months,
    h.avg_recent_6m,
    h.avg_prior_6m,
    h.avg_recent_txns,
    h.avg_prior_txns,

    -- spend momentum: % change in avg monthly spend
    ROUND(SAFE_DIVIDE(h.avg_recent_6m - h.avg_prior_6m, NULLIF(h.avg_prior_6m, 0)) * 100, 1) AS spend_change_pct,

    -- frequency momentum
    ROUND(SAFE_DIVIDE(h.avg_recent_txns - h.avg_prior_txns, NULLIF(h.avg_prior_txns, 0)) * 100, 1) AS txn_change_pct,

    -- momentum status
    CASE
        WHEN h.avg_prior_6m IS NULL OR h.avg_prior_6m = 0 THEN 'New'
        WHEN SAFE_DIVIDE(h.avg_recent_6m - h.avg_prior_6m, h.avg_prior_6m) > 0.2 THEN 'Accelerating'
        WHEN SAFE_DIVIDE(h.avg_recent_6m - h.avg_prior_6m, h.avg_prior_6m) > -0.05 THEN 'Steady'
        WHEN SAFE_DIVIDE(h.avg_recent_6m - h.avg_prior_6m, h.avg_prior_6m) > -0.3 THEN 'Slowing'
        ELSE 'Declining'
    END AS momentum_status,

    -- priority score: high spend + declining = most urgent
    ROUND(
        SAFE_DIVIDE(h.total_spend_12m, 1000) *
        GREATEST(COALESCE(1 - SAFE_DIVIDE(h.avg_recent_6m, NULLIF(h.avg_prior_6m, 0)), 0), 0)
    , 2) AS urgency_score,

    c.age, c.gender_label, c.income_segment, c.age_group
FROM halves h
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON h.UNIQUE_ID = c.UNIQUE_ID;
