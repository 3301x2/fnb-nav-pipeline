-- predict_clv.sql
-- scores every active customer with predicted lifetime value
-- source: clv_predictor model + stg_transactions + stg_customers -> marts.mart_customer_clv

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_customer_clv`
CLUSTER BY clv_tier
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

current_features AS (
    SELECT
        t.UNIQUE_ID,
        COUNT(*) AS nr_trns,
        ROUND(SUM(t.trns_amt), 2) AS val_trns,
        ROUND(AVG(t.trns_amt), 2) AS avg_val,
        COUNT(DISTINCT FORMAT_DATE('%Y-%m', t.EFF_DATE)) AS active_months,
        ROUND(SAFE_DIVIDE(
            DATE_DIFF(MAX(t.EFF_DATE), MIN(t.EFF_DATE), DAY),
            NULLIF(COUNT(*) - 1, 0)
        ), 2) AS days_between,
        COUNT(DISTINCT t.DESTINATION_ID) AS active_destinations,
        COUNT(DISTINCT t.NAV_CATEGORY_ID) AS active_nav_categories,
        COUNTIF(t.trns_dow IN (1, 7)) AS nr_trns_weekend,
        COUNTIF(t.trns_dow NOT IN (1, 7)) AS nr_trns_week,
        DATE_DIFF((SELECT max_date FROM date_bounds), MAX(t.EFF_DATE), DAY) AS recency_days,
        COALESCE(ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH) THEN t.trns_amt ELSE 0 END),
            NULLIF(SUM(CASE WHEN t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 9 MONTH) THEN t.trns_amt ELSE 0 END), 0)
        ), 2), 0) AS spend_trend
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 3
),

model_input AS (
    SELECT
        cf.UNIQUE_ID,
        cf.nr_trns, cf.val_trns, cf.avg_val, cf.active_months,
        cf.days_between, cf.active_destinations, cf.active_nav_categories,
        cf.nr_trns_weekend, cf.nr_trns_week, cf.recency_days,
        cf.spend_trend,
        COALESCE(c.age, 0) AS age,
        COALESCE(c.estimated_income, 0) AS estimated_income,
        COALESCE(c.main_banked, 0) AS main_banked
    FROM current_features cf
    LEFT JOIN `__PROJECT__.staging.stg_customers` c ON cf.UNIQUE_ID = c.UNIQUE_ID
),

predictions AS (
    SELECT * FROM ML.PREDICT(
        MODEL `__PROJECT__.analytics.clv_predictor`,
        (SELECT * FROM model_input)
    )
),

-- cap extreme predictions at 99th percentile to avoid outlier extrapolation
clv_cap AS (
    SELECT APPROX_QUANTILES(GREATEST(predicted_future_spend, 0), 100)[OFFSET(99)] AS p99
    FROM predictions
),

-- assign tiers using NTILE (guarantees exactly 5 groups)
ranked AS (
    SELECT
        p.UNIQUE_ID,
        ROUND(LEAST(GREATEST(p.predicted_future_spend, 0), cap.p99), 2) AS predicted_clv,
        p.val_trns AS historical_spend,
        p.nr_trns,
        p.active_months,
        p.recency_days,
        p.spend_trend,
        NTILE(5) OVER (ORDER BY LEAST(GREATEST(p.predicted_future_spend, 0), cap.p99) ASC) AS clv_quintile
    FROM predictions p
    CROSS JOIN clv_cap cap
)

SELECT
    r.UNIQUE_ID,
    r.predicted_clv,
    r.historical_spend,
    r.nr_trns,
    r.active_months,
    r.recency_days,
    r.spend_trend,
    CASE r.clv_quintile
        WHEN 5 THEN 'Platinum'
        WHEN 4 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        WHEN 2 THEN 'Bronze'
        WHEN 1 THEN 'Basic'
    END AS clv_tier,
    c.age, c.gender_label, c.income_segment, c.age_group, c.income_group
FROM ranked r
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON r.UNIQUE_ID = c.UNIQUE_ID;
