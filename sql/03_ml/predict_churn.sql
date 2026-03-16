-- predict_churn.sql
-- scores every active customer with ML churn probability
-- outputs churn_probability (0-1) and risk level (Critical to Stable)
-- source: churn_classifier model + stg_transactions + stg_customers

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_churn_risk`
CLUSTER BY churn_risk_level
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- build current features (same shape as training data)
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
        COUNTIF(t.trns_dow IN (1, 7)) AS NR_TRNS_WEEKEND,
        COUNTIF(t.trns_dow NOT IN (1, 7)) AS NR_TRNS_WEEK,
        ROUND(COUNTIF(t.trns_hour BETWEEN 6 AND 10) * 100.0 / COUNT(*), 1) AS pct_morning,
        ROUND(COUNTIF(t.trns_hour BETWEEN 17 AND 21) * 100.0 / COUNT(*), 1) AS pct_evening,
        COALESCE(ROUND(SAFE_DIVIDE(
            COUNTIF(t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)),
            NULLIF(COUNTIF(t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 6 MONTH)), 0)
        ), 2), 0) AS txn_trend,
        DATE_DIFF((SELECT max_date FROM date_bounds), MAX(t.EFF_DATE), DAY) AS days_since_last,
        COUNTIF(t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)) AS txns_last_3m,
        COUNTIF(
            t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 6 MONTH)
            AND t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)
        ) AS txns_prev_3m
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 3
),

-- model input (features only, matching training schema)
model_input AS (
    SELECT
        cf.UNIQUE_ID,
        cf.nr_trns,
        cf.val_trns,
        cf.avg_val,
        cf.active_months,
        cf.days_between,
        cf.active_destinations,
        cf.active_nav_categories,
        cf.NR_TRNS_WEEKEND,
        cf.NR_TRNS_WEEK,
        cf.pct_morning,
        cf.pct_evening,
        cf.txn_trend,
        COALESCE(c.age, 0) AS age,
        COALESCE(c.estimated_income, 0) AS estimated_income,
        COALESCE(c.main_banked, 0) AS main_banked
    FROM current_features cf
    LEFT JOIN `__PROJECT__.staging.stg_customers` c ON cf.UNIQUE_ID = c.UNIQUE_ID
),

-- run predictions
predictions AS (
    SELECT *
    FROM ML.PREDICT(
        MODEL `__PROJECT__.analytics.churn_classifier`,
        (SELECT * FROM model_input)
    )
),

-- extract probability
scored AS (
    SELECT
        p.UNIQUE_ID,
        p.predicted_churned AS predicted_label,
        ROUND((
            SELECT prob.prob
            FROM UNNEST(p.predicted_churned_probs) AS prob
            WHERE prob.label = 1
        ), 4) AS churn_probability
    FROM predictions p
)

SELECT
    s.UNIQUE_ID,
    s.churn_probability,
    s.predicted_label,
    CASE
        WHEN s.churn_probability >= 0.8 THEN 'Critical'
        WHEN s.churn_probability >= 0.6 THEN 'High'
        WHEN s.churn_probability >= 0.4 THEN 'Medium'
        WHEN s.churn_probability >= 0.2 THEN 'Low'
        ELSE 'Stable'
    END AS churn_risk_level,
    cf.nr_trns AS total_txns,
    cf.val_trns AS total_spend,
    cf.days_since_last,
    cf.txns_last_3m,
    cf.txns_prev_3m,
    cf.active_months,
    cf.active_destinations,
    cf.txn_trend,
    c.age,
    c.gender_label,
    c.income_segment,
    c.estimated_income,
    c.credit_risk_class,
    c.age_group,
    c.income_group
FROM scored s
JOIN current_features cf ON s.UNIQUE_ID = cf.UNIQUE_ID
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON s.UNIQUE_ID = c.UNIQUE_ID;
