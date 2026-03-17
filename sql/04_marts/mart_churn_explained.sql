-- mart_churn_explained.sql
-- uses ML.EXPLAIN_PREDICT to show WHY each customer is flagged as at-risk
-- gives the top 3 features driving each churn prediction
-- source: churn_classifier model + stg_transactions + stg_customers

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_churn_explained`
CLUSTER BY churn_risk_level
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
        COUNTIF(t.trns_dow IN (1, 7)) AS NR_TRNS_WEEKEND,
        COUNTIF(t.trns_dow NOT IN (1, 7)) AS NR_TRNS_WEEK,
        ROUND(COUNTIF(t.trns_hour BETWEEN 6 AND 10) * 100.0 / COUNT(*), 1) AS pct_morning,
        ROUND(COUNTIF(t.trns_hour BETWEEN 17 AND 21) * 100.0 / COUNT(*), 1) AS pct_evening,
        COALESCE(ROUND(SAFE_DIVIDE(
            COUNTIF(t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)),
            NULLIF(COUNTIF(t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 9 MONTH)), 0)
        ), 2), 0) AS txn_trend
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
        cf.NR_TRNS_WEEKEND, cf.NR_TRNS_WEEK,
        cf.pct_morning, cf.pct_evening, cf.txn_trend,
        COALESCE(c.age, 0) AS age,
        COALESCE(c.estimated_income, 0) AS estimated_income,
        COALESCE(c.main_banked, 0) AS main_banked
    FROM current_features cf
    LEFT JOIN `__PROJECT__.staging.stg_customers` c ON cf.UNIQUE_ID = c.UNIQUE_ID
),

-- limit to high-risk customers to keep costs reasonable
high_risk AS (
    SELECT mi.*
    FROM model_input mi
    JOIN `__PROJECT__.marts.mart_churn_risk` cr ON mi.UNIQUE_ID = cr.UNIQUE_ID
    WHERE cr.churn_risk_level IN ('Critical', 'High')
),

explained AS (
    SELECT *
    FROM ML.EXPLAIN_PREDICT(
        MODEL `__PROJECT__.analytics.churn_classifier`,
        (SELECT * FROM high_risk),
        STRUCT(3 AS top_k_features)
    )
)

SELECT
    e.UNIQUE_ID,
    ROUND((
        SELECT prob.prob FROM UNNEST(e.predicted_churned_probs) AS prob WHERE prob.label = 1
    ), 4) AS churn_probability,
    CASE
        WHEN (SELECT prob.prob FROM UNNEST(e.predicted_churned_probs) AS prob WHERE prob.label = 1) >= 0.8 THEN 'Critical'
        WHEN (SELECT prob.prob FROM UNNEST(e.predicted_churned_probs) AS prob WHERE prob.label = 1) >= 0.6 THEN 'High'
        ELSE 'Medium'
    END AS churn_risk_level,
    -- top 3 reasons for the prediction
    (SELECT feature FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 0) AS reason_1,
    (SELECT ROUND(attribution, 4) FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 0) AS reason_1_weight,
    (SELECT feature FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 1) AS reason_2,
    (SELECT ROUND(attribution, 4) FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 1) AS reason_2_weight,
    (SELECT feature FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 2) AS reason_3,
    (SELECT ROUND(attribution, 4) FROM UNNEST(e.top_feature_attributions) WITH OFFSET AS pos ORDER BY pos LIMIT 1 OFFSET 2) AS reason_3_weight,
    e.val_trns AS total_spend,
    e.nr_trns,
    e.active_months,
    e.txn_trend
FROM explained e;
