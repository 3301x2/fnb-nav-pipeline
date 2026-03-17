-- train_clv_model.sql
-- predicts customer lifetime value (spend over next 12 months)
-- uses historical behavioural features to forecast future spend
-- source: stg_transactions, stg_customers -> analytics.clv_predictor

CREATE OR REPLACE MODEL `__PROJECT__.analytics.clv_predictor`
OPTIONS (
    model_type = 'LINEAR_REG',
    input_label_cols = ['future_spend'],
    data_split_method = 'AUTO_SPLIT',
    max_iterations = 50
) AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 6 MONTH) AS split_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 18 MONTH) AS obs_start
    FROM `__PROJECT__.staging.stg_transactions`
),

-- features from first 12 months (observation)
observation AS (
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
        DATE_DIFF(
            (SELECT split_date FROM date_bounds),
            MAX(t.EFF_DATE), DAY
        ) AS recency_days,
        -- spend trend within observation
        ROUND(SAFE_DIVIDE(
            SUM(CASE WHEN t.EFF_DATE >= DATE_SUB((SELECT split_date FROM date_bounds), INTERVAL 3 MONTH) THEN t.trns_amt ELSE 0 END),
            NULLIF(SUM(CASE WHEN t.EFF_DATE < DATE_SUB((SELECT split_date FROM date_bounds), INTERVAL 9 MONTH) THEN t.trns_amt ELSE 0 END), 0)
        ), 2) AS spend_trend
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.obs_start
      AND t.EFF_DATE < d.split_date
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 3
),

-- label: actual spend in the next 6 months (what we want to predict)
future AS (
    SELECT
        t.UNIQUE_ID,
        ROUND(SUM(t.trns_amt), 2) AS future_spend
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.split_date
    GROUP BY t.UNIQUE_ID
)

SELECT
    o.nr_trns,
    o.val_trns,
    o.avg_val,
    o.active_months,
    o.days_between,
    o.active_destinations,
    o.active_nav_categories,
    o.nr_trns_weekend,
    o.nr_trns_week,
    o.recency_days,
    COALESCE(o.spend_trend, 0) AS spend_trend,
    COALESCE(c.age, 0) AS age,
    COALESCE(c.estimated_income, 0) AS estimated_income,
    COALESCE(c.main_banked, 0) AS main_banked,
    COALESCE(f.future_spend, 0) AS future_spend
FROM observation o
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON o.UNIQUE_ID = c.UNIQUE_ID
LEFT JOIN future f ON o.UNIQUE_ID = f.UNIQUE_ID;
