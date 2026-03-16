-- train_churn_model.sql
-- logistic regression to predict churn based on 15 features
-- splits history into 9mo observation + 3mo outcome window
-- churned = had txns in observation but zero in outcome period
-- source: staging.stg_transactions, stg_customers -> analytics.churn_classifier

CREATE OR REPLACE MODEL `__PROJECT__.analytics.churn_classifier`
OPTIONS (
    model_type = 'LOGISTIC_REG',
    input_label_cols = ['churned'],
    auto_class_weights = TRUE,
    max_iterations = 50,
    data_split_method = 'AUTO_SPLIT'
) AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 3 MONTH) AS outcome_start,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS obs_start
    FROM `__PROJECT__.staging.stg_transactions`
),

-- observation period features (months 1-9)
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
        COUNTIF(t.trns_dow IN (1, 7)) AS NR_TRNS_WEEKEND,
        COUNTIF(t.trns_dow NOT IN (1, 7)) AS NR_TRNS_WEEK,

        -- time of day %
        ROUND(COUNTIF(t.trns_hour BETWEEN 6 AND 10) * 100.0 / COUNT(*), 1) AS pct_morning,
        ROUND(COUNTIF(t.trns_hour BETWEEN 17 AND 21) * 100.0 / COUNT(*), 1) AS pct_evening,

        -- trend: recent 3m vs prior 3m txn ratio, declining = pre-churn
        ROUND(SAFE_DIVIDE(
            COUNTIF(t.EFF_DATE >= DATE_SUB((SELECT outcome_start FROM date_bounds), INTERVAL 3 MONTH)),
            NULLIF(COUNTIF(t.EFF_DATE < DATE_SUB((SELECT outcome_start FROM date_bounds), INTERVAL 6 MONTH)), 0)
        ), 2) AS txn_trend

    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.obs_start
      AND t.EFF_DATE < d.outcome_start
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 3
),

-- outcome: did they transact in last 3 months?
outcome AS (
    SELECT
        t.UNIQUE_ID,
        1 AS came_back
    FROM `__PROJECT__.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.outcome_start
    GROUP BY t.UNIQUE_ID
),

-- label: active in observation but didnt come back = churned
labelled AS (
    SELECT
        o.*,
        CASE WHEN oc.came_back IS NULL THEN 1 ELSE 0 END AS churned
    FROM observation o
    LEFT JOIN outcome oc ON o.UNIQUE_ID = oc.UNIQUE_ID
)

SELECT
    l.nr_trns,
    l.val_trns,
    l.avg_val,
    l.active_months,
    l.days_between,
    l.active_destinations,
    l.active_nav_categories,
    l.NR_TRNS_WEEKEND,
    l.NR_TRNS_WEEK,
    l.pct_morning,
    l.pct_evening,
    COALESCE(l.txn_trend, 0) AS txn_trend,
    COALESCE(c.age, 0) AS age,
    COALESCE(c.estimated_income, 0) AS estimated_income,
    COALESCE(c.main_banked, 0) AS main_banked,
    l.churned

FROM labelled l
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON l.UNIQUE_ID = c.UNIQUE_ID;
