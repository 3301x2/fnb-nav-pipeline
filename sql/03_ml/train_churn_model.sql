-- ════════════════════════════════════════════════════════════════
-- train_churn_model.sql
-- ════════════════════════════════════════════════════════════════
-- Trains a supervised ML model to predict customer churn.
--
-- APPROACH:
--   We split history into two windows:
--   - "observation period": 9 months of behavior data (features)
--   - "outcome period": last 3 months (did they come back or not?)
--
--   A customer is labelled "churned" (1) if they had transactions
--   in the observation period but ZERO in the outcome period.
--
--   The model learns which behavioral patterns (declining frequency,
--   fewer merchants, shifting time-of-day) predict churn.
--
-- MODEL: BOOSTED_TREE_CLASSIFIER (gradient boosted decision tree)
--   - Handles non-linear relationships
--   - Provides feature importance
--   - Auto train/test split (80/20)
--   - Outputs probability scores (0.0 to 1.0)
--
-- FEATURES (15):
--   From RFM: val_trns, nr_trns, avg_val, active_months, days_between,
--             active_destinations, active_nav_categories,
--             NR_TRNS_WEEKEND, NR_TRNS_WEEK
--   Temporal: pct_morning, pct_evening
--   Trend:    txn_trend (recent 3m vs prior 3m ratio within observation)
--   Demo:     age, estimated_income, main_banked
--
-- Source: staging.stg_transactions, staging.stg_customers
-- Target: analytics.churn_classifier (MODEL)
-- Runtime: ~3-5 minutes on production data
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE MODEL `fmn-sandbox.analytics.churn_classifier`
OPTIONS (
    model_type = 'BOOSTED_TREE_CLASSIFIER',
    input_label_cols = ['churned'],
    auto_class_weights = TRUE,
    max_iterations = 50,
    learn_rate = 0.1,
    data_split_method = 'AUTO_SPLIT'
) AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 3 MONTH) AS outcome_start,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS obs_start
    FROM `fmn-sandbox.staging.stg_transactions`
),

-- Observation period features (months 1-9)
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

        -- Time-of-day percentages
        ROUND(COUNTIF(t.trns_hour BETWEEN 6 AND 10) * 100.0 / COUNT(*), 1) AS pct_morning,
        ROUND(COUNTIF(t.trns_hour BETWEEN 17 AND 21) * 100.0 / COUNT(*), 1) AS pct_evening,

        -- Trend: ratio of transactions in last 3 months of observation
        -- vs first 3 months. A declining trend signals pre-churn behavior.
        ROUND(SAFE_DIVIDE(
            COUNTIF(t.EFF_DATE >= DATE_SUB((SELECT outcome_start FROM date_bounds), INTERVAL 3 MONTH)),
            NULLIF(COUNTIF(t.EFF_DATE < DATE_SUB((SELECT outcome_start FROM date_bounds), INTERVAL 6 MONTH)), 0)
        ), 2) AS txn_trend

    FROM `fmn-sandbox.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.obs_start
      AND t.EFF_DATE < d.outcome_start
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 3
),

-- Outcome: did they transact in the last 3 months?
outcome AS (
    SELECT
        t.UNIQUE_ID,
        1 AS came_back
    FROM `fmn-sandbox.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.outcome_start
    GROUP BY t.UNIQUE_ID
),

-- Label: churned = was active in observation but did NOT come back
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
LEFT JOIN `fmn-sandbox.staging.stg_customers` c ON l.UNIQUE_ID = c.UNIQUE_ID;
