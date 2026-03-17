-- mart_churn_explained.sql
-- explains why each high-risk customer is flagged for churn
-- uses feature importance from the model + each customer's actual feature values
-- source: churn_classifier model + mart_churn_risk + stg_transactions

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_churn_explained`
CLUSTER BY churn_risk_level
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE) AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- get the high risk customers with their features
high_risk_features AS (
    SELECT
        t.UNIQUE_ID,
        cr.churn_probability,
        cr.churn_risk_level,
        cr.total_spend,
        cr.days_since_last,
        cr.txns_last_3m,
        cr.txns_prev_3m,
        cr.active_months,
        cr.active_destinations,
        cr.txn_trend,
        -- compute per-customer signals that explain the churn
        CASE WHEN cr.days_since_last > 90 THEN 1 ELSE 0 END AS flag_inactive_long,
        CASE WHEN cr.txns_last_3m < cr.txns_prev_3m * 0.5 THEN 1 ELSE 0 END AS flag_frequency_dropped,
        CASE WHEN cr.active_destinations <= 2 THEN 1 ELSE 0 END AS flag_low_diversity,
        CASE WHEN cr.active_months <= 3 THEN 1 ELSE 0 END AS flag_low_engagement,
        ROUND(SAFE_DIVIDE(cr.txns_last_3m, NULLIF(cr.txns_prev_3m, 0)), 2) AS frequency_ratio
    FROM `__PROJECT__.marts.mart_churn_risk` cr
    LEFT JOIN `__PROJECT__.staging.stg_transactions` t ON cr.UNIQUE_ID = t.UNIQUE_ID
    WHERE cr.churn_risk_level IN ('Critical', 'High')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cr.UNIQUE_ID ORDER BY t.EFF_DATE DESC) = 1
)

SELECT
    UNIQUE_ID,
    ROUND(churn_probability, 4) AS churn_probability,
    churn_risk_level,
    total_spend,
    days_since_last,
    txns_last_3m,
    txns_prev_3m,
    active_months,
    active_destinations,
    COALESCE(txn_trend, 0) AS txn_trend,
    frequency_ratio,

    -- human readable explanation: build the top reasons
    CASE
        WHEN flag_inactive_long = 1 THEN 'Inactive for ' || CAST(days_since_last AS STRING) || ' days'
        WHEN flag_frequency_dropped = 1 THEN 'Transaction frequency dropped >50%'
        WHEN flag_low_engagement = 1 THEN 'Only active ' || CAST(active_months AS STRING) || ' months'
        ELSE 'Declining overall activity pattern'
    END AS reason_1,

    CASE
        WHEN flag_frequency_dropped = 1 AND flag_inactive_long = 0 THEN 'Frequency dropped from ' || CAST(txns_prev_3m AS STRING) || ' to ' || CAST(txns_last_3m AS STRING) || ' txns'
        WHEN flag_low_diversity = 1 THEN 'Only visiting ' || CAST(active_destinations AS STRING) || ' merchants'
        WHEN days_since_last > 60 THEN 'Last seen ' || CAST(days_since_last AS STRING) || ' days ago'
        ELSE 'Spending trend declining'
    END AS reason_2,

    CASE
        WHEN flag_low_diversity = 1 AND flag_frequency_dropped = 0 THEN 'Low merchant diversity (' || CAST(active_destinations AS STRING) || ')'
        WHEN COALESCE(txn_trend, 0) < 0.5 AND txn_trend IS NOT NULL THEN 'Spend trend ratio: ' || CAST(ROUND(txn_trend, 2) AS STRING)
        WHEN active_months <= 6 THEN 'Low engagement: ' || CAST(active_months AS STRING) || ' active months'
        ELSE 'Multiple declining signals'
    END AS reason_3

FROM high_risk_features;
