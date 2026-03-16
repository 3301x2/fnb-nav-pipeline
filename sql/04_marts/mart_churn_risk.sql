-- mart_churn_risk.sql
-- customer level churn scoring, compares last 3m vs previous 3m
-- rule-based for now, ML version is in predict_churn.sql

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_churn_risk`
CLUSTER BY risk_level
AS

WITH date_bounds AS (
    SELECT MAX(EFF_DATE) AS max_date
    FROM `__PROJECT__.staging.stg_transactions`
),

activity AS (
    SELECT
        t.UNIQUE_ID,
        COUNT(*)                                               AS total_txns,
        ROUND(SUM(t.trns_amt), 2)                             AS total_spend,
        DATE_DIFF(
            (SELECT max_date FROM date_bounds),
            MAX(t.EFF_DATE), DAY
        )                                                      AS days_since_last,
        COUNTIF(t.EFF_DATE >= DATE_SUB(
            (SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)
        )                                                      AS txns_last_3m,
        COUNTIF(
            t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 6 MONTH)
            AND t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)
        )                                                      AS txns_prev_3m,
        ROUND(SUM(CASE
            WHEN t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)
            THEN t.trns_amt ELSE 0 END), 2)                    AS spend_last_3m,
        ROUND(SUM(CASE
            WHEN t.EFF_DATE >= DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 6 MONTH)
             AND t.EFF_DATE < DATE_SUB((SELECT max_date FROM date_bounds), INTERVAL 3 MONTH)
            THEN t.trns_amt ELSE 0 END), 2)                    AS spend_prev_3m
    FROM `__PROJECT__.staging.stg_transactions` t
    GROUP BY t.UNIQUE_ID
    HAVING COUNT(*) >= 2
)

SELECT
    a.UNIQUE_ID,
    a.total_txns,
    a.total_spend,
    a.days_since_last,
    a.txns_last_3m,
    a.txns_prev_3m,
    a.spend_last_3m,
    a.spend_prev_3m,

    CASE
        WHEN a.days_since_last > 180 THEN 'Churned'
        WHEN a.days_since_last > 90
             AND a.txns_last_3m < a.txns_prev_3m THEN 'High Risk'
        WHEN a.days_since_last > 60
             OR a.txns_last_3m < a.txns_prev_3m * 0.5 THEN 'Medium Risk'
        WHEN a.txns_last_3m >= a.txns_prev_3m THEN 'Low Risk'
        ELSE 'Stable'
    END                                                        AS risk_level,

    -- demographics
    c.age,
    c.gender_label,
    c.income_segment,
    c.credit_risk_class,
    c.age_group,
    c.income_group

FROM activity a
LEFT JOIN `__PROJECT__.staging.stg_customers` c
    ON a.UNIQUE_ID = c.UNIQUE_ID;
