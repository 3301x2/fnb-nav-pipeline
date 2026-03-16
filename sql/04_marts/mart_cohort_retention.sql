-- mart_cohort_retention.sql
-- tracks customer retention by their first transaction month
-- shows how many customers from each cohort are still active after 1m, 3m, 6m, 12m

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_cohort_retention` AS

WITH date_bounds AS (
    SELECT MAX(EFF_DATE) AS max_date
    FROM `__PROJECT__.staging.stg_transactions`
),

-- find each customer's first transaction month
first_txn AS (
    SELECT
        UNIQUE_ID,
        DATE_TRUNC(MIN(EFF_DATE), MONTH) AS cohort_month
    FROM `__PROJECT__.staging.stg_transactions`
    GROUP BY UNIQUE_ID
),

-- for each customer, find which months they were active
monthly_activity AS (
    SELECT DISTINCT
        t.UNIQUE_ID,
        DATE_TRUNC(t.EFF_DATE, MONTH) AS active_month
    FROM `__PROJECT__.staging.stg_transactions` t
),

-- join cohort to activity and calculate months since first txn
cohort_activity AS (
    SELECT
        f.cohort_month,
        m.active_month,
        DATE_DIFF(m.active_month, f.cohort_month, MONTH) AS months_since_first,
        f.UNIQUE_ID
    FROM first_txn f
    JOIN monthly_activity m ON f.UNIQUE_ID = m.UNIQUE_ID
    WHERE m.active_month >= f.cohort_month
),

-- count customers per cohort
cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT UNIQUE_ID) AS cohort_size
    FROM first_txn
    GROUP BY cohort_month
),

-- count retained customers at each month interval
retention AS (
    SELECT
        ca.cohort_month,
        ca.months_since_first,
        COUNT(DISTINCT ca.UNIQUE_ID) AS active_customers
    FROM cohort_activity ca
    WHERE ca.months_since_first <= 12
    GROUP BY ca.cohort_month, ca.months_since_first
)

SELECT
    r.cohort_month,
    cs.cohort_size,
    r.months_since_first,
    r.active_customers,
    ROUND(r.active_customers * 100.0 / cs.cohort_size, 1) AS retention_pct
FROM retention r
JOIN cohort_sizes cs ON r.cohort_month = cs.cohort_month
ORDER BY r.cohort_month, r.months_since_first;
