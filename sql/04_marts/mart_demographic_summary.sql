-- mart_demographic_summary.sql
-- demographics of customers within each category, dashboard filters by category/client

CREATE OR REPLACE TABLE `fmn-sandbox.marts.mart_demographic_summary`
CLUSTER BY CATEGORY_TWO
AS

SELECT
    cs.CATEGORY_TWO,
    c.gender_label,
    c.age_group,
    c.income_group,
    c.income_segment,
    c.credit_risk_class,
    COUNT(DISTINCT cs.UNIQUE_ID)                               AS customers,
    SUM(cs.category_txn_count)                                 AS total_txns,
    ROUND(SUM(cs.category_spend), 0)                           AS total_spend,
    ROUND(AVG(cs.category_spend), 0)                           AS avg_spend_per_customer

FROM (
    -- dedup to one row per customer per category
    SELECT UNIQUE_ID, CATEGORY_TWO,
           SUM(dest_txn_count)    AS category_txn_count,
           ROUND(SUM(dest_spend), 2) AS category_spend
    FROM `fmn-sandbox.analytics.int_customer_category_spend`
    GROUP BY UNIQUE_ID, CATEGORY_TWO
) cs

LEFT JOIN `fmn-sandbox.staging.stg_customers` c
    ON cs.UNIQUE_ID = c.UNIQUE_ID

GROUP BY cs.CATEGORY_TWO, c.gender_label, c.age_group,
         c.income_group, c.income_segment, c.credit_risk_class;
