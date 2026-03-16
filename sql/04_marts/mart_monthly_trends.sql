-- mart_monthly_trends.sql
-- monthly spend per category x destination, dashboard shows client trend vs category

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_monthly_trends`
PARTITION BY month
CLUSTER BY CATEGORY_TWO, DESTINATION
AS

SELECT
    DATE_TRUNC(t.EFF_DATE, MONTH)                              AS month,
    t.CATEGORY_TWO,
    t.DESTINATION,
    COUNT(*)                                                   AS txn_count,
    COUNT(DISTINCT t.UNIQUE_ID)                                AS customer_count,
    ROUND(SUM(t.trns_amt), 0)                                  AS total_spend,
    ROUND(AVG(t.trns_amt), 2)                                  AS avg_txn

FROM `__PROJECT__.staging.stg_transactions` t
WHERE t.CATEGORY_TWO IS NOT NULL
  AND t.DESTINATION IS NOT NULL
GROUP BY month, t.CATEGORY_TWO, t.DESTINATION;
