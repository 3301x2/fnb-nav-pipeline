-- mart_geo_summary.sql
-- spend by province x municipality with category breakdown for geo views

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_geo_summary`
CLUSTER BY PROVINCE, CATEGORY_TWO
AS

SELECT
    t.PROVINCE,
    t.MUNICIPALITY,
    t.CATEGORY_TWO,
    COUNT(DISTINCT t.UNIQUE_ID)                                AS customers,
    COUNT(*)                                                   AS transactions,
    ROUND(SUM(t.trns_amt), 0)                                  AS total_spend,
    ROUND(AVG(t.trns_amt), 2)                                  AS avg_txn,
    COUNT(DISTINCT t.DESTINATION)                               AS distinct_merchants

FROM `__PROJECT__.staging.stg_transactions` t
WHERE t.PROVINCE IS NOT NULL
  AND t.CATEGORY_TWO IS NOT NULL
GROUP BY t.PROVINCE, t.MUNICIPALITY, t.CATEGORY_TWO;
