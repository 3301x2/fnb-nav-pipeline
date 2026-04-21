-- mart_audience_client_overlap.sql
-- For each (DESTINATION, CATEGORY_TWO, audience), how many of that client's
-- customers are in that audience. Pre-computed here so Looker reads a small
-- aggregated table instead of re-running the heavy join on every dashboard
-- interaction. Grain: DESTINATION × CATEGORY_TWO × audience_id.
-- Built once per pipeline run (Step 4).

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_audience_client_overlap`
CLUSTER BY DESTINATION, CATEGORY_TWO
AS

WITH client_customers AS (
    SELECT DISTINCT DESTINATION, CATEGORY_TWO, UNIQUE_ID
    FROM `__PROJECT__.analytics.int_customer_category_spend`
),

client_totals AS (
    SELECT
        DESTINATION,
        CATEGORY_TWO,
        COUNT(DISTINCT UNIQUE_ID) AS client_total_customers
    FROM client_customers
    GROUP BY DESTINATION, CATEGORY_TWO
)

SELECT
    cc.DESTINATION,
    cc.CATEGORY_TWO,
    am.audience_id,
    ac.audience_name,
    ac.audience_type,
    COUNT(DISTINCT cc.UNIQUE_ID) AS overlap_customers,
    ct.client_total_customers,
    ROUND(
        COUNT(DISTINCT cc.UNIQUE_ID) * 100.0
        / NULLIF(ct.client_total_customers, 0),
        2
    ) AS pct_of_client
FROM client_customers cc
JOIN `__PROJECT__.marts.mart_audience_members` am
    ON cc.UNIQUE_ID = am.UNIQUE_ID
JOIN client_totals ct
    ON cc.DESTINATION = ct.DESTINATION
   AND cc.CATEGORY_TWO = ct.CATEGORY_TWO
LEFT JOIN `__PROJECT__.marts.mart_audience_catalog` ac
    ON am.audience_id = ac.audience_id
WHERE ct.client_total_customers >= 1000
GROUP BY
    cc.DESTINATION,
    cc.CATEGORY_TWO,
    am.audience_id,
    ac.audience_name,
    ac.audience_type,
    ct.client_total_customers;
