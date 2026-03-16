-- int_destination_metrics.sql
-- aggregates KPIs per destination within each category
-- powers the benchmarks page on the dashboard
-- source: analytics.int_customer_category_spend -> analytics.int_destination_metrics

CREATE OR REPLACE TABLE `fmn-sandbox.analytics.int_destination_metrics`
CLUSTER BY CATEGORY_TWO, DESTINATION
AS

WITH destination_agg AS (
    SELECT
        CATEGORY_TWO,
        DESTINATION,
        COUNT(DISTINCT UNIQUE_ID)                              AS customers,
        SUM(dest_txn_count)                                    AS transactions,
        ROUND(SUM(dest_spend), 0)                              AS total_spend,
        ROUND(AVG(dest_spend / NULLIF(dest_txn_count, 0)), 2)  AS avg_txn_value,
        ROUND(SUM(dest_spend) / NULLIF(COUNT(DISTINCT UNIQUE_ID), 0), 0)
                                                               AS spend_per_customer,
        ROUND(AVG(share_of_wallet_pct), 1)                     AS avg_share_of_wallet
    FROM `fmn-sandbox.analytics.int_customer_category_spend`
    GROUP BY CATEGORY_TWO, DESTINATION
),

category_totals AS (
    SELECT
        CATEGORY_TWO,
        SUM(total_spend)    AS cat_total_spend,
        SUM(customers)      AS cat_total_customers
    FROM destination_agg
    GROUP BY CATEGORY_TWO
)

SELECT
    d.*,

    -- market share within category
    ROUND(d.total_spend * 100.0 / NULLIF(ct.cat_total_spend, 0), 2)
                                                               AS market_share_pct,

    -- customer penetration within category
    ROUND(d.customers * 100.0 / NULLIF(ct.cat_total_customers, 0), 2)
                                                               AS penetration_pct,

    -- rank within category by spend
    ROW_NUMBER() OVER (
        PARTITION BY d.CATEGORY_TWO
        ORDER BY d.total_spend DESC
    )                                                          AS spend_rank

FROM destination_agg d
JOIN category_totals ct ON d.CATEGORY_TWO = ct.CATEGORY_TWO;
