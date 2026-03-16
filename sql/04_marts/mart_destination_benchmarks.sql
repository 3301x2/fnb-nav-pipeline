-- mart_destination_benchmarks.sql
-- every destination's KPIs within its category
-- dashboard picks a client, shows them by name, anonymises the rest as competitors

CREATE OR REPLACE TABLE `fmn-sandbox.marts.mart_destination_benchmarks`
CLUSTER BY CATEGORY_TWO
AS

SELECT
    CATEGORY_TWO,
    DESTINATION,
    customers,
    transactions,
    total_spend,
    avg_txn_value,
    spend_per_customer,
    avg_share_of_wallet,
    market_share_pct,
    penetration_pct,
    spend_rank

FROM `fmn-sandbox.analytics.int_destination_metrics`
WHERE total_spend > 0;
