-- ════════════════════════════════════════════════════════════════
-- mart_destination_benchmarks.sql
-- Every destination's KPIs within its category. The dashboard
-- picks a client → shows them by name, anonymizes the rest as
-- "Competitor 1", "Competitor 2", etc.
--
-- This is the table that solves the meeting ask:
--   "This week we meet with Adidas, next week with Nike.
--    Instead of recoding everything..."
--
-- Now: just pick from a dropdown. SQL never changes.
-- ════════════════════════════════════════════════════════════════

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
