-- ════════════════════════════════════════════════════════════════
-- mart_behavioral_summary.sql
-- Shopping patterns per segment: when they shop, how diverse.
-- Expected: 5 rows
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.marts.mart_behavioral_summary` AS

WITH txn_time AS (
    SELECT
        t.UNIQUE_ID,
        CASE
            WHEN t.trns_hour BETWEEN 6  AND 10 THEN 'Morning'
            WHEN t.trns_hour BETWEEN 11 AND 16 THEN 'Afternoon'
            WHEN t.trns_hour BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Late Night'
        END                                                    AS time_slot
    FROM `fmn-sandbox.staging.stg_transactions` t
),

customer_patterns AS (
    SELECT
        UNIQUE_ID,
        COUNTIF(time_slot = 'Morning')                         AS morning_txns,
        COUNTIF(time_slot = 'Afternoon')                       AS afternoon_txns,
        COUNTIF(time_slot = 'Evening')                         AS evening_txns,
        COUNTIF(time_slot = 'Late Night')                      AS late_night_txns,
        COUNT(*)                                               AS total_txns
    FROM txn_time
    GROUP BY UNIQUE_ID
)

SELECT
    co.segment_name,
    COUNT(*)                                                   AS customers,

    -- Time of day distribution
    ROUND(SUM(cp.morning_txns) * 100.0 /
        NULLIF(SUM(cp.total_txns), 0), 1)                     AS pct_morning,
    ROUND(SUM(cp.afternoon_txns) * 100.0 /
        NULLIF(SUM(cp.total_txns), 0), 1)                     AS pct_afternoon,
    ROUND(SUM(cp.evening_txns) * 100.0 /
        NULLIF(SUM(cp.total_txns), 0), 1)                     AS pct_evening,
    ROUND(SUM(cp.late_night_txns) * 100.0 /
        NULLIF(SUM(cp.total_txns), 0), 1)                     AS pct_late_night,

    -- Weekend ratio
    ROUND(SUM(co.NR_TRNS_WEEKEND) * 100.0 /
        NULLIF(SUM(co.NR_TRNS_WEEKEND + co.NR_TRNS_WEEK), 0), 1)
                                                               AS pct_weekend,

    -- Diversity
    ROUND(AVG(co.active_nav_categories), 1)                    AS avg_categories,
    ROUND(AVG(co.active_destinations), 1)                      AS avg_merchants,
    ROUND(AVG(co.avg_val), 2)                                  AS avg_txn_value,
    ROUND(SUM(cp.total_txns) * 1.0 / COUNT(*), 1)             AS avg_txns_per_customer

FROM `fmn-sandbox.marts.mart_cluster_output` co
JOIN customer_patterns cp ON co.UNIQUE_ID = cp.UNIQUE_ID
GROUP BY co.segment_name
ORDER BY avg_txns_per_customer DESC;
