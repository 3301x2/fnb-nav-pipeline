-- ════════════════════════════════════════════════════════════════
-- int_rfm_features.sql
-- ════════════════════════════════════════════════════════════════
-- Builds RFM (Recency, Frequency, Monetary) features plus
-- extended behavioral metrics for every customer with 2+
-- transactions. This feeds the clustering model.
--
-- Features:
--   Monetary:  val_trns, avg_val, median_val, std_val
--   Frequency: nr_trns, active_months, mnthly_avg_nr, mnthly_avg_val
--   Recency:   lst_trns_days, days_between
--   Temporal:  avg_dow, avg_hour, NR_TRNS_MORNING/MIDDAY/EVENING/LATE
--   Behavioral: NR_TRNS_WEEKEND, NR_TRNS_WEEK
--   Diversity: active_nav_categories, active_destinations,
--              active_suburbs, active_locations
--
-- Source: staging.stg_transactions
-- Target: analytics.int_rfm_features
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.analytics.int_rfm_features`
CLUSTER BY UNIQUE_ID
AS

WITH date_bounds AS (
    SELECT
        MAX(EFF_DATE)                            AS max_date,
        DATE_SUB(MAX(EFF_DATE), INTERVAL 12 MONTH) AS start_date
    FROM `fmn-sandbox.staging.stg_transactions`
),

filtered AS (
    SELECT t.*
    FROM `fmn-sandbox.staging.stg_transactions` t
    CROSS JOIN date_bounds d
    WHERE t.EFF_DATE >= d.start_date
)

SELECT
    f.UNIQUE_ID,

    -- Monetary
    COUNT(*)                                                       AS nr_trns,
    ROUND(SUM(f.trns_amt), 2)                                     AS val_trns,
    ROUND(AVG(f.trns_amt), 2)                                     AS avg_val,
    ROUND(APPROX_QUANTILES(f.trns_amt, 2)[OFFSET(1)], 2)         AS median_val,
    ROUND(STDDEV(f.trns_amt), 2)                                  AS std_val,

    -- Recency
    DATE_DIFF(
        (SELECT max_date FROM date_bounds),
        MAX(f.EFF_DATE), DAY
    )                                                              AS lst_trns_days,

    -- Frequency
    COUNT(DISTINCT FORMAT_DATE('%Y-%m', f.EFF_DATE))               AS active_months,
    ROUND(COUNT(*) * 1.0 /
        NULLIF(COUNT(DISTINCT FORMAT_DATE('%Y-%m', f.EFF_DATE)), 0), 2)
                                                                   AS mnthly_avg_nr,
    ROUND(SUM(f.trns_amt) /
        NULLIF(COUNT(DISTINCT FORMAT_DATE('%Y-%m', f.EFF_DATE)), 0), 2)
                                                                   AS mnthly_avg_val,

    -- Inter-purchase interval
    ROUND(SAFE_DIVIDE(
        DATE_DIFF(MAX(f.EFF_DATE), MIN(f.EFF_DATE), DAY),
        NULLIF(COUNT(*) - 1, 0)
    ), 2)                                                          AS days_between,

    -- Temporal patterns
    ROUND(AVG(f.trns_dow), 2)                                     AS avg_dow,
    ROUND(AVG(f.trns_hour), 2)                                    AS avg_hour,
    COUNTIF(f.trns_hour BETWEEN 6  AND 10)                         AS NR_TRNS_MORNING,
    COUNTIF(f.trns_hour BETWEEN 11 AND 16)                         AS NR_TRNS_MIDDAY,
    COUNTIF(f.trns_hour BETWEEN 17 AND 21)                         AS NR_TRNS_EVENING,
    COUNTIF(f.trns_hour < 6 OR f.trns_hour > 21)                  AS NR_TRNS_LATE,
    COUNTIF(f.trns_dow IN (1, 7))                                  AS NR_TRNS_WEEKEND,
    COUNTIF(f.trns_dow NOT IN (1, 7))                              AS NR_TRNS_WEEK,

    -- Diversity
    COUNT(DISTINCT f.NAV_CATEGORY_ID)                              AS active_nav_categories,
    COUNT(DISTINCT f.DESTINATION_ID)                                AS active_destinations,
    COUNT(DISTINCT f.SUBURB_ID)                                     AS active_suburbs,
    COUNT(DISTINCT f.LOCATION_ID)                                   AS active_locations

FROM filtered f
GROUP BY f.UNIQUE_ID
HAVING COUNT(*) >= 2;
