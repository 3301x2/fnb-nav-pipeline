-- ════════════════════════════════════════════════════════════════
-- mart_cluster_profiles.sql
-- One row per segment with averages, ranges, and demographics.
-- Expected: 5 rows
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.marts.mart_cluster_profiles` AS

SELECT
    segment_name,
    cluster_id,
    COUNT(*)                                                       AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1)             AS pct_of_total,

    -- Monetary
    ROUND(AVG(val_trns), 2)                                        AS avg_total_spend,
    ROUND(MIN(val_trns), 2)                                        AS min_total_spend,
    ROUND(MAX(val_trns), 2)                                        AS max_total_spend,
    ROUND(SUM(val_trns), 0)                                        AS total_segment_spend,

    -- Frequency
    ROUND(AVG(nr_trns), 1)                                         AS avg_transactions,
    ROUND(AVG(mnthly_avg_nr), 1)                                   AS avg_monthly_txns,

    -- Recency
    ROUND(AVG(lst_trns_days), 1)                                   AS avg_recency_days,
    ROUND(AVG(days_between), 1)                                    AS avg_days_between_txns,

    -- Transaction value
    ROUND(AVG(avg_val), 2)                                         AS avg_txn_value,

    -- Diversity
    ROUND(AVG(active_destinations), 1)                             AS avg_merchants,
    ROUND(AVG(active_nav_categories), 1)                           AS avg_categories,
    ROUND(AVG(active_months), 1)                                   AS avg_active_months,

    -- Temporal
    ROUND(AVG(NR_TRNS_WEEKEND), 1)                                 AS avg_weekend_txns,
    ROUND(AVG(NR_TRNS_WEEK), 1)                                    AS avg_weekday_txns,

    -- Demographics
    ROUND(AVG(age), 1)                                             AS avg_age,
    ROUND(AVG(estimated_income), 0)                                AS avg_income,
    COUNTIF(age BETWEEN 18 AND 25)                                 AS age_18_25,
    COUNTIF(age BETWEEN 26 AND 35)                                 AS age_26_35,
    COUNTIF(age BETWEEN 36 AND 45)                                 AS age_36_45,
    COUNTIF(age BETWEEN 46 AND 60)                                 AS age_46_60,
    COUNTIF(age > 60)                                              AS age_over_60,
    COUNTIF(gender = 0)                                            AS male_count,
    COUNTIF(gender = 1)                                            AS female_count,
    APPROX_TOP_COUNT(age_group, 1)[OFFSET(0)].value                AS top_age_group,
    APPROX_TOP_COUNT(income_group, 1)[OFFSET(0)].value             AS top_income_group

FROM `fmn-sandbox.marts.mart_cluster_output`
GROUP BY segment_name, cluster_id
ORDER BY avg_total_spend DESC;
