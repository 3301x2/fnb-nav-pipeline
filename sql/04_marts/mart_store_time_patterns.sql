-- mart_store_time_patterns.sql
-- When do customers shop at each store? Time of day, day of week, month.
-- Reads from: stg_transactions (uses partition on month, filters last 12m)

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_store_time_patterns` AS

SELECT
    CATEGORY_TWO,
    DESTINATION,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT UNIQUE_ID) AS unique_customers,
    ROUND(SUM(trns_amt), 0) AS total_spend,

    -- Time of day percentages
    ROUND(COUNTIF(trns_hour BETWEEN 6 AND 9) * 100.0 / COUNT(*), 1) AS pct_early_morning,
    ROUND(COUNTIF(trns_hour BETWEEN 10 AND 12) * 100.0 / COUNT(*), 1) AS pct_mid_morning,
    ROUND(COUNTIF(trns_hour BETWEEN 13 AND 15) * 100.0 / COUNT(*), 1) AS pct_afternoon,
    ROUND(COUNTIF(trns_hour BETWEEN 16 AND 18) * 100.0 / COUNT(*), 1) AS pct_late_afternoon,
    ROUND(COUNTIF(trns_hour BETWEEN 19 AND 21) * 100.0 / COUNT(*), 1) AS pct_evening,
    ROUND(COUNTIF(trns_hour >= 22 OR trns_hour < 6) * 100.0 / COUNT(*), 1) AS pct_late_night,

    -- Day of week percentages
    ROUND(COUNTIF(trns_dow = 1) * 100.0 / COUNT(*), 1) AS pct_sunday,
    ROUND(COUNTIF(trns_dow = 2) * 100.0 / COUNT(*), 1) AS pct_monday,
    ROUND(COUNTIF(trns_dow = 3) * 100.0 / COUNT(*), 1) AS pct_tuesday,
    ROUND(COUNTIF(trns_dow = 4) * 100.0 / COUNT(*), 1) AS pct_wednesday,
    ROUND(COUNTIF(trns_dow = 5) * 100.0 / COUNT(*), 1) AS pct_thursday,
    ROUND(COUNTIF(trns_dow = 6) * 100.0 / COUNT(*), 1) AS pct_friday,
    ROUND(COUNTIF(trns_dow = 7) * 100.0 / COUNT(*), 1) AS pct_saturday,

    -- Weekend vs weekday
    ROUND(COUNTIF(trns_dow IN (1, 7)) * 100.0 / COUNT(*), 1) AS pct_weekend,
    ROUND(COUNTIF(trns_dow BETWEEN 2 AND 6) * 100.0 / COUNT(*), 1) AS pct_weekday,

    -- Avg basket by time slot
    ROUND(AVG(IF(trns_hour BETWEEN 6 AND 9, trns_amt, NULL)), 2) AS avg_basket_early,
    ROUND(AVG(IF(trns_hour BETWEEN 10 AND 14, trns_amt, NULL)), 2) AS avg_basket_midday,
    ROUND(AVG(IF(trns_hour BETWEEN 15 AND 18, trns_amt, NULL)), 2) AS avg_basket_afternoon,
    ROUND(AVG(IF(trns_hour BETWEEN 19 AND 23, trns_amt, NULL)), 2) AS avg_basket_evening,

    -- Peak hour and peak day
    APPROX_TOP_COUNT(trns_hour, 1)[OFFSET(0)].value AS peak_hour,
    APPROX_TOP_COUNT(trns_dow, 1)[OFFSET(0)].value AS peak_day_number

FROM `__PROJECT__.staging.stg_transactions`
WHERE EFF_DATE >= DATE_SUB(
    (SELECT MAX(EFF_DATE) FROM `__PROJECT__.staging.stg_transactions`),
    INTERVAL 12 MONTH
)
GROUP BY CATEGORY_TWO, DESTINATION
HAVING COUNT(*) >= 1000;
