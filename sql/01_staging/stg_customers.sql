-- ════════════════════════════════════════════════════════════════
-- stg_customers.sql
-- ════════════════════════════════════════════════════════════════
-- Deduplicates base_data (one row per customer, most recent month).
-- Renames all demo_* fields to human-readable names.
-- Creates pre-binned age_group and income_group for dashboards.
--
-- Source: customer_spend.base_data (82M+ rows, one per customer per month)
-- Target: staging.stg_customers (one row per customer)
--
-- Field mapping (from data dictionary):
--   demo_1 → gender        (0=Male, 1=Female)
--   demo_2 → age           (years)
--   demo_3 → profile_age   (years the profile has existed)
--   demo_4 → vertical_sales_index
--   demo_5 → income_segment (EW0, EB0, GLD, etc.)
--   demo_6 → hyper_segment  (Salaried, Self-employed, etc.)
--   demo_7 → estimated_income (smoothed credit turnover)
--   demo_8 → main_banked   (0/1 based on FNB rules)
--   demo_9 → credit_risk_class (PRISM score categories)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.staging.stg_customers`
CLUSTER BY income_segment, gender
AS

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY UNIQUE_ID
            ORDER BY month DESC
        ) AS rn
    FROM `fmn-sandbox.customer_spend.base_data`
    WHERE UNIQUE_ID IS NOT NULL
)

SELECT
    UNIQUE_ID,

    -- Demographics (renamed from demo_* to meaningful names)
    demo_1                                                         AS gender,
    demo_2                                                         AS age,
    demo_3                                                         AS profile_age,
    demo_4                                                         AS vertical_sales_index,
    demo_5                                                         AS income_segment,
    demo_6                                                         AS hyper_segment,
    demo_7                                                         AS estimated_income,
    demo_8                                                         AS main_banked,
    demo_9                                                         AS credit_risk_class,

    -- Pre-binned groups (for dashboard filters and charts)
    CASE
        WHEN demo_2 IS NULL THEN 'Unknown'
        WHEN demo_2 <= 25   THEN '18-25'
        WHEN demo_2 <= 35   THEN '26-35'
        WHEN demo_2 <= 45   THEN '36-45'
        WHEN demo_2 <= 60   THEN '46-60'
        ELSE '60+'
    END                                                            AS age_group,

    CASE
        WHEN demo_7 IS NULL OR demo_7 = 0 THEN 'Unknown'
        WHEN demo_7 <= 5500   THEN 'R0-R5.5k'
        WHEN demo_7 <= 13500  THEN 'R5.5k-R13.5k'
        WHEN demo_7 <= 23500  THEN 'R13.5k-R23.5k'
        WHEN demo_7 <= 32500  THEN 'R23.5k-R32.5k'
        WHEN demo_7 <= 56000  THEN 'R32.5k-R56k'
        ELSE 'R56k+'
    END                                                            AS income_group,

    CASE
        WHEN demo_1 = 0 THEN 'Male'
        WHEN demo_1 = 1 THEN 'Female'
        ELSE 'Unknown'
    END                                                            AS gender_label

FROM ranked
WHERE rn = 1;
