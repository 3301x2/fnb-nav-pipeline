-- stg_customers.sql
-- deduplicates base_data to one row per customer (keeps most recent month)
-- renames demo_* fields to readable names, adds pre-binned age/income groups
-- source: customer_spend.base_data -> staging.stg_customers

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

    -- demographics renamed from demo_*
    demo_1                                                         AS gender,
    demo_2                                                         AS age,
    demo_3                                                         AS profile_age,
    demo_4                                                         AS vertical_sales_index,
    demo_5                                                         AS income_segment,
    demo_6                                                         AS hyper_segment,
    demo_7                                                         AS estimated_income,
    demo_8                                                         AS main_banked,
    demo_9                                                         AS credit_risk_class,

    -- binned groups for the dashboard filters
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
