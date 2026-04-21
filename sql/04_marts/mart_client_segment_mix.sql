-- mart_client_segment_mix.sql
-- Per-client × category segment distribution.
-- Option A: same global K-means segment definitions, but each client sees
-- the mix relative to THEIR OWN customer base (not the FNB-wide mix).
--
-- Why this exists: mart_cluster_output has one row per customer with a single
-- global "Champion / Loyal / ... / Dormant" label. When two reports (e.g. Clicks
-- and PNP) both summarise that table without a destination filter, the numbers
-- come out identical — because the underlying population is identical.
-- This mart restricts the population to each client's shoppers.
--
-- Grain: DESTINATION × CATEGORY_TWO × segment_name
-- Source: int_customer_category_spend (who shopped where) + mart_cluster_output (segment label)
-- Consumed by: generate_clicks_brands.py, generate_cipla_pitch.py, generate_report_v3.py,
--              dashboards/app.py Customer Segments page, Looker views.

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_client_segment_mix`
CLUSTER BY DESTINATION, CATEGORY_TWO
AS

WITH client_customers AS (
    -- unique customer × destination × category touchpoints (one row per combo)
    SELECT DISTINCT
        cs.DESTINATION,
        cs.CATEGORY_TWO,
        cs.UNIQUE_ID,
        cs.dest_spend
    FROM `__PROJECT__.analytics.int_customer_category_spend` cs
),

labeled AS (
    SELECT
        cc.DESTINATION,
        cc.CATEGORY_TWO,
        cc.UNIQUE_ID,
        cc.dest_spend,
        co.segment_name,
        co.cluster_id
    FROM client_customers cc
    LEFT JOIN `__PROJECT__.marts.mart_cluster_output` co
        ON cc.UNIQUE_ID = co.UNIQUE_ID
),

client_totals AS (
    SELECT
        DESTINATION,
        CATEGORY_TWO,
        COUNT(DISTINCT UNIQUE_ID)  AS client_total_customers,
        SUM(dest_spend)            AS client_total_spend
    FROM labeled
    GROUP BY DESTINATION, CATEGORY_TWO
),

fnb_totals AS (
    -- FNB-wide (global) segment mix — used for the index / over-representation
    SELECT
        segment_name,
        COUNT(DISTINCT UNIQUE_ID) AS fnb_customers,
        COUNT(DISTINCT UNIQUE_ID) * 1.0 /
            NULLIF(SUM(COUNT(DISTINCT UNIQUE_ID)) OVER (), 0) AS fnb_pct
    FROM `__PROJECT__.marts.mart_cluster_output`
    GROUP BY segment_name
),

per_client_segment AS (
    SELECT
        l.DESTINATION,
        l.CATEGORY_TWO,
        l.segment_name,
        COUNT(DISTINCT l.UNIQUE_ID) AS segment_customers,
        ROUND(SUM(l.dest_spend), 0) AS segment_spend
    FROM labeled l
    WHERE l.segment_name IS NOT NULL
    GROUP BY l.DESTINATION, l.CATEGORY_TWO, l.segment_name
)

SELECT
    p.DESTINATION,
    p.CATEGORY_TWO,
    p.segment_name,

    p.segment_customers,
    p.segment_spend,

    t.client_total_customers,
    t.client_total_spend,

    -- share of THIS client's customers in this segment
    ROUND(p.segment_customers * 100.0 / NULLIF(t.client_total_customers, 0), 2)
                                               AS pct_of_client_customers,

    -- share of THIS client's spend from this segment
    ROUND(p.segment_spend * 100.0 / NULLIF(t.client_total_spend, 0), 2)
                                               AS pct_of_client_spend,

    -- FNB-wide share of this segment (for reference)
    ROUND(f.fnb_pct * 100, 2)                  AS fnb_pct_of_customers,

    -- index: 100 = same as FNB-wide, >100 = over-indexed for this client
    ROUND(
        (p.segment_customers * 1.0 / NULLIF(t.client_total_customers, 0))
        / NULLIF(f.fnb_pct, 0) * 100, 0
    )                                          AS index_vs_fnb

FROM per_client_segment p
JOIN client_totals  t ON p.DESTINATION = t.DESTINATION
                      AND p.CATEGORY_TWO = t.CATEGORY_TWO
LEFT JOIN fnb_totals f ON p.segment_name = f.segment_name
WHERE t.client_total_customers >= 1000;  -- skip noisy low-volume clients
-- No ORDER BY: BigQuery disallows ORDER BY with CLUSTER BY in CREATE TABLE AS.
-- Downstream queries / the Looker view sort as needed.
