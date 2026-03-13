-- ════════════════════════════════════════════════════════════════
-- stg_transactions.sql
-- ════════════════════════════════════════════════════════════════
-- Joins raw transaction_data with all lookup tables to produce
-- human-readable names. Strips PII (no email/phone). Partitioned
-- by month for cost efficiency on the 2.2B row production table.
--
-- Source:  customer_spend.transaction_data
-- Lookups: spend_lookups.category_one_id, category_two_id,
--          destination_id, location_id, suburb_id, nav_category_id
-- Target:  staging.stg_transactions
--
-- Partitioning: EFF_DATE (monthly) — queries filtering by date
--   only scan relevant partitions, not the full table.
-- Clustering: CATEGORY_TWO, DESTINATION — the two most common
--   filter columns in downstream queries.
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.staging.stg_transactions`
PARTITION BY DATE_TRUNC(EFF_DATE, MONTH)
CLUSTER BY CATEGORY_TWO, DESTINATION
AS

SELECT
    t.UNIQUE_ID,
    t.EFF_DATE,
    t.trns_amt,
    t.trns_time,

    -- Time dimensions (pre-extracted for downstream use)
    EXTRACT(YEAR FROM t.EFF_DATE)                                  AS trns_year,
    EXTRACT(MONTH FROM t.EFF_DATE)                                 AS trns_month,
    EXTRACT(DAYOFWEEK FROM t.EFF_DATE)                             AS trns_dow,
    SAFE_CAST(LEFT(t.trns_time, 2) AS INT64)                      AS trns_hour,

    -- Category hierarchy (resolved from IDs → names)
    t.CATEGORY_ONE_ID,
    c1.CATEGORY_ONE,
    t.CATEGORY_TWO_ID,
    c2.CATEGORY_TWO,
    t.NAV_CATEGORY_ID,
    nc.NAV_CATEGORY,

    -- Destination (the actual shop / merchant)
    t.DESTINATION_ID,
    d.DESTINATION,
    d.BUSINESS_CATEGORY_ONE,
    d.BUSINESS_CATEGORY_TWO,

    -- Location and geography
    CAST(t.LOCATION_ID AS INT64)                                   AS LOCATION_ID,
    l.LOCATION_NAME,
    l.X                                                            AS longitude,
    l.Y                                                            AS latitude,
    CAST(t.SUBURB_ID AS INT64)                                     AS SUBURB_ID,
    s.SUBURB,
    s.TOWN,
    s.MUNICIPALITY,
    s.PROVINCE

FROM `fmn-sandbox.customer_spend.transaction_data` t

LEFT JOIN `fmn-sandbox.spend_lookups.category_one_id` c1
    ON t.CATEGORY_ONE_ID = c1.CATEGORY_ONE_ID

LEFT JOIN `fmn-sandbox.spend_lookups.category_two_id` c2
    ON t.CATEGORY_TWO_ID = c2.CATEGORY_TWO_ID

LEFT JOIN `fmn-sandbox.spend_lookups.nav_category_id` nc
    ON t.NAV_CATEGORY_ID = nc.NAV_CATEGORY_ID

LEFT JOIN `fmn-sandbox.spend_lookups.destination_id` d
    ON t.DESTINATION_ID = d.DESTINATION_ID

LEFT JOIN `fmn-sandbox.spend_lookups.location_id` l
    ON CAST(t.LOCATION_ID AS INT64) = l.LOCATION_ID

LEFT JOIN `fmn-sandbox.spend_lookups.suburb_id` s
    ON CAST(t.SUBURB_ID AS INT64) = s.SUBURB_ID

WHERE t.UNIQUE_ID IS NOT NULL
  AND t.EFF_DATE IS NOT NULL
  AND t.trns_amt IS NOT NULL;
