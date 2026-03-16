-- stg_transactions.sql
-- joins transaction_data with all lookup tables for human readable names
-- partitioned by month + clustered by category/destination for cost efficency
-- source: customer_spend.transaction_data -> staging.stg_transactions

CREATE OR REPLACE TABLE `__PROJECT__.staging.stg_transactions`
PARTITION BY DATE_TRUNC(EFF_DATE, MONTH)
CLUSTER BY CATEGORY_TWO, DESTINATION
AS

SELECT
    t.UNIQUE_ID,
    t.EFF_DATE,
    t.trns_amt,
    t.trns_time,

    -- time dimensions extracted upfront
    EXTRACT(YEAR FROM t.EFF_DATE)                                  AS trns_year,
    EXTRACT(MONTH FROM t.EFF_DATE)                                 AS trns_month,
    EXTRACT(DAYOFWEEK FROM t.EFF_DATE)                             AS trns_dow,
    SAFE_CAST(LEFT(t.trns_time, 2) AS INT64)                      AS trns_hour,

    -- category hierarchy (IDs resolved to names)
    t.CATEGORY_ONE_ID,
    c1.CATEGORY_ONE,
    t.CATEGORY_TWO_ID,
    c2.CATEGORY_TWO,
    t.NAV_CATEGORY_ID,
    nc.NAV_CATEGORY,

    -- destination (the actual merchant)
    t.DESTINATION_ID,
    d.DESTINATION,
    d.BUSINESS_CATEGORY_ONE,
    d.BUSINESS_CATEGORY_TWO,

    -- location + geography
    CAST(t.LOCATION_ID AS INT64)                                   AS LOCATION_ID,
    l.LOCATION_NAME,
    l.X                                                            AS longitude,
    l.Y                                                            AS latitude,
    CAST(t.SUBURB_ID AS INT64)                                     AS SUBURB_ID,
    s.SUBURB,
    s.TOWN,
    s.MUNICIPALITY,
    s.PROVINCE

FROM `__PROJECT__.customer_spend.transaction_data` t

LEFT JOIN `__PROJECT__.spend_lookups.category_one_id` c1
    ON t.CATEGORY_ONE_ID = c1.CATEGORY_ONE_ID

LEFT JOIN `__PROJECT__.spend_lookups.category_two_id` c2
    ON t.CATEGORY_TWO_ID = c2.CATEGORY_TWO_ID

LEFT JOIN `__PROJECT__.spend_lookups.nav_category_id` nc
    ON t.NAV_CATEGORY_ID = nc.NAV_CATEGORY_ID

LEFT JOIN `__PROJECT__.spend_lookups.destination_id` d
    ON t.DESTINATION_ID = d.DESTINATION_ID

LEFT JOIN `__PROJECT__.spend_lookups.location_id` l
    ON CAST(t.LOCATION_ID AS INT64) = l.LOCATION_ID

LEFT JOIN `__PROJECT__.spend_lookups.suburb_id` s
    ON CAST(t.SUBURB_ID AS INT64) = s.SUBURB_ID

WHERE t.UNIQUE_ID IS NOT NULL
  AND t.EFF_DATE IS NOT NULL
  AND t.trns_amt IS NOT NULL;
