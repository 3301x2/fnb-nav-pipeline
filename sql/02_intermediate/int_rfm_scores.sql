-- int_rfm_scores.sql
-- quintile scoring (1-5) on RFM features, 5 = best
-- recency is reversed so fewer days since last txn = higher score
-- source: analytics.int_rfm_features -> analytics.int_rfm_scores

CREATE OR REPLACE TABLE `__PROJECT__.analytics.int_rfm_scores`
CLUSTER BY UNIQUE_ID
AS

SELECT
    *,

    -- recency (reversed: fewer days = higher score)
    6 - NTILE(5) OVER (ORDER BY lst_trns_days ASC)                AS r_score,

    -- frequency
    NTILE(5) OVER (ORDER BY nr_trns ASC)                           AS f_score,

    -- monetary
    NTILE(5) OVER (ORDER BY val_trns ASC)                          AS m_score,

    -- combined RFM string eg "555" = best customer
    CONCAT(
        CAST(6 - NTILE(5) OVER (ORDER BY lst_trns_days ASC) AS STRING),
        CAST(NTILE(5) OVER (ORDER BY nr_trns ASC) AS STRING),
        CAST(NTILE(5) OVER (ORDER BY val_trns ASC) AS STRING)
    )                                                              AS rfm_combined

FROM `__PROJECT__.analytics.int_rfm_features`;
