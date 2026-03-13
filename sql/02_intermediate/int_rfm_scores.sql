-- ════════════════════════════════════════════════════════════════
-- int_rfm_scores.sql
-- ════════════════════════════════════════════════════════════════
-- Applies quintile scoring (1-5) to RFM features. Score 5 = best.
-- For recency, scoring is REVERSED: fewer days since last txn = 5.
--
-- This table feeds directly into the k-means model. BigQuery ML's
-- standardize_features=TRUE handles normalization for clustering,
-- but the quintile scores are also useful for RFM segment labels
-- and for the dashboard.
--
-- Source: analytics.int_rfm_features
-- Target: analytics.int_rfm_scores
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE `fmn-sandbox.analytics.int_rfm_scores`
CLUSTER BY UNIQUE_ID
AS

SELECT
    *,

    -- Recency score (REVERSED: lower days = higher score)
    6 - NTILE(5) OVER (ORDER BY lst_trns_days ASC)                AS r_score,

    -- Frequency score
    NTILE(5) OVER (ORDER BY nr_trns ASC)                           AS f_score,

    -- Monetary score
    NTILE(5) OVER (ORDER BY val_trns ASC)                          AS m_score,

    -- Combined RFM string (e.g. "555" = best customer)
    CONCAT(
        CAST(6 - NTILE(5) OVER (ORDER BY lst_trns_days ASC) AS STRING),
        CAST(NTILE(5) OVER (ORDER BY nr_trns ASC) AS STRING),
        CAST(NTILE(5) OVER (ORDER BY val_trns ASC) AS STRING)
    )                                                              AS rfm_combined

FROM `fmn-sandbox.analytics.int_rfm_features`;
