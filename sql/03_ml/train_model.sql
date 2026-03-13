-- ════════════════════════════════════════════════════════════════
-- train_model.sql
-- ════════════════════════════════════════════════════════════════
-- Trains a k-means clustering model on 9 customer features.
-- The model groups customers into 5 segments based on spending
-- behavior, frequency, recency, and shopping diversity.
--
-- Settings:
--   k=5: Business-friendly number of segments. Validated via
--         elbow method (see notebooks/03_cluster_profiling.ipynb).
--   standardize_features=TRUE: Normalizes all features to mean=0,
--         stddev=1 so no single feature dominates by scale.
--   max_iterations=20: Generous headroom; typically converges in 5-10.
--
-- The 9 features:
--   1. val_trns             → Total spend (monetary)
--   2. nr_trns              → Transaction count (frequency)
--   3. lst_trns_days        → Days since last purchase (recency)
--   4. avg_val              → Average transaction size
--   5. active_months        → Months with at least one transaction
--   6. active_destinations  → Distinct merchants visited
--   7. active_nav_categories → Distinct categories shopped
--   8. NR_TRNS_WEEKEND      → Weekend transaction count
--   9. NR_TRNS_WEEK         → Weekday transaction count
--
-- Source: analytics.int_rfm_scores
-- Target: analytics.kmeans_customer_segments (MODEL)
-- Runtime: ~60-120 seconds on sandbox, longer on production
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE MODEL `fmn-sandbox.analytics.kmeans_customer_segments`
OPTIONS (
    model_type   = 'KMEANS',
    num_clusters = 5,
    standardize_features = TRUE,
    max_iterations = 20
) AS

SELECT
    val_trns,
    nr_trns,
    lst_trns_days,
    avg_val,
    active_months,
    active_destinations,
    active_nav_categories,
    NR_TRNS_WEEKEND,
    NR_TRNS_WEEK
FROM `fmn-sandbox.analytics.int_rfm_scores`;
