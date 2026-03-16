-- train_model.sql
-- k-means clustering on 9 features, k=5 (validated with elbow method)
-- standardize_features=TRUE so no single feture dominates
-- source: analytics.int_rfm_scores -> analytics.kmeans_customer_segments

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
