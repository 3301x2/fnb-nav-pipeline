-- predict_and_name.sql
-- assigns every customer to a cluster then ranks by avg spend
-- names them Champions -> Dormant, joins demographics too
-- source: kmeans model + int_rfm_scores + stg_customers -> marts.mart_cluster_output

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_cluster_output`
CLUSTER BY segment_name
AS

WITH predictions AS (
    SELECT *
    FROM ML.PREDICT(
        MODEL `__PROJECT__.analytics.kmeans_customer_segments`,
        (SELECT * FROM `__PROJECT__.analytics.int_rfm_scores`)
    )
),

centroid_ranks AS (
    SELECT
        CENTROID_ID,
        AVG(val_trns)                                          AS avg_spend,
        ROW_NUMBER() OVER (ORDER BY AVG(val_trns) DESC)       AS spend_rank
    FROM predictions
    GROUP BY CENTROID_ID
),

named AS (
    SELECT
        p.*,
        cr.spend_rank                                          AS cluster_id,
        CASE cr.spend_rank
            WHEN 1 THEN 'Champions'
            WHEN 2 THEN 'Loyal High Value'
            WHEN 3 THEN 'Steady Mid-Tier'
            WHEN 4 THEN 'At Risk'
            WHEN 5 THEN 'Dormant'
        END                                                    AS segment_name
    FROM predictions p
    JOIN centroid_ranks cr ON p.CENTROID_ID = cr.CENTROID_ID
)

SELECT
    n.UNIQUE_ID,
    n.cluster_id,
    n.segment_name,

    -- rfm features
    n.nr_trns,
    n.val_trns,
    n.avg_val,
    n.median_val,
    n.std_val,
    n.lst_trns_days,
    n.active_months,
    n.mnthly_avg_nr,
    n.mnthly_avg_val,
    n.days_between,

    -- temporal
    n.avg_dow,
    n.avg_hour,
    n.NR_TRNS_MORNING,
    n.NR_TRNS_MIDDAY,
    n.NR_TRNS_EVENING,
    n.NR_TRNS_LATE,
    n.NR_TRNS_WEEKEND,
    n.NR_TRNS_WEEK,

    -- diversity
    n.active_nav_categories,
    n.active_destinations,
    n.active_suburbs,
    n.active_locations,

    -- rfm scores
    n.r_score,
    n.f_score,
    n.m_score,
    n.rfm_combined,

    -- demographics from staging
    c.gender,
    c.age,
    c.profile_age,
    c.income_segment,
    c.hyper_segment,
    c.estimated_income,
    c.main_banked,
    c.credit_risk_class,
    c.age_group,
    c.income_group,
    c.gender_label

FROM named n
LEFT JOIN `__PROJECT__.staging.stg_customers` c
    ON n.UNIQUE_ID = c.UNIQUE_ID;
