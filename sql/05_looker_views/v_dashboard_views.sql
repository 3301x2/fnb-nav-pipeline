-- v_dashboard_overview.sql
-- Flat view for the Overview page scorecards

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_dashboard_overview` AS
SELECT
    (SELECT COUNT(*) FROM `__PROJECT__.staging.stg_transactions`) AS total_transactions,
    (SELECT COUNT(*) FROM `__PROJECT__.staging.stg_customers`) AS total_customers,
    (SELECT COUNT(*) FROM `__PROJECT__.marts.mart_cluster_output`) AS segmented_customers,
    (SELECT COUNT(*) FROM `__PROJECT__.marts.mart_churn_risk`) AS churn_scored,
    (SELECT COUNT(DISTINCT DESTINATION) FROM `__PROJECT__.marts.mart_destination_benchmarks`) AS destinations;


-- v_dashboard_segments.sql
-- Segment data with behavioral and demographic detail for the Segments page

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_dashboard_segments` AS
SELECT
    co.segment_name,
    COUNT(*) AS customers,
    ROUND(AVG(co.val_trns), 0) AS avg_spend,
    ROUND(AVG(co.nr_trns), 0) AS avg_txns,
    ROUND(AVG(co.lst_trns_days), 0) AS avg_recency_days,
    ROUND(AVG(co.active_destinations), 1) AS avg_merchants,
    ROUND(AVG(co.active_months), 1) AS avg_active_months,
    ROUND(AVG(co.NR_TRNS_WEEKEND * 100.0
        / NULLIF(co.NR_TRNS_WEEKEND + co.NR_TRNS_WEEK, 0)), 1) AS pct_weekend,
    ROUND(AVG(c.age), 1) AS avg_age,
    ROUND(AVG(c.estimated_income), 0) AS avg_income,
    ROUND(COUNTIF(c.gender_label = 'Female') * 100.0 / NULLIF(COUNT(*), 0), 1) AS pct_female,
    APPROX_TOP_COUNT(c.age_group, 1)[OFFSET(0)].value AS top_age_group,
    APPROX_TOP_COUNT(c.income_group, 1)[OFFSET(0)].value AS top_income_group,
    ANY_VALUE(cp.business_description) AS business_description,
    ANY_VALUE(cp.recommended_action) AS recommended_action
FROM `__PROJECT__.marts.mart_cluster_output` co
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON co.UNIQUE_ID = c.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_cluster_summary` cp ON co.segment_name = cp.segment_name
GROUP BY co.segment_name;


-- v_dashboard_churn.sql
-- Churn + CLV combined for the Churn & CLV page

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_dashboard_churn` AS
SELECT
    cr.UNIQUE_ID,
    cr.churn_risk_level,
    cr.churn_probability,
    cr.total_spend,
    cr.days_since_last,
    co.segment_name,
    ce.reason_1,
    ce.reason_2,
    ce.reason_3,
    clv.clv_tier,
    clv.predicted_clv,
    clv.historical_spend,
    sm.momentum_status,
    sm.spend_change_pct,
    sm.urgency_score,
    c.age,
    c.age_group,
    c.gender_label,
    c.income_group
FROM `__PROJECT__.marts.mart_churn_risk` cr
LEFT JOIN `__PROJECT__.marts.mart_cluster_output` co ON cr.UNIQUE_ID = co.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_churn_explained` ce ON cr.UNIQUE_ID = ce.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_customer_clv` clv ON cr.UNIQUE_ID = clv.UNIQUE_ID
LEFT JOIN `__PROJECT__.marts.mart_spend_momentum` sm ON cr.UNIQUE_ID = sm.UNIQUE_ID
LEFT JOIN `__PROJECT__.staging.stg_customers` c ON cr.UNIQUE_ID = c.UNIQUE_ID;


-- v_dashboard_client_pitch.sql
-- Everything needed for the Client Pitch page in one flat view
-- Includes benchmarks, loyalty, time patterns

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_dashboard_client_pitch` AS
SELECT
    db.CATEGORY_TWO,
    db.DESTINATION,
    db.customers,
    db.total_spend,
    db.market_share_pct,
    db.penetration_pct,
    db.avg_txn_value,
    db.spend_per_customer,
    db.avg_share_of_wallet,
    db.spend_rank,

    -- Store loyalty
    sl.avg_loyalty_pct,
    sl.pct_loyal_50,
    sl.pct_loyal_80,
    sl.band_1_store,
    sl.band_2_stores,
    sl.band_3_4_stores,
    sl.band_5_7_stores,
    sl.band_8_plus,

    -- Time patterns
    tp.pct_early_morning,
    tp.pct_mid_morning,
    tp.pct_afternoon,
    tp.pct_late_afternoon,
    tp.pct_evening,
    tp.pct_weekend,
    tp.pct_weekday,
    tp.peak_hour,
    tp.peak_day_number,
    tp.pct_sunday, tp.pct_monday, tp.pct_tuesday, tp.pct_wednesday,
    tp.pct_thursday, tp.pct_friday, tp.pct_saturday

FROM `__PROJECT__.marts.mart_destination_benchmarks` db
LEFT JOIN `__PROJECT__.marts.mart_store_loyalty` sl
    ON db.CATEGORY_TWO = sl.CATEGORY_TWO AND db.DESTINATION = sl.DESTINATION
LEFT JOIN `__PROJECT__.marts.mart_store_time_patterns` tp
    ON db.CATEGORY_TWO = tp.CATEGORY_TWO AND db.DESTINATION = tp.DESTINATION;


-- v_client_segment_mix.sql
-- Per-client × category segment distribution for Looker Studio.
-- Feeds the "Customer Segments" page with client-specific numbers instead
-- of the FNB-wide mix (fixes the Clicks vs PNP identical-numbers problem).
-- Source: marts.mart_client_segment_mix (built in Step 4).

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_client_segment_mix` AS
SELECT
    csm.DESTINATION,
    csm.CATEGORY_TWO,
    csm.segment_name,
    csm.segment_customers,
    csm.segment_spend,
    csm.client_total_customers,
    csm.client_total_spend,
    csm.pct_of_client_customers,
    csm.pct_of_client_spend,
    csm.fnb_pct_of_customers,
    csm.index_vs_fnb,
    cs.business_description,
    cs.recommended_action,
    CASE csm.segment_name
        WHEN 'Champions'        THEN 1
        WHEN 'Loyal High Value' THEN 2
        WHEN 'Steady Mid-Tier'  THEN 3
        WHEN 'At Risk'          THEN 4
        WHEN 'Dormant'          THEN 5
        ELSE 6
    END AS segment_order
FROM `__PROJECT__.marts.mart_client_segment_mix` csm
LEFT JOIN `__PROJECT__.marts.mart_cluster_summary` cs
    ON csm.segment_name = cs.segment_name;


-- v_audience_catalog.sql
-- Audience Marketplace — one row per pre-packaged audience.
-- FNB-wide by design (advertisers buy audiences, activate via LiveRamp).
-- Source: marts.mart_audience_catalog.

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_audience_catalog` AS
SELECT
    audience_id,
    audience_name,
    audience_type,
    audience_size,
    avg_spend,
    avg_age,
    pct_female,
    avg_income,
    top_province,
    top_age_group,
    top_income_group,
    top_segment,
    avg_churn_prob,
    description
FROM `__PROJECT__.marts.mart_audience_catalog`;


-- v_audience_client_overlap.sql
-- Thin view wrapping the pre-computed mart_audience_client_overlap table.
-- The table is built once per pipeline run (Step 4); the view lets Looker
-- read it without re-running the heavy join on every dashboard interaction.
-- Keeping this as a VIEW (not a materialized re-aggregation) = R0 per query.

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_audience_client_overlap` AS
SELECT
    DESTINATION,
    CATEGORY_TWO,
    audience_id,
    audience_name,
    audience_type,
    overlap_customers,
    client_total_customers,
    pct_of_client
FROM `__PROJECT__.marts.mart_audience_client_overlap`;
