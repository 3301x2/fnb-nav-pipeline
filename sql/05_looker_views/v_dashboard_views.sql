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
