-- ════════════════════════════════════════════════════════════════
-- Looker Studio Views — ALL mart tables
-- ════════════════════════════════════════════════════════════════
-- One view per Looker Studio data source.
-- Run: bash scripts/run.sh sandbox 6
-- ════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════
-- CORE PIPELINE VIEWS (original 10)
-- ═══════════════════════════════════════════════════

-- 1. Executive Summary KPIs
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_executive_summary` AS
SELECT
    (SELECT COUNT(*) FROM `__PROJECT__.staging.stg_customers`) AS total_customers,
    (SELECT COUNT(*) FROM `__PROJECT__.staging.stg_transactions`) AS total_transactions,
    (SELECT SUM(val_trns) FROM `__PROJECT__.marts.mart_cluster_output`) AS total_spend,
    (SELECT COUNT(DISTINCT DESTINATION) FROM `__PROJECT__.marts.mart_destination_benchmarks`) AS total_destinations,
    (SELECT COUNT(*) FROM `__PROJECT__.marts.mart_cluster_output` WHERE segment_name = 'Champions') AS champion_customers,
    (SELECT ROUND(SUM(val_trns), 0) FROM `__PROJECT__.marts.mart_cluster_output` WHERE segment_name = 'Champions') AS champion_spend,
    (SELECT COUNT(*) FROM `__PROJECT__.marts.mart_churn_risk` WHERE churn_risk_level IN ('Critical', 'High')) AS at_risk_customers,
    (SELECT ROUND(SUM(total_spend), 0) FROM `__PROJECT__.marts.mart_churn_risk` WHERE churn_risk_level IN ('Critical', 'High')) AS at_risk_spend;

-- 2. Customer Segments
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_customer_segments` AS
SELECT
    segment_name,
    COUNT(*) AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_customers,
    ROUND(SUM(val_trns), 0) AS total_spend,
    ROUND(SUM(val_trns) * 100.0 / SUM(SUM(val_trns)) OVER(), 1) AS pct_revenue,
    ROUND(AVG(val_trns), 0) AS avg_spend,
    ROUND(AVG(nr_trns), 0) AS avg_transactions,
    ROUND(AVG(lst_trns_days), 0) AS avg_recency_days,
    ROUND(AVG(active_destinations), 1) AS avg_merchants,
    ROUND(AVG(active_months), 1) AS avg_active_months
FROM `__PROJECT__.marts.mart_cluster_output`
GROUP BY segment_name;

-- 3. Spend Share & Benchmarks
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_spend_share` AS
SELECT * FROM `__PROJECT__.marts.mart_destination_benchmarks`;

-- 4. Demographics
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_demographics` AS
SELECT * FROM `__PROJECT__.marts.mart_demographic_summary`;

-- 5. Monthly Trends
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_monthly_trends` AS
SELECT * FROM `__PROJECT__.marts.mart_monthly_trends`;

-- 6. Churn Risk Summary
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_churn_risk` AS
SELECT
    churn_risk_level,
    COUNT(*) AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total,
    ROUND(AVG(churn_probability) * 100, 1) AS avg_churn_pct,
    ROUND(SUM(total_spend), 0) AS total_spend_at_risk,
    ROUND(AVG(total_spend), 0) AS avg_spend,
    ROUND(AVG(days_since_last), 0) AS avg_days_since_last,
    ROUND(AVG(txns_last_3m), 1) AS avg_recent_txns
FROM `__PROJECT__.marts.mart_churn_risk`
GROUP BY churn_risk_level;

-- 7. Churn Detail (customer-level drill-down)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_churn_detail` AS
SELECT * FROM `__PROJECT__.marts.mart_churn_risk`;

-- 8. Geo Insights
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_geo_insights` AS
SELECT
    *,
    ROUND(total_spend * 100.0 / SUM(total_spend) OVER(PARTITION BY CATEGORY_TWO), 1) AS pct_of_category
FROM `__PROJECT__.marts.mart_geo_summary`;

-- 9. Behavioral
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_behavioral` AS
SELECT * FROM `__PROJECT__.marts.mart_behavioral_summary`;

-- 10. Cluster Profiles
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_cluster_profiles` AS
SELECT * FROM `__PROJECT__.marts.mart_cluster_profiles`;


-- ═══════════════════════════════════════════════════
-- NEW ANALYTICS VIEWS (added by Prosper)
-- ═══════════════════════════════════════════════════

-- 11. Cohort Retention
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_cohort_retention` AS
SELECT * FROM `__PROJECT__.marts.mart_cohort_retention`;

-- 12. Category Affinity (cross-shopping patterns)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_category_affinity` AS
SELECT * FROM `__PROJECT__.marts.mart_category_affinity`;

-- 13. Category Scorecard (portfolio overview)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_category_scorecard` AS
SELECT * FROM `__PROJECT__.marts.mart_category_scorecard`;

-- 14. Pitch Opportunities (ranked by growth potential)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_pitch_opportunities` AS
SELECT * FROM `__PROJECT__.marts.mart_pitch_opportunities`;

-- 15. Spend Momentum (acceleration/deceleration)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_spend_momentum` AS
SELECT * FROM `__PROJECT__.marts.mart_spend_momentum`;

-- 16. Churn Explained (ML.EXPLAIN_PREDICT — why each customer is at risk)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_churn_explained` AS
SELECT * FROM `__PROJECT__.marts.mart_churn_explained`;

-- 17. Category Propensity (next-best-category for each customer)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_category_propensity` AS
SELECT * FROM `__PROJECT__.marts.mart_category_propensity`;

-- 18. Customer Lifetime Value (predicted 12-month spend)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_customer_clv` AS
SELECT * FROM `__PROJECT__.marts.mart_customer_clv`;

-- 19. Cluster Summary (exec descriptions + actions)
CREATE OR REPLACE VIEW `__PROJECT__.marts.v_cluster_summary` AS
SELECT * FROM `__PROJECT__.marts.mart_cluster_summary`;
