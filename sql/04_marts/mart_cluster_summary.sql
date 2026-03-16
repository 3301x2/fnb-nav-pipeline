-- mart_cluster_summary.sql
-- business friendly summary with descriptions and recomended actions per segment (5 rows)

CREATE OR REPLACE TABLE `fmn-sandbox.marts.mart_cluster_summary` AS

SELECT
    segment_name,
    cluster_id,
    customer_count,
    pct_of_total,
    avg_total_spend,
    total_segment_spend,
    avg_transactions,
    avg_recency_days,
    avg_txn_value,
    avg_merchants,
    avg_categories,
    avg_age,
    avg_income,
    top_age_group,
    top_income_group,

    CASE segment_name
        WHEN 'Champions' THEN
            'Highest-value customers with strong recent activity. They spend the most per transaction and across the most categories. Priority: retain with exclusive offers and early access.'
        WHEN 'Loyal High Value' THEN
            'Consistently high spenders with the highest transaction frequency. They shop across many merchants regularly. Priority: reward loyalty, cross-sell into new categories.'
        WHEN 'Steady Mid-Tier' THEN
            'Reliable middle-ground customers with moderate spend and frequency. Largest segment by count. Priority: upsell to higher tiers with targeted promotions.'
        WHEN 'At Risk' THEN
            'Previously active customers showing declining engagement — lower frequency and higher recency. Priority: re-engagement campaigns before they churn.'
        WHEN 'Dormant' THEN
            'Lowest spend and frequency with the longest gaps between purchases. At high risk of full churn. Priority: win-back offers or accept natural attrition.'
    END                                                            AS business_description,

    CASE segment_name
        WHEN 'Champions'        THEN 'Retain & Reward'
        WHEN 'Loyal High Value' THEN 'Cross-Sell & Grow'
        WHEN 'Steady Mid-Tier'  THEN 'Upsell & Activate'
        WHEN 'At Risk'          THEN 'Re-Engage'
        WHEN 'Dormant'          THEN 'Win-Back or Release'
    END                                                            AS recommended_action

FROM `fmn-sandbox.marts.mart_cluster_profiles`
ORDER BY avg_total_spend DESC;
