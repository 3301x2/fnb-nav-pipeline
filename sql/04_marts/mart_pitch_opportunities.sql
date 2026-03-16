-- mart_pitch_opportunities.sql
-- ranks every client by growth potential within their category
-- helps sales team decide who to pitch next

CREATE OR REPLACE TABLE `__PROJECT__.marts.mart_pitch_opportunities` AS

WITH benchmarks AS (
    SELECT
        CATEGORY_TWO,
        DESTINATION,
        customers,
        transactions,
        total_spend,
        avg_txn_value,
        spend_per_customer,
        market_share_pct,
        penetration_pct,
        spend_rank
    FROM `__PROJECT__.marts.mart_destination_benchmarks`
),

-- category level stats
category_stats AS (
    SELECT
        CATEGORY_TWO,
        SUM(total_spend) AS category_total_spend,
        MAX(market_share_pct) AS leader_market_share,
        MAX(penetration_pct) AS leader_penetration,
        AVG(market_share_pct) AS avg_market_share,
        AVG(spend_per_customer) AS avg_spend_per_customer,
        COUNT(DISTINCT DESTINATION) AS total_competitors
    FROM benchmarks
    GROUP BY CATEGORY_TWO
),

-- churn risk per destination (avg churn prob of their customers)
dest_churn AS (
    SELECT
        cs.DESTINATION,
        cs.CATEGORY_TWO,
        ROUND(AVG(cr.churn_probability), 4) AS avg_churn_probability,
        COUNTIF(cr.churn_risk_level IN ('Critical', 'High')) AS high_risk_customers
    FROM `__PROJECT__.analytics.int_customer_category_spend` cs
    JOIN `__PROJECT__.marts.mart_churn_risk` cr ON cs.UNIQUE_ID = cr.UNIQUE_ID
    WHERE cs.DESTINATION IS NOT NULL
    GROUP BY cs.DESTINATION, cs.CATEGORY_TWO
)

SELECT
    b.CATEGORY_TWO,
    b.DESTINATION,
    b.customers,
    b.total_spend,
    b.market_share_pct,
    b.penetration_pct,
    b.spend_per_customer,
    b.spend_rank,
    c.category_total_spend,
    c.total_competitors,

    -- growth potential: how much of the category they dont have yet
    ROUND(c.category_total_spend * (1 - b.market_share_pct / 100), 0) AS addressable_market,

    -- gap to leader
    ROUND(c.leader_market_share - b.market_share_pct, 2) AS gap_to_leader_pct,
    ROUND(c.leader_penetration - b.penetration_pct, 2) AS penetration_gap_pct,

    -- spend efficiency: are they above or below avg spend per customer
    ROUND(SAFE_DIVIDE(b.spend_per_customer, c.avg_spend_per_customer) * 100 - 100, 1) AS spend_vs_avg_pct,

    -- churn exposure
    COALESCE(dc.avg_churn_probability, 0) AS avg_churn_probability,
    COALESCE(dc.high_risk_customers, 0) AS high_risk_customers,

    -- composite pitch score (0-100, higher = better opportunity)
    -- all components normalized to 0-1 before weighting
    ROUND(
        -- market size: percentile rank across all categories (0-1)
        (SAFE_DIVIDE(
            RANK() OVER (ORDER BY c.category_total_spend ASC) - 1,
            NULLIF(COUNT(*) OVER () - 1, 0)
        ) * 30) +
        -- growth gap: how far behind the leader (0 = leader, 1 = zero share)
        (SAFE_DIVIDE(c.leader_market_share - b.market_share_pct, NULLIF(c.leader_market_share, 0)) * 30) +
        -- low churn: 1 = no churn risk, 0 = certain churn
        ((1 - COALESCE(dc.avg_churn_probability, 0)) * 20) +
        -- spend efficiency: capped at 1 (at or above avg = full score)
        (LEAST(SAFE_DIVIDE(b.spend_per_customer, NULLIF(c.avg_spend_per_customer, 0)), 1) * 20)
    , 2) AS pitch_score,

    -- recommended action
    CASE
        WHEN b.spend_rank = 1 THEN 'Defend - you are the leader, retain customers'
        WHEN b.market_share_pct >= c.avg_market_share AND COALESCE(dc.avg_churn_probability, 0) < 0.3 THEN 'Grow - strong position, push for more share'
        WHEN b.market_share_pct >= c.avg_market_share AND COALESCE(dc.avg_churn_probability, 0) >= 0.3 THEN 'Protect - good share but churn risk is high'
        WHEN b.market_share_pct < c.avg_market_share AND b.spend_per_customer > c.avg_spend_per_customer THEN 'Attack - low share but high value customers'
        WHEN b.market_share_pct < c.avg_market_share THEN 'Opportunity - low share, room to grow'
        ELSE 'Monitor'
    END AS recommended_action

FROM benchmarks b
JOIN category_stats c ON b.CATEGORY_TWO = c.CATEGORY_TWO
LEFT JOIN dest_churn dc ON b.DESTINATION = dc.DESTINATION AND b.CATEGORY_TWO = dc.CATEGORY_TWO
ORDER BY pitch_score DESC;
