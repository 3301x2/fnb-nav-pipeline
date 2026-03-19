-- v_pitch_internal.sql
-- Internal view: all real destination names visible
-- Used by the Client Pitch page in "Internal" mode

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_pitch_internal` AS
SELECT
    db.CATEGORY_TWO,
    db.DESTINATION,
    db.DESTINATION AS display_name,  -- real name
    db.customers,
    db.total_spend,
    db.market_share_pct,
    db.penetration_pct,
    db.avg_txn_value,
    db.spend_per_customer,
    db.avg_share_of_wallet,
    db.spend_rank,
    db.transactions,
    'internal' AS view_mode
FROM `__PROJECT__.marts.mart_destination_benchmarks` db;


-- v_pitch_external.sql
-- External view: selected client shows real name, all others anonymized
-- In Looker Studio, the user sets a parameter for the client name
-- For now we create the view with a placeholder — Looker Studio parameter replaces it

CREATE OR REPLACE VIEW `__PROJECT__.marts.v_pitch_external` AS
SELECT
    CATEGORY_TWO,
    DESTINATION,  -- keep real name for filtering (hidden in Looker)
    CASE
        WHEN spend_rank = 1 THEN CONCAT('Market Leader (#', CAST(spend_rank AS STRING), ')')
        ELSE CONCAT('Competitor #', CAST(spend_rank AS STRING))
    END AS display_name,
    customers,
    total_spend,
    market_share_pct,
    penetration_pct,
    avg_txn_value,
    spend_per_customer,
    avg_share_of_wallet,
    spend_rank,
    transactions,
    'external' AS view_mode
FROM `__PROJECT__.marts.mart_destination_benchmarks`;
