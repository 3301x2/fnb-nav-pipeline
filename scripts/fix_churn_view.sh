#!/bin/bash
# fix_churn_view.sh - recreates the churn dashboard view
# usage: bash scripts/fix_churn_view.sh [sandbox|production]

ENV="${1:-sandbox}"
case "${ENV}" in
    sandbox|dev)     PROJECT="fmn-sandbox" ;;
    production|prod) PROJECT="fmn-production" ;;
    *) echo "Usage: bash scripts/fix_churn_view.sh [sandbox|production]"; exit 1 ;;
esac

echo "Fixing churn view on ${PROJECT}..."

bq query --use_legacy_sql=false --project_id="${PROJECT}" "
CREATE OR REPLACE VIEW \`${PROJECT}.marts.v_dashboard_churn\` AS
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
FROM \`${PROJECT}.marts.mart_churn_risk\` cr
LEFT JOIN \`${PROJECT}.marts.mart_cluster_output\` co ON cr.UNIQUE_ID = co.UNIQUE_ID
LEFT JOIN \`${PROJECT}.marts.mart_churn_explained\` ce ON cr.UNIQUE_ID = ce.UNIQUE_ID
LEFT JOIN \`${PROJECT}.marts.mart_customer_clv\` clv ON cr.UNIQUE_ID = clv.UNIQUE_ID
LEFT JOIN \`${PROJECT}.marts.mart_spend_momentum\` sm ON cr.UNIQUE_ID = sm.UNIQUE_ID
LEFT JOIN \`${PROJECT}.staging.stg_customers\` c ON cr.UNIQUE_ID = c.UNIQUE_ID
"

echo "Done. Now run: bash scripts/run.sh ${ENV} 6"
