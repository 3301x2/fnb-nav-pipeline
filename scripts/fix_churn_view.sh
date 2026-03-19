#!/bin/bash
# fix_churn_view.sh — run this on the work machine, then re-run step 6
# Usage: bash fix_churn_view.sh

bq query --use_legacy_sql=false "
CREATE OR REPLACE VIEW \`fmn-sandbox.marts.v_dashboard_churn\` AS
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
FROM \`fmn-sandbox.marts.mart_churn_risk\` cr
LEFT JOIN \`fmn-sandbox.marts.mart_cluster_output\` co ON cr.UNIQUE_ID = co.UNIQUE_ID
LEFT JOIN \`fmn-sandbox.marts.mart_churn_explained\` ce ON cr.UNIQUE_ID = ce.UNIQUE_ID
LEFT JOIN \`fmn-sandbox.marts.mart_customer_clv\` clv ON cr.UNIQUE_ID = clv.UNIQUE_ID
LEFT JOIN \`fmn-sandbox.marts.mart_spend_momentum\` sm ON cr.UNIQUE_ID = sm.UNIQUE_ID
LEFT JOIN \`fmn-sandbox.staging.stg_customers\` c ON cr.UNIQUE_ID = c.UNIQUE_ID
"

echo ""
echo "Done. Now run: bash scripts/run.sh sandbox 6"
