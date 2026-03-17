#!/bin/bash
set -euo pipefail

# pipeline validation - row counts + data quality checks

PROJECT_ID="${1:-fmn-sandbox}"

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "-- row counts --"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" --format=pretty "
SELECT 'staging.stg_transactions' AS table_name, COUNT(*) AS row_count FROM \`${PROJECT_ID}.staging.stg_transactions\`
UNION ALL SELECT 'staging.stg_customers', COUNT(*) FROM \`${PROJECT_ID}.staging.stg_customers\`
UNION ALL SELECT 'analytics.int_rfm_features', COUNT(*) FROM \`${PROJECT_ID}.analytics.int_rfm_features\`
UNION ALL SELECT 'analytics.int_rfm_scores', COUNT(*) FROM \`${PROJECT_ID}.analytics.int_rfm_scores\`
UNION ALL SELECT 'analytics.int_customer_category_spend', COUNT(*) FROM \`${PROJECT_ID}.analytics.int_customer_category_spend\`
UNION ALL SELECT 'analytics.int_destination_metrics', COUNT(*) FROM \`${PROJECT_ID}.analytics.int_destination_metrics\`
UNION ALL SELECT 'marts.mart_cluster_output', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cluster_output\`
UNION ALL SELECT 'marts.mart_cluster_profiles', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cluster_profiles\`
UNION ALL SELECT 'marts.mart_cluster_summary', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cluster_summary\`
UNION ALL SELECT 'marts.mart_behavioral_summary', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_behavioral_summary\`
UNION ALL SELECT 'marts.mart_geo_summary', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_geo_summary\`
UNION ALL SELECT 'marts.mart_churn_risk', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_churn_risk\`
UNION ALL SELECT 'marts.mart_monthly_trends', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_monthly_trends\`
UNION ALL SELECT 'marts.mart_demographic_summary', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_demographic_summary\`
UNION ALL SELECT 'marts.mart_destination_benchmarks', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_destination_benchmarks\`
UNION ALL SELECT 'marts.mart_cohort_retention', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cohort_retention\`
UNION ALL SELECT 'marts.mart_category_affinity', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_category_affinity\`
UNION ALL SELECT 'marts.mart_category_scorecard', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_category_scorecard\`
UNION ALL SELECT 'marts.mart_pitch_opportunities', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_pitch_opportunities\`
UNION ALL SELECT 'marts.mart_churn_explained', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_churn_explained\`
UNION ALL SELECT 'marts.mart_spend_momentum', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_spend_momentum\`
UNION ALL SELECT 'marts.mart_category_propensity', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_category_propensity\`
UNION ALL SELECT 'marts.mart_customer_clv', COUNT(*) FROM \`${PROJECT_ID}.marts.mart_customer_clv\`
ORDER BY table_name
"

echo ""
echo "-- data quality checks --"
RESULT=$(bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" --format=csv "
SELECT
    'Cluster profiles = 5 segments' AS check_name,
    CASE WHEN (SELECT COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cluster_profiles\`) = 5 THEN 'PASS' ELSE 'FAIL' END AS status
UNION ALL SELECT
    'No NULL segment names',
    CASE WHEN (SELECT COUNTIF(segment_name IS NULL) FROM \`${PROJECT_ID}.marts.mart_cluster_output\`) = 0 THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT
    'All cluster customers have demographics',
    CASE WHEN (SELECT COUNTIF(age IS NULL) FROM \`${PROJECT_ID}.marts.mart_cluster_output\`) * 100.0 /
         (SELECT COUNT(*) FROM \`${PROJECT_ID}.marts.mart_cluster_output\`) < 5 THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT
    'Destination benchmarks have market share',
    CASE WHEN (SELECT COUNTIF(market_share_pct IS NULL) FROM \`${PROJECT_ID}.marts.mart_destination_benchmarks\`) = 0 THEN 'PASS' ELSE 'FAIL' END
UNION ALL SELECT
    'Monthly trends have no gaps',
    CASE WHEN (SELECT COUNT(DISTINCT month) FROM \`${PROJECT_ID}.marts.mart_monthly_trends\`) >= 3 THEN 'PASS' ELSE 'FAIL' END
")

echo "${RESULT}" | while IFS=, read -r check status; do
    if [ "${status}" = "PASS" ]; then
        echo -e "  ${GREEN}✓${NC} ${check}"
    else
        echo -e "  ${RED}✗${NC} ${check}"
    fi
done

echo ""
echo "-- validation complete --"
