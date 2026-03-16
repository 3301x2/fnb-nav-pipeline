#!/bin/bash
set -euo pipefail

# shows BigQuery cost breakdown for the pipeline
# usage: bash scripts/cost_report.sh [sandbox|production] [days]

ENV="${1:-sandbox}"
DAYS="${2:-30}"

case "${ENV}" in
    sandbox|dev)     PROJECT="fmn-sandbox" ;;
    production|prod) PROJECT="fmn-production" ;;
    *) echo "Usage: bash scripts/cost_report.sh [sandbox|production] [days]"; exit 1 ;;
esac

# BQ on-demand pricing: $6.25 per TB scanned (as of 2026, africa-south1)
PRICE_PER_TB=6.25

echo "----------------------------------------"
echo "  BigQuery Cost Report"
echo "  Project: ${PROJECT}"
echo "  Period:  last ${DAYS} days"
echo "----------------------------------------"
echo ""

echo "-- total bytes scanned + estimated cost --"
bq query --use_legacy_sql=false --project_id="${PROJECT}" --format=pretty "
SELECT
    COUNT(*) AS total_jobs,
    ROUND(SUM(total_bytes_processed) / POW(1024, 3), 2) AS total_gb_scanned,
    ROUND(SUM(total_bytes_processed) / POW(1024, 4), 4) AS total_tb_scanned,
    ROUND(SUM(total_bytes_processed) / POW(1024, 4) * ${PRICE_PER_TB}, 2) AS estimated_cost_usd,
    ROUND(SUM(total_slot_ms) / 1000 / 3600, 2) AS total_slot_hours
FROM \`${PROJECT}.region-africa-south1\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
"

echo ""
echo "-- cost by day --"
bq query --use_legacy_sql=false --project_id="${PROJECT}" --format=pretty "
SELECT
    DATE(creation_time) AS day,
    COUNT(*) AS queries,
    ROUND(SUM(total_bytes_processed) / POW(1024, 3), 2) AS gb_scanned,
    ROUND(SUM(total_bytes_processed) / POW(1024, 4) * ${PRICE_PER_TB}, 2) AS est_cost_usd
FROM \`${PROJECT}.region-africa-south1\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
GROUP BY day
ORDER BY day DESC
"

echo ""
echo "-- top 10 most expensive queries --"
bq query --use_legacy_sql=false --project_id="${PROJECT}" --format=pretty "
SELECT
    job_id,
    DATE(creation_time) AS day,
    ROUND(total_bytes_processed / POW(1024, 3), 2) AS gb_scanned,
    ROUND(total_bytes_processed / POW(1024, 4) * ${PRICE_PER_TB}, 4) AS est_cost_usd,
    ROUND(total_slot_ms / 1000, 1) AS slot_seconds,
    SUBSTR(query, 1, 80) AS query_preview
FROM \`${PROJECT}.region-africa-south1\`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
ORDER BY total_bytes_processed DESC
LIMIT 10
"

echo ""
echo "-- cost by table (which tables are scanned most) --"
bq query --use_legacy_sql=false --project_id="${PROJECT}" --format=pretty "
SELECT
    referenced_table.dataset_id,
    referenced_table.table_id,
    COUNT(*) AS times_scanned,
    ROUND(SUM(j.total_bytes_processed) / POW(1024, 3), 2) AS total_gb,
    ROUND(SUM(j.total_bytes_processed) / POW(1024, 4) * ${PRICE_PER_TB}, 2) AS est_cost_usd
FROM \`${PROJECT}.region-africa-south1\`.INFORMATION_SCHEMA.JOBS j,
     UNNEST(referenced_tables) AS referenced_table
WHERE j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${DAYS} DAY)
  AND j.job_type = 'QUERY'
  AND j.state = 'DONE'
  AND j.error_result IS NULL
GROUP BY dataset_id, table_id
ORDER BY total_gb DESC
LIMIT 15
"

echo ""
echo "-- storage size (current) --"
bq query --use_legacy_sql=false --project_id="${PROJECT}" --format=pretty "
SELECT
    table_schema AS dataset,
    table_name,
    ROUND(total_rows / 1e6, 2) AS rows_millions,
    ROUND(total_logical_bytes / POW(1024, 3), 2) AS size_gb,
    ROUND(total_logical_bytes / POW(1024, 3) * 0.02, 4) AS monthly_storage_usd
FROM \`${PROJECT}.region-africa-south1\`.INFORMATION_SCHEMA.TABLE_STORAGE
WHERE total_logical_bytes > 0
ORDER BY total_logical_bytes DESC
"

echo ""
echo "----------------------------------------"
echo "  pricing: \$${PRICE_PER_TB}/TB scanned (on-demand)"
echo "  storage: \$0.02/GB/month"
echo "----------------------------------------"
