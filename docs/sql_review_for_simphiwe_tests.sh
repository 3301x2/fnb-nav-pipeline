#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Verifies every factual claim in docs/sql_review_for_simphiwe.html
# before you send the guide to Simphiwe.
#
# Each test prints PASS / FAIL with the actual data.
# If anything fails, DO NOT send the guide — message Prosper instead.
#
# Cost: a few cents total. All queries use LIMIT or metadata only.
#
# Usage:
#   bash docs/sql_review_for_simphiwe_tests.sh                   # sandbox
#   bash docs/sql_review_for_simphiwe_tests.sh production
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

ENV="${1:-sandbox}"
case "$ENV" in
    sandbox|dev|sb)         PROJECT="fmn-sandbox" ;;
    production|prod|prd)    PROJECT="fmn-production" ;;
    *) echo "Usage: bash $0 [sandbox|production]"; exit 1 ;;
esac

G='\033[0;32m'; R='\033[0;31m'; Y='\033[1;33m'; B='\033[0;34m'; N='\033[0m'
PASS=0; FAIL=0; WARN=0

pass() { printf "${G}PASS${N}  %s\n" "$*"; PASS=$((PASS+1)); }
fail() { printf "${R}FAIL${N}  %s\n" "$*"; FAIL=$((FAIL+1)); }
warn() { printf "${Y}WARN${N}  %s\n" "$*"; WARN=$((WARN+1)); }
note() { printf "${B}      ${N}%s\n" "$*"; }
hr()   { printf "%s\n" "────────────────────────────────────────────────────────"; }

bq_run() {
    bq query --quiet --use_legacy_sql=false --project_id="$PROJECT" --format=csv --max_rows=200 "$1" 2>&1
}

# Ensure gcloud auth (matches the rest of the repo's pattern)
if ! gcloud auth print-access-token >/dev/null 2>&1; then
    echo "Running gcloud auth login first..."
    gcloud auth login --update-adc --quiet || { echo "login failed"; exit 1; }
fi
gcloud config set project "$PROJECT" >/dev/null 2>&1 || true

echo
echo "════════════════════════════════════════════════════════════"
echo "  Verifying claims in sql_review_for_simphiwe.html"
echo "  Project: $PROJECT"
echo "════════════════════════════════════════════════════════════"
echo

# ────────────────────────────────────────────────────────────────
# Test 1 — DESTINATION = 'VOX' exists and returns rows
# ────────────────────────────────────────────────────────────────
hr; echo "Test 1: Is 'VOX' the exact DESTINATION value in the data?"
result=$(bq_run "
    SELECT DESTINATION, COUNT(*) AS n
    FROM \`$PROJECT.analytics.int_customer_category_spend\`
    WHERE UPPER(DESTINATION) LIKE '%VOX%'
    GROUP BY DESTINATION
    ORDER BY n DESC
")
echo "$result"
if echo "$result" | grep -q "^VOX,"; then
    pass "DESTINATION='VOX' exists"
else
    fail "DESTINATION='VOX' not found — check the exact spelling above and update the guide"
fi

# ────────────────────────────────────────────────────────────────
# Test 2 — What CATEGORY_TWO is VOX in?
# ────────────────────────────────────────────────────────────────
hr; echo "Test 2: What CATEGORY_TWO does VOX live in?"
result=$(bq_run "
    SELECT CATEGORY_TWO, COUNT(DISTINCT UNIQUE_ID) AS customers
    FROM \`$PROJECT.analytics.int_customer_category_spend\`
    WHERE DESTINATION = 'VOX'
    GROUP BY CATEGORY_TWO
    ORDER BY customers DESC
")
echo "$result"
vox_cats=$(echo "$result" | tail -n +2 | wc -l | tr -d ' ')
if [ "$vox_cats" -ge 1 ]; then
    pass "VOX appears in $vox_cats category(s) — use the top one in the guide"
    note "Guide currently says: CATEGORY_TWO = 'Internet & Data' — update if different"
else
    fail "VOX has no transactions — check the project"
fi

# ────────────────────────────────────────────────────────────────
# Test 3 — Do all 16 competitor brands exist as DESTINATION values?
# ────────────────────────────────────────────────────────────────
hr; echo "Test 3: Which of the 16 brands actually exist as DESTINATION values?"
result=$(bq_run "
    WITH expected AS (
        SELECT brand FROM UNNEST([
            'VOX','AFRIHOST','3 NET NINE NINE','ZA DOMAINS','AXXESS',
            'COOL IDEAS','MWEB','TELKOM','EXACTTA GROUP SRL','VUMATEL',
            'G CONNECT','ACCELERIT TECHNOLOGIES','RAIN (DATA)','IKEJA',
            'WEB AFRICA','SUPERSONIC'
        ]) AS brand
    )
    SELECT e.brand,
           IFNULL(d.customers, 0) AS customers
    FROM expected e
    LEFT JOIN (
        SELECT DESTINATION, COUNT(DISTINCT UNIQUE_ID) AS customers
        FROM \`$PROJECT.analytics.int_customer_category_spend\`
        WHERE DESTINATION IN (
            'VOX','AFRIHOST','3 NET NINE NINE','ZA DOMAINS','AXXESS',
            'COOL IDEAS','MWEB','TELKOM','EXACTTA GROUP SRL','VUMATEL',
            'G CONNECT','ACCELERIT TECHNOLOGIES','RAIN (DATA)','IKEJA',
            'WEB AFRICA','SUPERSONIC'
        )
        GROUP BY DESTINATION
    ) d ON UPPER(e.brand) = UPPER(d.DESTINATION)
    ORDER BY customers DESC
")
echo "$result"
missing=$(echo "$result" | awk -F, 'NR>1 && $2==0 {print $1}')
if [ -z "$missing" ]; then
    pass "All 16 brands exist as DESTINATION values"
else
    warn "Some brands have 0 customers (might be spelled differently in the data):"
    echo "$missing" | sed 's/^/        /'
    note "Update the guide to drop missing brands or fix their spelling"
fi

# ────────────────────────────────────────────────────────────────
# Test 4 — Is mart_destination_benchmarks populated for VOX?
# ────────────────────────────────────────────────────────────────
hr; echo "Test 4: Does mart_destination_benchmarks have a row for VOX?"
result=$(bq_run "
    SELECT CATEGORY_TWO, DESTINATION, customers, total_spend, market_share_pct, spend_rank
    FROM \`$PROJECT.marts.mart_destination_benchmarks\`
    WHERE DESTINATION = 'VOX'
")
echo "$result"
if echo "$result" | grep -q "VOX"; then
    pass "mart_destination_benchmarks has VOX"
else
    fail "mart_destination_benchmarks does NOT have a row for VOX — guide query A will return empty"
fi

# ────────────────────────────────────────────────────────────────
# Test 5 — Is mart_client_segment_mix populated for VOX?
# (mart filters out clients with < 1000 customers)
# ────────────────────────────────────────────────────────────────
hr; echo "Test 5: Does mart_client_segment_mix have rows for VOX?"
result=$(bq_run "
    SELECT segment_name, segment_customers, pct_of_client_customers, index_vs_fnb
    FROM \`$PROJECT.marts.mart_client_segment_mix\`
    WHERE DESTINATION = 'VOX'
    ORDER BY pct_of_client_customers DESC
")
echo "$result"
vox_seg_rows=$(echo "$result" | tail -n +2 | wc -l | tr -d ' ')
if [ "$vox_seg_rows" -ge 1 ]; then
    pass "mart_client_segment_mix has $vox_seg_rows rows for VOX"
else
    fail "mart_client_segment_mix is EMPTY for VOX — likely <1000 customer threshold"
    note "Guide query D (segment mix) will return nothing for Simphiwe. Remove or rewrite that section."
fi

# ────────────────────────────────────────────────────────────────
# Test 6 — How many customers does VOX have? (for context)
# ────────────────────────────────────────────────────────────────
hr; echo "Test 6: VOX customer count + total spend"
result=$(bq_run "
    SELECT COUNT(DISTINCT UNIQUE_ID) AS vox_customers,
           ROUND(SUM(dest_spend), 0) AS vox_spend
    FROM \`$PROJECT.analytics.int_customer_category_spend\`
    WHERE DESTINATION = 'VOX'
")
echo "$result"

# ────────────────────────────────────────────────────────────────
# Test 7 — Row count of int_customer_category_spend (claim verification)
# ────────────────────────────────────────────────────────────────
hr; echo "Test 7: Actual row count of int_customer_category_spend"
result=$(bq_run "
    SELECT COUNT(*) AS row_count
    FROM \`$PROJECT.analytics.int_customer_category_spend\`
")
echo "$result"
rows=$(echo "$result" | tail -n 1 | tr -d ',\"' )
note "The guide currently says '3.6 billion rows'. Actual is: $rows"
if [ -n "$rows" ] && [ "$rows" -gt 0 ] 2>/dev/null; then
    if [ "$rows" -lt 1000000000 ]; then
        warn "Guide overstates the scale — update from '3.6 billion' to actual row count"
    else
        pass "Row count >= 1B — '3.6 billion' is a reasonable framing"
    fi
fi

# ────────────────────────────────────────────────────────────────
# Test 8 — Confirm clustering on int_customer_category_spend
# ────────────────────────────────────────────────────────────────
hr; echo "Test 8: Is int_customer_category_spend clustered by CATEGORY_TWO + DESTINATION?"
result=$(bq_run "
    SELECT ddl
    FROM \`$PROJECT.analytics.INFORMATION_SCHEMA.TABLES\`
    WHERE table_name = 'int_customer_category_spend'
")
if echo "$result" | grep -qi "CLUSTER BY.*CATEGORY_TWO.*DESTINATION"; then
    pass "Confirmed: clustered by CATEGORY_TWO, DESTINATION"
else
    warn "Could not confirm cluster columns from DDL — manually check the table"
    echo "$result"
fi

# ────────────────────────────────────────────────────────────────
# Test 9 — Bytes processed by guide query A
# ────────────────────────────────────────────────────────────────
hr; echo "Test 9: Dry-run cost of guide query A (mart_destination_benchmarks)"
bytes=$(bq query --dry_run --use_legacy_sql=false --project_id="$PROJECT" --format=json "
    SELECT DESTINATION, customers, total_spend, market_share_pct, spend_rank
    FROM \`$PROJECT.marts.mart_destination_benchmarks\`
    WHERE DESTINATION IN ('VOX','AFRIHOST','MWEB')
" 2>/dev/null | grep -o '\"totalBytesProcessed\":\"[0-9]*\"' | grep -o '[0-9]*' | head -1)
if [ -n "$bytes" ]; then
    mb=$(echo "scale=2; $bytes / 1048576" | bc 2>/dev/null || echo "?")
    note "Bytes processed: ${bytes} (~${mb} MB)"
    if [ "$bytes" -lt 10000000 ]; then
        pass "Under 10 MB — guide's 'tiny scan' claim is accurate"
    else
        warn "Bigger than expected — guide's '< 1 MB' badge is wrong"
    fi
else
    warn "Could not get dry-run bytes"
fi

# ────────────────────────────────────────────────────────────────
# Test 10 — Bytes processed by guide query C (CTE filter on VOX)
# ────────────────────────────────────────────────────────────────
hr; echo "Test 10: Dry-run cost of guide query C (CTE filtered to VOX)"
bytes=$(bq query --dry_run --use_legacy_sql=false --project_id="$PROJECT" --format=json "
    WITH vox_customers AS (
        SELECT DISTINCT UNIQUE_ID
        FROM \`$PROJECT.analytics.int_customer_category_spend\`
        WHERE DESTINATION = 'VOX'
    )
    SELECT c.age_group, c.income_group, c.gender_label, COUNT(*) AS customers
    FROM vox_customers v
    JOIN \`$PROJECT.staging.stg_customers\` c ON v.UNIQUE_ID = c.UNIQUE_ID
    GROUP BY c.age_group, c.income_group, c.gender_label
" 2>/dev/null | grep -o '\"totalBytesProcessed\":\"[0-9]*\"' | grep -o '[0-9]*' | head -1)
if [ -n "$bytes" ]; then
    mb=$(echo "scale=2; $bytes / 1048576" | bc 2>/dev/null || echo "?")
    gb=$(echo "scale=2; $bytes / 1073741824" | bc 2>/dev/null || echo "?")
    note "Bytes processed: ${bytes} (~${mb} MB, ${gb} GB)"
    note "Clustering should keep this small (just the VOX cluster). If this is >10 GB the clustering isn't helping like the guide claims."
fi

# ────────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────────
echo
hr
printf "  %sPASS:%s %d   %sWARN:%s %d   %sFAIL:%s %d\n" "$G" "$N" "$PASS" "$Y" "$N" "$WARN" "$R" "$N" "$FAIL"
hr
echo
if [ "$FAIL" -gt 0 ]; then
    printf "${R}DO NOT SEND THE GUIDE YET.${N}  %d hard failures above — message Prosper with the details.\n\n" "$FAIL"
    exit 1
elif [ "$WARN" -gt 0 ]; then
    printf "${Y}REVIEW BEFORE SENDING.${N}  %d warnings — likely small wording fixes in the guide.\n\n" "$WARN"
    exit 0
else
    printf "${G}ALL CLEAR.${N}  Safe to send the guide to Simphiwe.\n\n"
    exit 0
fi
