#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════════
# FNB NAV Pipeline — Step-by-Step Build
#
# Usage:
#   bash scripts/run.sh          → runs ALL steps (0-5)
#   bash scripts/run.sh 0        → create datasets only
#   bash scripts/run.sh 1        → staging only
#   bash scripts/run.sh 2        → intermediate only
#   bash scripts/run.sh 3        → ML model + predictions only
#   bash scripts/run.sh 4        → marts only
#   bash scripts/run.sh 5        → validation only
#
# Run them one at a time:
#   bash scripts/run.sh 0   # check BigQuery → datasets exist
#   bash scripts/run.sh 1   # check BigQuery → staging tables populated
#   bash scripts/run.sh 2   # check BigQuery → analytics tables populated
#   bash scripts/run.sh 3   # check BigQuery → ML model + cluster_output
#   bash scripts/run.sh 4   # check BigQuery → all 8 mart tables
#   bash scripts/run.sh 5   # validation report
#
# Or run everything:
#   bash scripts/run.sh
#
# Prerequisites:
#   - gcloud CLI authenticated (gcloud auth login)
#   - bq CLI available
#   - terraform installed (optional, for infra management)
# ════════════════════════════════════════════════════════════════

# ── Configuration ──────────────────────────────────────────────
# ── Environment ────────────────────────────────────────────────
# Usage:
#   bash scripts/run.sh sandbox          → all steps on fmn-sandbox
#   bash scripts/run.sh production 3     → step 3 on fmn-production
#   bash scripts/run.sh sandbox 1        → step 1 on fmn-sandbox

ENV="${1:-sandbox}"
STEP="${2:-all}"

case "${ENV}" in
    sandbox|dev|sb)
        PROJECT_ID="fmn-sandbox"
        ;;
    production|prod|prd)
        PROJECT_ID="fmn-production"
        ;;
    *)
        # If first arg looks like a step number, assume sandbox
        if [[ "${ENV}" =~ ^[0-5]$|^all$ ]]; then
            STEP="${ENV}"
            PROJECT_ID="fmn-sandbox"
            ENV="sandbox"
        else
            echo "Unknown environment: ${ENV}"
            echo "Usage: bash scripts/run.sh [sandbox|production] [0-5|all]"
            echo ""
            echo "Examples:"
            echo "  bash scripts/run.sh sandbox       → full pipeline on fmn-sandbox"
            echo "  bash scripts/run.sh production 3   → step 3 on fmn-production"
            echo "  bash scripts/run.sh sandbox 1      → step 1 on fmn-sandbox"
            echo "  bash scripts/run.sh 3              → step 3 on fmn-sandbox (default)"
            exit 1
        fi
        ;;
esac

LOCATION="africa-south1"

# Source datasets (where raw data lives)
RAW_DATASET="customer_spend"
LOOKUP_DATASET="spend_lookups"

# Target datasets (pipeline creates these)
STAGING_DATASET="staging"
ANALYTICS_DATASET="analytics"
MARTS_DATASET="marts"

# ── Helpers ────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${REPO_ROOT}/sql"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC} $*"; }
warn() { echo -e "${YELLOW}  ⚠${NC} $*"; }
fail() { echo -e "${RED}  ✗ $*${NC}"; exit 1; }

run_sql() {
    local file="$1"
    local desc="$2"
    log "Running: ${desc}"
    # Replace placeholder with actual project ID at runtime
    sed "s/__PROJECT__/${PROJECT_ID}/g" "${file}" | \
    bq query \
        --use_legacy_sql=false \
        --project_id="${PROJECT_ID}" \
        --max_rows=0 \
    && ok "${desc}" \
    || fail "${desc} — query failed"
}

elapsed() {
    local start=$1
    local end=$(date +%s)
    local diff=$((end - start))
    echo "$((diff / 60))m $((diff % 60))s"
}

# ── Pre-flight checks ─────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════════"
echo "  FNB NAV Pipeline"
echo "  Environment: ${ENV}"
echo "  Project:     ${PROJECT_ID}"
echo "  Step:        ${STEP}"
echo "  Started:     $(date)"
echo "════════════════════════════════════════════════════════════"
echo ""

command -v bq >/dev/null 2>&1 || fail "bq CLI not found. Install: gcloud components install bq"
log "Pre-flight checks passed"
PIPELINE_START=$(date +%s)


# ══════════════════════════════════════════════════════════════
# STEP 0: Create datasets
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "0" ]]; then
    log "STEP 0: Creating datasets..."
    for ds in "${STAGING_DATASET}" "${ANALYTICS_DATASET}" "${MARTS_DATASET}"; do
        bq mk --project_id="${PROJECT_ID}" --location="${LOCATION}" --dataset "${PROJECT_ID}:${ds}" 2>/dev/null \
            && ok "Created dataset: ${ds}" \
            || ok "Dataset exists: ${ds}"
    done
    echo ""
    ok "Step 0 complete. Check BigQuery → 3 datasets should exist."
    echo ""
    if [[ "${STEP}" == "0" ]]; then
        echo "  Next: bash scripts/run.sh 1"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 1: Staging
# Creates: staging.stg_transactions, staging.stg_customers
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "1" ]]; then
    log "STEP 1: Staging layer"
    log "  → stg_transactions: joins all 6 lookup tables, strips PII"
    log "  → stg_customers: deduplicates, renames demo_* → real names"
    STEP_START=$(date +%s)

    run_sql "${SQL_DIR}/01_staging/stg_transactions.sql" \
        "stg_transactions (join lookups, partition by month, cluster by category + destination)"

    run_sql "${SQL_DIR}/01_staging/stg_customers.sql" \
        "stg_customers (deduplicate, rename demo_1→gender, demo_2→age, demo_7→estimated_income, etc.)"

    ok "Step 1 complete ($(elapsed ${STEP_START}))"
    echo ""
    echo "  ✅ Check BigQuery:"
    echo "     SELECT COUNT(*) FROM staging.stg_transactions;"
    echo "     SELECT COUNT(*) FROM staging.stg_customers;"
    echo "     SELECT * FROM staging.stg_customers LIMIT 5;  -- verify column names"
    echo ""
    if [[ "${STEP}" == "1" ]]; then
        echo "  Next: bash scripts/run.sh 2"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 2: Intermediate
# Creates: analytics.int_rfm_features, int_rfm_scores,
#          int_customer_category_spend, int_destination_metrics
# Depends: Step 1 (staging tables must exist)
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "2" ]]; then
    log "STEP 2: Intermediate layer"
    log "  → int_rfm_features: 20+ behavioral features per customer"
    log "  → int_rfm_scores: quintile scoring 1-5 for clustering"
    log "  → int_customer_category_spend: per customer × category × destination"
    log "  → int_destination_metrics: per-destination KPIs for benchmarks"
    STEP_START=$(date +%s)

    run_sql "${SQL_DIR}/02_intermediate/int_rfm_features.sql" \
        "int_rfm_features (20+ behavioral features per customer)"

    run_sql "${SQL_DIR}/02_intermediate/int_rfm_scores.sql" \
        "int_rfm_scores (quintile scoring 1-5)"

    run_sql "${SQL_DIR}/02_intermediate/int_customer_category_spend.sql" \
        "int_customer_category_spend (per customer × category × destination spend)"

    run_sql "${SQL_DIR}/02_intermediate/int_destination_metrics.sql" \
        "int_destination_metrics (per destination KPIs within each category)"

    ok "Step 2 complete ($(elapsed ${STEP_START}))"
    echo ""
    echo "  ✅ Check BigQuery:"
    echo "     SELECT COUNT(*) FROM analytics.int_rfm_features;"
    echo "     SELECT * FROM analytics.int_rfm_features LIMIT 5;"
    echo "     SELECT COUNT(*) FROM analytics.int_customer_category_spend;"
    echo "     SELECT DISTINCT CATEGORY_TWO FROM analytics.int_destination_metrics ORDER BY 1;"
    echo ""
    if [[ "${STEP}" == "2" ]]; then
        echo "  Next: bash scripts/run.sh 3"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 3: ML Models
# Creates: analytics.kmeans_customer_segments (MODEL),
#          analytics.churn_classifier (MODEL),
#          marts.mart_cluster_output (TABLE),
#          marts.mart_churn_risk (TABLE)
# Depends: Step 2 (int_rfm_scores must exist)
#
# ⚠️  This step takes ~5-10 minutes:
#   - train_model.sql: k-means clustering (~60-120s)
#   - predict_and_name.sql: cluster assignment (~30s)
#   - train_churn_model.sql: boosted tree classifier (~3-5 min)
#   - predict_churn.sql: churn probability scoring (~1-2 min)
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "3" ]]; then
    log "STEP 3: ML layer (2 models)"
    log "  → K-means customer segmentation"
    log "  → Boosted tree churn classifier"
    log "  ⏱️  This step takes ~5-10 minutes..."
    STEP_START=$(date +%s)

    run_sql "${SQL_DIR}/03_ml/train_model.sql" \
        "K-means model training (9 features, k=5, standardize=TRUE)"

    run_sql "${SQL_DIR}/03_ml/predict_and_name.sql" \
        "ML.PREDICT → assign clusters → name segments → join demographics"

    run_sql "${SQL_DIR}/03_ml/train_churn_model.sql" \
        "Churn classifier training (logistic regression, 15 features)"

    bq rm -f ${PROJECT_ID}:marts.mart_churn_risk 2>/dev/null || true

    run_sql "${SQL_DIR}/03_ml/predict_churn.sql" \
        "Churn prediction → probability scores → mart_churn_risk"

    ok "Step 3 complete ($(elapsed ${STEP_START}))"
    echo ""
    echo "  ✅ Check BigQuery:"
    echo "     -- K-means model:"
    echo "     SELECT * FROM ML.EVALUATE(MODEL analytics.kmeans_customer_segments);"
    echo "     -- Segments:"
    echo "     SELECT segment_name, COUNT(*) AS n FROM marts.mart_cluster_output GROUP BY 1 ORDER BY n DESC;"
    echo "     -- Churn model:"
    echo "     SELECT * FROM ML.EVALUATE(MODEL analytics.churn_classifier);"
    echo "     -- Churn scores:"
    echo "     SELECT churn_risk_level, COUNT(*) AS n FROM marts.mart_churn_risk GROUP BY 1 ORDER BY n DESC;"
    echo ""
    if [[ "${STEP}" == "3" ]]; then
        echo "  Next: bash scripts/run.sh 4"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 4: Marts
# Creates: 10 mart tables + 2 audience tables (dashboard-ready)
# Depends: Steps 1-3 (all upstream tables + model must exist)
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "4" ]]; then
    log "STEP 4: Mart layer (10 tables — churn already built in step 3)"
    STEP_START=$(date +%s)

    run_sql "${SQL_DIR}/04_marts/mart_cluster_profiles.sql" \
        "mart_cluster_profiles (segment averages + demographics)"

    run_sql "${SQL_DIR}/04_marts/mart_cluster_summary.sql" \
        "mart_cluster_summary (business descriptions + recommended actions)"

    run_sql "${SQL_DIR}/04_marts/mart_behavioral_summary.sql" \
        "mart_behavioral_summary (time-of-day, weekend/weekday patterns)"

    run_sql "${SQL_DIR}/04_marts/mart_geo_summary.sql" \
        "mart_geo_summary (province × municipality × category)"

    run_sql "${SQL_DIR}/04_marts/mart_monthly_trends.sql" \
        "mart_monthly_trends (monthly spend per category × destination)"

    run_sql "${SQL_DIR}/04_marts/mart_demographic_summary.sql" \
        "mart_demographic_summary (demographics per category)"

    run_sql "${SQL_DIR}/04_marts/mart_destination_benchmarks.sql" \
        "mart_destination_benchmarks (all destinations — dashboard anonymizes competitors)"

    run_sql "${SQL_DIR}/04_marts/mart_store_loyalty.sql" \
        "mart_store_loyalty (loyalty bands per store per category)"

    run_sql "${SQL_DIR}/04_marts/mart_store_time_patterns.sql" \
        "mart_store_time_patterns (time-of-day, day-of-week by store)"

    run_sql "${SQL_DIR}/04_marts/mart_client_segment_mix.sql" \
        "mart_client_segment_mix (per-client × category segment distribution — Option A)"

    run_sql "${SQL_DIR}/04_marts/mart_audience_catalog.sql" \
        "mart_audience_members + mart_audience_catalog (pre-packaged audiences)"

    run_sql "${SQL_DIR}/04_marts/mart_audience_client_overlap.sql" \
        "mart_audience_client_overlap (per-client audience overlap — depends on audience_members)"

    ok "Step 4 complete ($(elapsed ${STEP_START}))"
    echo ""
    echo "  ✅ Check BigQuery:"
    echo "     SELECT 'profiles' AS t, COUNT(*) AS n FROM marts.mart_cluster_profiles"
    echo "     UNION ALL SELECT 'summary', COUNT(*) FROM marts.mart_cluster_summary"
    echo "     UNION ALL SELECT 'behavioral', COUNT(*) FROM marts.mart_behavioral_summary"
    echo "     UNION ALL SELECT 'geo', COUNT(*) FROM marts.mart_geo_summary"
    echo "     UNION ALL SELECT 'churn', COUNT(*) FROM marts.mart_churn_risk"
    echo "     UNION ALL SELECT 'trends', COUNT(*) FROM marts.mart_monthly_trends"
    echo "     UNION ALL SELECT 'demographics', COUNT(*) FROM marts.mart_demographic_summary"
    echo "     UNION ALL SELECT 'benchmarks', COUNT(*) FROM marts.mart_destination_benchmarks"
    echo "     UNION ALL SELECT 'store_loyalty', COUNT(*) FROM marts.mart_store_loyalty"
    echo "     UNION ALL SELECT 'store_time', COUNT(*) FROM marts.mart_store_time_patterns"
    echo "     UNION ALL SELECT 'client_segment_mix', COUNT(*) FROM marts.mart_client_segment_mix"
    echo "     UNION ALL SELECT 'audience_members', COUNT(*) FROM marts.mart_audience_members"
    echo "     UNION ALL SELECT 'audience_catalog', COUNT(*) FROM marts.mart_audience_catalog"
    echo "     UNION ALL SELECT 'audience_client_overlap', COUNT(*) FROM marts.mart_audience_client_overlap;"
    echo ""
    if [[ "${STEP}" == "4" ]]; then
        echo "  Next: bash scripts/run.sh 5"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 5: Validation
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "5" ]]; then
    log "STEP 5: Validation"
    bash "${SCRIPT_DIR}/validate.sh" "${PROJECT_ID}"
    echo ""
    if [[ "${STEP}" == "5" ]]; then
        echo "  Next: bash scripts/run.sh [env] 6  (Looker Studio views)"
        exit 0
    fi
fi


# ══════════════════════════════════════════════════════════════
# STEP 6: Looker Studio Views
# Creates: views in marts dataset for Looker Studio
# Depends: Steps 1-4 (all mart tables must exist)
# ══════════════════════════════════════════════════════════════
if [[ "${STEP}" == "all" || "${STEP}" == "6" ]]; then
    log "STEP 6: Looker Studio views"
    STEP_START=$(date +%s)

    run_sql "${SQL_DIR}/05_looker_views/create_views.sql" \
        "Looker Studio views (original 10 views)"

    run_sql "${SQL_DIR}/05_looker_views/v_pitch_views.sql" \
        "Pitch views (v_pitch_internal + v_pitch_external for anonymization)"

    run_sql "${SQL_DIR}/05_looker_views/v_dashboard_views.sql" \
        "Dashboard views (overview, segments, churn, client pitch)"

    ok "Step 6 complete ($(elapsed ${STEP_START}))"
    echo ""
    echo "  ✅ Views created. Generate Looker Studio dashboards:"
    echo "     python scripts/looker_generator.py --all-views"
    echo "     python scripts/looker_generator.py --client Adidas --category 'Clothing & Apparel'"
    echo ""
    if [[ "${STEP}" == "6" ]]; then
        echo "  Next: python scripts/looker_generator.py"
        exit 0
    fi
fi


# ── Summary (only when running all) ──────────────────────────
if [[ "${STEP}" == "all" ]]; then
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  Pipeline complete — $(elapsed ${PIPELINE_START})"
    echo "  Project: ${PROJECT_ID}"
    echo "  Finished: $(date)"
    echo ""
    echo "  Next steps:"
    echo "    python scripts/generate_report_v3.py             # HTML/PDF report"
    echo "    python scripts/looker_generator.py --all-views   # Looker Studio"
    echo "════════════════════════════════════════════════════════════"
fi
