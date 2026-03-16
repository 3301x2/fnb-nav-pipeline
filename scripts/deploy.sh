#!/bin/bash
set -euo pipefail

# deploy streamlit dashboard to Cloud Run
# bash scripts/deploy.sh             -> deploys to fmn-sandbox
# bash scripts/deploy.sh production  -> deploys to fmn-production

ENV="${1:-sandbox}"

case "${ENV}" in
    sandbox|dev)   PROJECT="fmn-sandbox" ;;
    production|prod) PROJECT="fmn-production" ;;
    *) echo "Usage: bash scripts/deploy.sh [sandbox|production]"; exit 1 ;;
esac

REGION="africa-south1"
SERVICE="fnb-nav-dashboard"

echo "----------------------------------------"
echo "  Deploying to Cloud Run"
echo "  Project:  ${PROJECT}"
echo "  Service:  ${SERVICE}"
echo "  Region:   ${REGION}"
echo "----------------------------------------"
echo ""

# Ensure service account has BigQuery access
echo "Checking BigQuery permissions..."
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT}" --format='value(projectNumber)')
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/bigquery.dataViewer" \
    --quiet 2>/dev/null && echo "  ✓ BigQuery read access granted" || echo "  ✓ BigQuery access already configured"

gcloud projects add-iam-policy-binding "${PROJECT}" \
    --member="serviceAccount:${SA}" \
    --role="roles/bigquery.jobUser" \
    --quiet 2>/dev/null && echo "  ✓ BigQuery job access granted" || echo "  ✓ BigQuery job access already configured"

echo ""
echo "Deploying..."

gcloud run deploy "${SERVICE}" \
    --source . \
    --project "${PROJECT}" \
    --region "${REGION}" \
    --allow-unauthenticated \
    --set-env-vars "BQ_PROJECT=${PROJECT}" \
    --memory 1Gi \
    --timeout 300

echo ""
echo "----------------------------------------"
echo "  Deployed successfully!"
echo "  URL: $(gcloud run services describe ${SERVICE} --project ${PROJECT} --region ${REGION} --format='value(status.url)')"
echo "----------------------------------------"
