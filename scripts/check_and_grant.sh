#!/bin/bash
set -euo pipefail

# ════════════════════════════════════════════════════════════════
# Check permissions and grant domain access to Cloud Run dashboard
# ════════════════════════════════════════════════════════════════

PROJECT="fmn-sandbox"
SERVICE="fnb-nav-dashboard"
REGION="africa-south1"
DOMAIN="nav.co.za"

echo "════════════════════════════════════════"
echo "  Permission check + domain access"
echo "════════════════════════════════════════"
echo ""

# 1. Who am I?
echo "Current account:"
gcloud config get account
echo ""

# 2. What roles do I have?
echo "Your roles on ${PROJECT}:"
gcloud projects get-iam-policy "${PROJECT}" \
    --flatten="bindings[].members" \
    --filter="bindings.members:$(gcloud config get account)" \
    --format="table(bindings.role)" 2>/dev/null || echo "  Could not fetch roles"
echo ""

# 3. Try granting domain access
echo "Attempting to grant @${DOMAIN} access to ${SERVICE}..."
echo ""
gcloud run services add-iam-policy-binding "${SERVICE}" \
    --region="${REGION}" \
    --member="domain:${DOMAIN}" \
    --role="roles/run.invoker" \
    --project="${PROJECT}" \
    && echo "" && echo "  ✓ Done! Anyone with @${DOMAIN} email can now access the dashboard." \
    || echo "" && echo "  ✗ Failed. Copy the error above and send to your GCP admin."

echo ""
echo "Dashboard URL:"
gcloud run services describe "${SERVICE}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --format='value(status.url)' 2>/dev/null || echo "  Run: gcloud run services describe ${SERVICE}"
