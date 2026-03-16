#!/bin/bash
set -euo pipefail

# grant dashboard access to users in dashboards/viewers.txt
# bash scripts/grant_access.sh             -> sandbox
# bash scripts/grant_access.sh production  -> production

ENV="${1:-sandbox}"
case "${ENV}" in
    sandbox|dev)     PROJECT="fmn-sandbox" ;;
    production|prod) PROJECT="fmn-production" ;;
    *) echo "Usage: bash scripts/grant_access.sh [sandbox|production]"; exit 1 ;;
esac

SERVICE="fnb-nav-dashboard"
REGION="africa-south1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VIEWERS="${SCRIPT_DIR}/../dashboards/viewers.txt"

if [[ ! -f "${VIEWERS}" ]]; then
    echo "Error: ${VIEWERS} not found."
    echo "Create it with one email per line."
    exit 1
fi

echo "----------------------------------------"
echo "  Granting dashboard access"
echo "  Project: ${PROJECT}"
echo "  Service: ${SERVICE}"
echo "----------------------------------------"
echo ""

# Try public access first
echo "Attempting public access..."
gcloud run services add-iam-policy-binding "${SERVICE}" \
    --region="${REGION}" \
    --member="allUsers" \
    --role="roles/run.invoker" \
    --project="${PROJECT}" 2>/dev/null \
    && echo "  ✓ Public access granted — anyone with the URL can view" \
    && exit 0

echo "  ⚠ Public access blocked by org policy. Granting per-user access..."
echo ""

COUNT=0
while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "${line}" ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue

    # Trim whitespace
    email=$(echo "${line}" | xargs)
    [[ -z "${email}" ]] && continue

    echo "  Granting access to: ${email}"
    gcloud run services add-iam-policy-binding "${SERVICE}" \
        --region="${REGION}" \
        --member="user:${email}" \
        --role="roles/run.invoker" \
        --project="${PROJECT}" \
        --quiet 2>/dev/null \
        && echo "    ✓ Done" \
        || echo "    ✗ Failed — check email address"

    COUNT=$((COUNT + 1))
done < "${VIEWERS}"

echo ""
if [[ ${COUNT} -eq 0 ]]; then
    echo "No emails found in dashboards/viewers.txt"
    echo "Add emails (one per line) and run again."
else
    echo "----------------------------------------"
    echo "  Granted access to ${COUNT} user(s)"
    echo "  URL: $(gcloud run services describe ${SERVICE} --project ${PROJECT} --region ${REGION} --format='value(status.url)' 2>/dev/null || echo 'run: gcloud run services describe fnb-nav-dashboard')"
    echo ""
    echo "  Users will authenticate with their Google account"
    echo "  when they open the URL."
    echo "----------------------------------------"
fi
