#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# which_bucket.sh — answers ONE question, in one screenshot:
#   Where does Pierre upload — customer_spend_data or customer_spend_data_processed?
#
# Compares the 5 newest objects in each bucket. Whichever has fresh
# burger_*.parquet / ebucks_*.parquet style files is the upload target.
#
# Usage:   bash retail_cdao/which_bucket.sh
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

PROJECT="fmn-sandbox"
B1="customer_spend_data"
B2="customer_spend_data_processed"

# Auto-login if needed (same pattern as inspect_source.sh)
if ! gcloud auth print-access-token >/dev/null 2>&1; then
    echo "Logging in to gcloud..."
    gcloud auth login --update-adc --quiet || { echo "login failed"; exit 1; }
fi

probe() {
    local bucket="$1"
    echo
    echo "═══ gs://${bucket}/ ═══"
    # Newest 5 objects, just name + size + modified date
    if ! gsutil ls -lr "gs://${bucket}/**" 2>/dev/null \
        | grep -v '^TOTAL:' \
        | grep -v '^$' \
        | sort -k 2 -r \
        | head -5 \
        | awk '{
            # convert bytes to MB
            mb = $1 / 1048576
            # strip date down to YYYY-MM-DD
            date = substr($2, 1, 10)
            # strip bucket prefix from path for readability
            n = split($3, a, "/")
            printf "  %8.1f MB  %s  %s\n", mb, date, a[n]
          }'
    then
        echo "  (no access or empty)"
    fi

    # Total object count (cheap — uses ls)
    local count
    count=$(gsutil ls "gs://${bucket}/**" 2>/dev/null | grep -c '^gs://' || echo 0)
    echo "  ── total objects: ${count}"
}

echo "════════════════════════════════════════════════════════════"
echo "  Which bucket does Pierre upload to?"
echo "  Project: ${PROJECT}"
echo "════════════════════════════════════════════════════════════"

probe "$B1"
probe "$B2"

echo
echo "════════════════════════════════════════════════════════════"
echo "  Verdict:"
echo "  - Most recent uploads + 'burger_*.parquet' files = the bucket"
echo "    Pierre writes to."
echo "  - If both have fresh files, he writes to BOTH (raw + processed)."
echo "════════════════════════════════════════════════════════════"
