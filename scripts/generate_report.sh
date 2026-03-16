#!/bin/bash
set -euo pipefail

# generates the insights report (HTML + PDF)
# usage: bash scripts/generate_report.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "-- checking dependencies --"

pip3 install -q google-cloud-bigquery db-dtypes pandas

python3 -c "import playwright" 2>/dev/null || {
    echo "installing playwright..."
    pip3 install playwright
}

python3 -c "from playwright.sync_api import sync_playwright; p = sync_playwright().start(); p.chromium.launch(headless=True).close(); p.stop()" 2>/dev/null || {
    echo "installing chromium..."
    playwright install chromium
}

echo "-- generating report --"

python3 scripts/generate_report.py

echo ""
if [ -f insights_report.pdf ]; then
    echo "done: $(pwd)/insights_report.pdf"
elif [ -f insights_report.html ]; then
    echo "done: $(pwd)/insights_report.html"
fi
