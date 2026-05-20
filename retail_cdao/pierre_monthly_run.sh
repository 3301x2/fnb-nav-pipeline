#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Monthly run — one command, end-to-end.
#
# What this does:
#   1. SFTP your monthly CSV from avalonwinscp
#   2. Hash PII (cust_id_reg_no, EMAIL_ADDR, CUST_CELL_NO)
#   3. CSV → Parquet (chunked, memory-safe)
#   4. Upload to gs://customer_spend_data/
#
# Easy mode (if TEST_BUCKET is set in .env, this defaults to test — safe):
#   bash pierre_monthly_run.sh                       # today's date, test bucket
#   bash pierre_monthly_run.sh --prod                # today's date, PROD bucket
#
# Specific date:
#   bash pierre_monthly_run.sh --stamp 20260512
#   bash pierre_monthly_run.sh --stamp 20260512 --prod
#
# Other options:
#   bash pierre_monthly_run.sh --stem ebucks         # different client stem
#   bash pierre_monthly_run.sh --skip-upload         # no upload at all (local only)
#   bash pierre_monthly_run.sh --yes                 # skip the "Proceed?" prompt
#
# First-time setup (do once):
#   cp .env.example .env       # then edit .env with your AD password + test bucket name
#
# Prerequisites on this machine:
#   - VPN connected
#   - Python 3 available
#   - gcloud auth application-default login  (one-time)
#   - AD_USERNAME + AD_PASSWORD in env or .env  (otherwise the script will prompt)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

G='\033[0;32m'; R='\033[0;31m'; B='\033[0;34m'; N='\033[0m'
ok()   { printf "${G}✓${N} %s\n" "$*"; }
fail() { printf "${R}✗${N} %s\n" "$*" >&2; exit 1; }
info() { printf "${B}▸${N} %s\n" "$*"; }

# ── Python interpreter ──────────────────────────────────────────────────────
if command -v python3 >/dev/null 2>&1; then
    PY=python3
elif command -v python >/dev/null 2>&1; then
    PY=python
else
    fail "No Python 3 found. Install from https://www.python.org/downloads/"
fi

PY_VER=$($PY -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
info "Python: $PY  ($PY_VER)"

# ── Install missing deps quietly ────────────────────────────────────────────
info "Checking Python packages..."
MISSING=$($PY - <<'PYEOF'
import importlib
needed = {
    "paramiko": "paramiko",
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "google.cloud.storage": "google-cloud-storage",
}
miss = []
for mod, pkg in needed.items():
    try:
        importlib.import_module(mod)
    except ImportError:
        miss.append(pkg)
print(" ".join(miss))
PYEOF
)

if [ -n "$MISSING" ]; then
    info "Installing missing packages: $MISSING"
    $PY -m pip install --quiet --upgrade pip
    $PY -m pip install --quiet $MISSING || fail "pip install failed."
fi
ok "Python packages OK"

# ── GCP auth check ──────────────────────────────────────────────────────────
if command -v gcloud >/dev/null 2>&1; then
    if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
        info "GCP ADC not set up — running 'gcloud auth application-default login'..."
        gcloud auth application-default login --quiet || fail "GCP auth failed."
    fi
    ok "GCP ADC ready"
else
    info "gcloud CLI not found — relying on GOOGLE_APPLICATION_CREDENTIALS env var."
    [ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ] || fail "Neither gcloud nor GOOGLE_APPLICATION_CREDENTIALS available."
fi

# ── Run the Python pipeline ─────────────────────────────────────────────────
info "Running pipeline..."
$PY "$SCRIPT_DIR/pierre_monthly_run.py" "$@"
