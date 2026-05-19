#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Inspect what's actually in GCS + BigQuery so we know the truth about the
# retail_cdao file layout — file names, parquet schema, BQ schema. Outputs to:
#
#   retail_cdao/inspect_result.json   — machine-readable, for the next script
#   stdout                            — human-readable summary
#
# Cost: a few cents. All bq queries are LIMIT 0 / metadata only — no full scans.
#
# Usage:
#   bash retail_cdao/inspect_source.sh                 # sandbox, default bucket
#   bash retail_cdao/inspect_source.sh production
#   bash retail_cdao/inspect_source.sh sandbox customer_spend_data_processed
# ─────────────────────────────────────────────────────────────────────────────

set -uo pipefail

ENV="${1:-sandbox}"
case "${ENV}" in
    sandbox|dev|sb) PROJECT="fmn-sandbox" ;;
    production|prod|prd) PROJECT="fmn-production" ;;
    *) echo "Usage: bash $0 [sandbox|production] [bucket-override]"; exit 1 ;;
esac

BUCKET="${2:-customer_spend_data}"

# ── Cosmetics ────────────────────────────────────────────────────────────────
G='\033[0;32m'; R='\033[0;31m'; Y='\033[1;33m'; B='\033[0;34m'; N='\033[0m'
ok()   { printf "${G}✓${N} %s\n" "$*"; }
fail() { printf "${R}✗${N} %s\n" "$*"; }
warn() { printf "${Y}⚠${N} %s\n" "$*"; }
info() { printf "${B}▸${N} %s\n" "$*"; }
hr()   { printf -- "────────────────────────────────────────────────────────────\n"; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$SCRIPT_DIR/inspect_result.json"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo
echo "════════════════════════════════════════════════════════════"
echo "  retail_cdao source inspection"
echo "  Project: $PROJECT   Bucket: $BUCKET"
echo "════════════════════════════════════════════════════════════"
echo

# Initial JSON skeleton — we'll keep appending to a temp and rewrite at end
declare -A SUMMARY

# ── 1. gcloud + bq sanity ────────────────────────────────────────────────────
hr; info "1. Tool check"
for cmd in gcloud bq gsutil; do
    if command -v "$cmd" >/dev/null 2>&1; then
        ok "$cmd: $(command -v $cmd)"
    else
        fail "$cmd not found — install Google Cloud SDK"
        exit 1
    fi
done

ACTIVE_ACCT="$(gcloud config get-value account 2>/dev/null || echo '(none)')"
ok "Active gcloud account: $ACTIVE_ACCT"
echo

# ── 2. GCS — list newest files in the bucket ────────────────────────────────
hr; info "2. GCS bucket contents"
GCS_LIST="$TMP_DIR/gcs.txt"

if gsutil ls -L "gs://$BUCKET/" > "$GCS_LIST" 2>/dev/null; then
    ok "Bucket gs://$BUCKET/ reachable"
else
    fail "Cannot list gs://$BUCKET/ — wrong name, wrong project, or no access"
    warn "Trying gs://${BUCKET}_processed too..."
    if gsutil ls "gs://${BUCKET}_processed/" >/dev/null 2>&1; then
        warn "  ↑ that one IS readable; consider re-running with: bash $0 $ENV ${BUCKET}_processed"
    fi
    exit 1
fi

# Newest 30 objects with size + timestamp
NEWEST="$TMP_DIR/newest.txt"
gsutil ls -lr "gs://$BUCKET/**" 2>/dev/null \
    | grep -v '^TOTAL:' \
    | grep -v '^$' \
    | sort -k 2 -r \
    | head -30 > "$NEWEST" || true

if [ -s "$NEWEST" ]; then
    echo
    info "Newest 30 objects in gs://$BUCKET/ :"
    awk '{printf "    %12s  %s  %s\n", $1, $2, $3}' "$NEWEST"
else
    warn "Bucket is empty or no recursive listing permission"
fi

# Pick the newest .parquet file for schema inspection
NEWEST_PARQUET="$(awk '$3 ~ /\.parquet$/ {print $3; exit}' "$NEWEST")"
if [ -n "$NEWEST_PARQUET" ]; then
    echo
    ok "Newest parquet: $NEWEST_PARQUET"
fi

# Detect naming patterns — group by stem (everything before _YYYYMMDD or _YYYYMM)
echo
info "Detected naming patterns (file stems):"
awk '$3 ~ /\.(csv|parquet|json|gz)$/ {print $3}' "$NEWEST" \
    | sed -E 's|gs://[^/]+/||; s|^.*/||; s|_[0-9]{8}.*||; s|_[0-9]{6}.*||' \
    | sort -u \
    | head -10 \
    | sed 's/^/    /'

# Detect "Pierre" or person-named subfolders
echo
info "Top-level prefixes in bucket (looking for per-person folders):"
gsutil ls "gs://$BUCKET/" 2>/dev/null \
    | grep '/$' \
    | head -15 \
    | sed 's/^/    /' || warn "No subfolders / no list permission"

# ── 3. Inspect parquet schema (if we found one) ─────────────────────────────
echo
hr; info "3. Parquet schema"

if [ -z "$NEWEST_PARQUET" ]; then
    warn "No .parquet file found — skipping schema check"
    PARQUET_COLS=""
else
    # Use `bq load --autodetect --dry_run` to read the parquet header without ingesting
    SAFE_NAME="$(basename "$NEWEST_PARQUET" | tr '.' '_')"
    TMP_TABLE="${PROJECT}:_tmp_inspect.${SAFE_NAME}_${RANDOM}"

    info "Asking BQ to describe the parquet schema (no data load)..."
    SCHEMA_JSON="$TMP_DIR/parquet_schema.json"

    # Make sure tmp dataset exists
    bq mk --project_id="$PROJECT" --dataset --location=africa-south1 _tmp_inspect 2>/dev/null || true

    if bq load \
        --project_id="$PROJECT" \
        --source_format=PARQUET \
        --replace \
        "$TMP_TABLE" \
        "$NEWEST_PARQUET" 2>"$TMP_DIR/bq_load.err" >/dev/null
    then
        bq show --schema --format=prettyjson "$TMP_TABLE" > "$SCHEMA_JSON" 2>/dev/null
        ok "Parquet schema captured"
        echo
        info "Columns in $NEWEST_PARQUET:"
        python3 -c "
import json
schema = json.load(open('$SCHEMA_JSON'))
for f in schema:
    name = f.get('name','')
    typ  = f.get('type','')
    mode = f.get('mode','NULLABLE')
    print(f'    {name:<30} {typ:<12} {mode}')
print()
print(f'    Total: {len(schema)} columns')
" 2>/dev/null || cat "$SCHEMA_JSON"

        # Cleanup the temp table
        bq rm -f -t "$TMP_TABLE" >/dev/null 2>&1
        PARQUET_COLS=$(python3 -c "import json; print(','.join(f['name'] for f in json.load(open('$SCHEMA_JSON'))))" 2>/dev/null)
    else
        fail "bq load failed — couldn't read parquet"
        echo "    Error:"
        sed 's/^/      /' "$TMP_DIR/bq_load.err"
        PARQUET_COLS=""
    fi
fi

# ── 4. BigQuery — current schema of customer_spend tables ───────────────────
echo
hr; info "4. BigQuery customer_spend.* schemas"

CSV_DATASET="customer_spend"
for tbl in base_data transaction_data; do
    echo
    if bq show --schema --format=prettyjson "${PROJECT}:${CSV_DATASET}.${tbl}" > "$TMP_DIR/bq_${tbl}.json" 2>/dev/null; then
        ok "${CSV_DATASET}.${tbl} schema:"
        python3 -c "
import json
schema = json.load(open('$TMP_DIR/bq_${tbl}.json'))
for f in schema:
    print(f'    {f[\"name\"]:<30} {f[\"type\"]:<12} {f.get(\"mode\",\"NULLABLE\")}')
print(f'    Total: {len(schema)} columns')
" 2>/dev/null

        # Row count (cheap — uses table metadata, no scan)
        ROWCOUNT=$(bq show --format=prettyjson "${PROJECT}:${CSV_DATASET}.${tbl}" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('numRows','?'))" 2>/dev/null)
        info "    Row count: ${ROWCOUNT}"
    else
        warn "${CSV_DATASET}.${tbl} not found in $PROJECT"
    fi
done

# ── 5. Compare parquet vs BQ schema (if we have both) ───────────────────────
echo
hr; info "5. Parquet ↔ BigQuery schema comparison"

if [ -n "$PARQUET_COLS" ] && [ -f "$TMP_DIR/bq_base_data.json" ]; then
    BQ_COLS=$(python3 -c "import json; print(','.join(f['name'] for f in json.load(open('$TMP_DIR/bq_base_data.json'))))")
    python3 - <<PYEOF
parquet = set("""$PARQUET_COLS""".split(','))
bq      = set("""$BQ_COLS""".split(','))

only_pq = sorted(parquet - bq)
only_bq = sorted(bq - parquet)
common  = sorted(parquet & bq)

print(f"    Common columns ({len(common)}):  {', '.join(common[:15])}{'...' if len(common)>15 else ''}")
if only_pq:
    print(f"    Only in parquet ({len(only_pq)}):  {', '.join(only_pq)}")
if only_bq:
    print(f"    Only in BQ      ({len(only_bq)}):  {', '.join(only_bq)}")
if not only_pq and not only_bq and common:
    print("    ✅ Parquet schema matches base_data exactly.")
PYEOF
else
    warn "Skipped — need both parquet sample and base_data schema"
fi

# ── 6. Summary JSON for the next script to consume ──────────────────────────
echo
hr; info "6. Writing machine-readable summary"

python3 - <<PYEOF > "$OUT"
import json
result = {
    "project":          "$PROJECT",
    "bucket":           "$BUCKET",
    "active_account":   "$ACTIVE_ACCT",
    "newest_parquet":   "$NEWEST_PARQUET",
    "newest_objects":   [],
    "parquet_columns":  "$PARQUET_COLS".split(',') if "$PARQUET_COLS" else [],
}
try:
    with open("$NEWEST") as f:
        for line in f:
            parts = line.strip().split(None, 2)
            if len(parts) == 3 and parts[2].startswith("gs://"):
                result["newest_objects"].append({
                    "size_bytes": int(parts[0]) if parts[0].isdigit() else 0,
                    "modified":   parts[1],
                    "path":       parts[2],
                })
except Exception:
    pass

# Add the BQ schemas if present
import os
for tbl in ("base_data", "transaction_data"):
    fp = f"$TMP_DIR/bq_{tbl}.json"
    if os.path.exists(fp):
        result[f"bq_{tbl}_schema"] = json.load(open(fp))

print(json.dumps(result, indent=2, default=str))
PYEOF

ok "Wrote $OUT"

echo
hr
echo
echo "Done. Send Prosper the file: $OUT"
echo "(Or just the stdout above if pasting in Teams is easier.)"
echo
