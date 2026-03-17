#!/bin/bash
set -euo pipefail

# generates repo_context.txt with full structure and code
# usage: bash scripts/generate_context.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUT="${REPO_ROOT}/repo_context.txt"
cd "${REPO_ROOT}"

{
echo "================================================"
echo "FNB NAV Pipeline — Full Repository Context"
echo "Generated: $(date)"
echo "================================================"
echo ""

echo "================================================"
echo "DIRECTORY STRUCTURE"
echo "================================================"
echo ""
find . -type f \
    -not -path './.git/*' \
    -not -path './venv/*' \
    -not -path './__pycache__/*' \
    -not -path './node_modules/*' \
    -not -path './.DS_Store' \
    -not -name '*.pyc' \
    -not -name '*.docx' \
    -not -name '*.pdf' \
    -not -name '*.mp4' \
    -not -name '*.webm' \
    -not -name '*.png' \
    -not -name '*.jpg' \
    -not -name '*.jpeg' \
    -not -name '*.ico' \
    -not -name 'repo_context.txt' \
    | sort
echo ""

# files to include (code + config + docs)
FILES=$(find . -type f \
    \( -name '*.sql' -o -name '*.py' -o -name '*.sh' -o -name '*.tf' \
       -o -name '*.md' -o -name '*.txt' -o -name '*.yml' -o -name '*.yaml' \
       -o -name '*.json' -o -name 'Dockerfile' -o -name '.dockerignore' \
       -o -name '.gitignore' \) \
    -not -path './.git/*' \
    -not -path './venv/*' \
    -not -path './node_modules/*' \
    -not -name 'repo_context.txt' \
    | sort)

for f in $FILES; do
    # skip notebooks (too large, mostly json noise)
    if [[ "$f" == *.ipynb ]]; then
        echo "================================================"
        echo "FILE: $f (notebook — cell sources only)"
        echo "================================================"
        python3 -c "
import json, sys
with open('$f') as fh:
    nb = json.load(fh)
for i, cell in enumerate(nb.get('cells', [])):
    ctype = cell.get('cell_type', 'code')
    src = ''.join(cell.get('source', []))
    print(f'--- cell {i} ({ctype}) ---')
    print(src)
    print()
" 2>/dev/null || echo "(could not parse notebook)"
        echo ""
        continue
    fi

    echo "================================================"
    echo "FILE: $f"
    echo "================================================"
    cat "$f"
    echo ""
    echo ""
done

} > "${OUT}"

# size check
SIZE=$(wc -c < "${OUT}" | tr -d ' ')
LINES=$(wc -l < "${OUT}" | tr -d ' ')
echo "saved: repo_context.txt (${LINES} lines, $(( SIZE / 1024 ))KB)"
