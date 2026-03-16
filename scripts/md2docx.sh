#!/bin/bash
set -euo pipefail

# md2docx - converts markdown to styled Word doc using the team template
# usage: bash scripts/md2docx.sh docs/architecture.md [output.docx]
# needs pandoc installed and docs/template.docx for branding

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TEMPLATE="${REPO_ROOT}/docs/template.docx"

# -- check dependencies --
command -v pandoc >/dev/null 2>&1 || {
    echo "Error: pandoc not found."
    echo "  Mac:   brew install pandoc"
    echo "  Linux: sudo apt install pandoc"
    exit 1
}

# -- check template --
if [[ ! -f "${TEMPLATE}" ]]; then
    echo "Error: Template not found at docs/template.docx"
    echo ""
    echo "To set up:"
    echo "  1. Get the team's branded .docx template"
    echo "  2. Save it as docs/template.docx"
    echo "  3. Run this script again"
    echo ""
    echo "The template should have the FirstRand styles:"
    echo "  - Heading 1, Heading 2, Heading 3 styles defined"
    echo "  - Headers with branding"
    echo "  - Footers with logos and brand stripe"
    echo "  - Calibri font, 12pt body text"
    exit 1
fi

# -- parse arguments --
INPUT="${1:-}"
if [[ -z "${INPUT}" ]]; then
    echo "Usage: bash scripts/md2docx.sh <input.md> [output.docx]"
    echo ""
    echo "Examples:"
    echo "  bash scripts/md2docx.sh docs/architecture.md"
    echo "  bash scripts/md2docx.sh docs/architecture.md output/arch_doc.docx"
    exit 1
fi

if [[ ! -f "${INPUT}" ]]; then
    echo "Error: Input file not found: ${INPUT}"
    exit 1
fi

# Output: use second arg, or replace .md with .docx
if [[ -n "${2:-}" ]]; then
    OUTPUT="${2}"
else
    OUTPUT="${INPUT%.md}.docx"
fi

# Ensure output directory exists
mkdir -p "$(dirname "${OUTPUT}")"

# -- convert --
echo "Converting: ${INPUT} → ${OUTPUT}"
echo "Template:   ${TEMPLATE}"

pandoc "${INPUT}" \
    --reference-doc="${TEMPLATE}" \
    --from=markdown \
    --to=docx \
    --toc \
    --toc-depth=3 \
    -o "${OUTPUT}"

echo "Done: ${OUTPUT}"
echo ""
echo "The document inherits all styles from the template:"
echo "  - Fonts, colors, spacing"
echo "  - Headers and footers (logos, brand stripe)"
echo "  - Table formatting"
echo "  - Page margins and layout"
