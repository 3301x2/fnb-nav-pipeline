#!/bin/bash
# convert_assets.sh
# Converts .eps logos to .png and prepares the assets folder
# Usage: bash scripts/convert_assets.sh
#
# Put your .eps files in the assets/ folder, then run this script.
# It converts them to PNG so the dashboard can embed them.

ASSETS_DIR="$(cd "$(dirname "$0")/../assets" && pwd)"

echo "═══════════════════════════════════════════════"
echo "  Asset Converter"
echo "  Looking in: ${ASSETS_DIR}"
echo "═══════════════════════════════════════════════"
echo ""

# Check for eps files
EPS_COUNT=$(find "${ASSETS_DIR}" -name "*.eps" -o -name "*.EPS" 2>/dev/null | wc -l)

if [ "$EPS_COUNT" -eq 0 ]; then
    echo "  No .eps files found in assets/"
    echo "  Put your .eps logo files there and run again"
    echo ""
    echo "  Supported: .eps .EPS .ai .svg"
    echo ""
    # Check if logo.png already exists
    if [ -f "${ASSETS_DIR}/logo.png" ]; then
        echo "  ✓ logo.png already exists — ready to go"
        echo "  Run: python3 scripts/generate_dashboard.py"
    fi
    exit 0
fi

echo "  Found ${EPS_COUNT} .eps file(s)"
echo ""

# Try different conversion methods
convert_eps() {
    local input="$1"
    local output="$2"
    local basename=$(basename "$input")
    
    # Method 1: sips (macOS built-in)
    if command -v sips &>/dev/null; then
        echo "  Converting ${basename} with sips..."
        sips -s format png "$input" --out "$output" 2>/dev/null
        if [ $? -eq 0 ] && [ -f "$output" ]; then
            echo "  ✓ ${basename} → $(basename "$output")"
            return 0
        fi
    fi
    
    # Method 2: ImageMagick convert
    if command -v convert &>/dev/null; then
        echo "  Converting ${basename} with ImageMagick..."
        convert -density 300 "$input" -resize 400x "$output" 2>/dev/null
        if [ $? -eq 0 ] && [ -f "$output" ]; then
            echo "  ✓ ${basename} → $(basename "$output")"
            return 0
        fi
    fi
    
    # Method 3: Ghostscript
    if command -v gs &>/dev/null; then
        echo "  Converting ${basename} with Ghostscript..."
        gs -dSAFER -dBATCH -dNOPAUSE -sDEVICE=png16m -r300 -sOutputFile="$output" "$input" 2>/dev/null
        if [ $? -eq 0 ] && [ -f "$output" ]; then
            echo "  ✓ ${basename} → $(basename "$output")"
            return 0
        fi
    fi
    
    echo "  ✗ Could not convert ${basename}"
    echo "    Try opening it in Preview → File → Export as PNG"
    echo "    Save as: assets/logo.png"
    return 1
}

# Convert each eps
for f in "${ASSETS_DIR}"/*.eps "${ASSETS_DIR}"/*.EPS; do
    [ -f "$f" ] || continue
    outname="${f%.*}.png"
    convert_eps "$f" "$outname"
done

echo ""

# If we have any png now, copy the first one as logo.png
FIRST_PNG=$(find "${ASSETS_DIR}" -name "*.png" ! -name "logo.png" | head -1)
if [ -n "$FIRST_PNG" ] && [ ! -f "${ASSETS_DIR}/logo.png" ]; then
    cp "$FIRST_PNG" "${ASSETS_DIR}/logo.png"
    echo "  ✓ Set $(basename "$FIRST_PNG") as logo.png"
fi

if [ -f "${ASSETS_DIR}/logo.png" ]; then
    echo ""
    echo "  ✓ Ready! Now run:"
    echo "    python3 scripts/generate_dashboard.py"
    echo ""
    echo "  The dashboard will use:"
    echo "    Logo: assets/logo.png"
    echo "    Colors: assets/brand.json (edit to change)"
else
    echo ""
    echo "  ✗ No PNG logo found. Convert manually:"
    echo "    Open the .eps in Preview → File → Export → PNG"
    echo "    Save to: assets/logo.png"
fi
