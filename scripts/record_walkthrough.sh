#!/bin/bash
set -euo pipefail

# records an automated walkthrough of the dashboard
# usage: bash scripts/record_walkthrough.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

echo "-- step 1: checking dependencies --"

if ! command -v python3 &>/dev/null; then
    echo "python3 not found. install it first."
    exit 1
fi

python3 -c "import playwright" 2>/dev/null || {
    echo "installing playwright..."
    pip3 install playwright
}

python3 -c "from playwright.sync_api import sync_playwright; p = sync_playwright().start(); p.chromium.launch(headless=True).close(); p.stop()" 2>/dev/null || {
    echo "installing chromium browser..."
    playwright install chromium
}

echo "-- step 2: starting dashboard --"

pip3 install -q -r dashboards/requirements.txt

streamlit run dashboards/app.py --server.headless true &
DASH_PID=$!

# wait for streamlit to be ready
echo "  waiting for dashboard to start..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8501 >/dev/null 2>&1; then
        echo "  dashboard is up"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  dashboard didnt start in 30s, giving up"
        kill $DASH_PID 2>/dev/null
        exit 1
    fi
    sleep 1
done

echo "-- step 3: recording walkthrough --"

python3 scripts/record_demo.py

echo "-- step 4: stopping dashboard --"

kill $DASH_PID 2>/dev/null || true
wait $DASH_PID 2>/dev/null || true

# convert to mp4 if ffmpeg is available
if [ -f demo_walkthrough.webm ]; then
    if command -v ffmpeg &>/dev/null; then
        echo "-- step 5: converting to mp4 --"
        ffmpeg -y -i demo_walkthrough.webm -c:v libx264 -preset fast demo_walkthrough.mp4 2>/dev/null
        echo "saved: demo_walkthrough.mp4"
    else
        echo "saved: demo_walkthrough.webm"
        echo "install ffmpeg to convert to mp4: brew install ffmpeg"
    fi
else
    echo "no video file found, something went wrong"
fi

echo "-- done --"
