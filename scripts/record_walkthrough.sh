#!/bin/bash
set -euo pipefail

# records an automated walkthrough of the dashboard as mp4
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

if ! command -v ffmpeg &>/dev/null; then
    echo "installing ffmpeg..."
    if command -v brew &>/dev/null; then
        brew install ffmpeg
    elif command -v apt-get &>/dev/null; then
        sudo apt-get install -y ffmpeg
    else
        echo "  cant auto-install ffmpeg, output will be .webm instead of .mp4"
    fi
fi

echo "-- step 2: starting dashboard --"

pip3 install -q -r dashboards/requirements.txt

streamlit run dashboards/app.py --server.headless true &
DASH_PID=$!

echo "  waiting for dashboard to start..."
for i in $(seq 1 60); do
    if curl -s http://localhost:8501 >/dev/null 2>&1; then
        echo "  dashboard is up"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "  dashboard didnt start in 60s, giving up"
        kill $DASH_PID 2>/dev/null
        exit 1
    fi
    sleep 1
done

# give streamlit a moment to fully initialize
sleep 5

echo "-- step 3: recording walkthrough --"

python3 scripts/record_demo.py

echo "-- step 4: stopping dashboard --"

kill $DASH_PID 2>/dev/null || true
wait $DASH_PID 2>/dev/null || true

echo "-- done --"

if [ -f demo_walkthrough.mp4 ]; then
    echo "output: $(pwd)/demo_walkthrough.mp4"
elif [ -f demo_walkthrough.webm ]; then
    echo "output: $(pwd)/demo_walkthrough.webm"
fi
