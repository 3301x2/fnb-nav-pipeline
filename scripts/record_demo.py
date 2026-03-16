"""
record_demo.py
records an automated walkthrough of the streamlit dashboard as an mp4
clicks through every page, scrolls, waits for content to load

usage:
    1. start the dashboard:  streamlit run dashboards/app.py
    2. in another terminal:  python scripts/record_demo.py

output: demo_walkthrough.mp4 in the repo root

requires: pip install playwright && playwright install chromium
"""

import time
from playwright.sync_api import sync_playwright

DASHBOARD_URL = "http://localhost:8501"
OUTPUT_FILE = "demo_walkthrough.mp4"

# pages in the sidebar radio - must match the labels in app.py
PAGES = [
    "📊 Executive Summary",
    "👥 Customer Segments",
    "💰 Spend Share",
    "🧬 Demographics",
    "📈 Trends",
    "🕐 Behavioral",
    "🗺️ Geo Insights",
    "⚠️ Churn Risk",
    "📊 Benchmarks",
    "💡 ROI Simulator",
    "🤖 ML Evaluation",
    "🏥 Data Health",
]

WAIT_AFTER_NAV = 4       # seconds to wait after clicking a page
SCROLL_PAUSE = 1.5        # seconds between scrolls
SCROLL_AMOUNT = 400       # pixels per scroll step


def scroll_page(page):
    """scroll down the page slowly so the viewer can see everthing"""
    height = page.evaluate("document.body.scrollHeight")
    current = 0
    while current < height:
        current += SCROLL_AMOUNT
        page.evaluate(f"window.scrollTo(0, {current})")
        time.sleep(SCROLL_PAUSE)
    # scroll back to top
    time.sleep(0.5)
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(0.5)


def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            record_video_dir=".",
            record_video_size={"width": 1440, "height": 900},
        )
        page = context.new_page()

        print(f"opening {DASHBOARD_URL}...")
        page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=60000)
        time.sleep(5)  # let streamlit fully render

        for pg in PAGES:
            print(f"  navigating to: {pg}")
            try:
                # click the radio button for this page
                page.get_by_text(pg, exact=True).click()
                time.sleep(WAIT_AFTER_NAV)
                scroll_page(page)
                time.sleep(1)
            except Exception as e:
                print(f"    skipped ({e})")
                continue

        print("walkthrough complete, saving video...")
        time.sleep(2)
        page.close()
        context.close()
        browser.close()

    # playwright saves as webm, rename to find it
    import glob
    import shutil
    videos = glob.glob("*.webm")
    if videos:
        # take the most recent one
        latest = max(videos, key=lambda f: __import__('os').path.getmtime(f))
        shutil.move(latest, OUTPUT_FILE.replace('.mp4', '.webm'))
        print(f"\nsaved: {OUTPUT_FILE.replace('.mp4', '.webm')}")
        print("to convert to mp4 run:")
        print(f"  ffmpeg -i {OUTPUT_FILE.replace('.mp4', '.webm')} -c:v libx264 {OUTPUT_FILE}")
    else:
        print("warning: no video file found. check the current directory.")


if __name__ == "__main__":
    run()
