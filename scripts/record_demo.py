"""
record_demo.py
records an automated walkthrough of the streamlit dashboard as an mp4
clicks through every page, waits for data to load, scrolls slowly

requires: pip install playwright && playwright install chromium
"""

import time
import glob
import os
import shutil
import subprocess
from playwright.sync_api import sync_playwright

DASHBOARD_URL = "http://localhost:8501"
OUTPUT_FILE = "demo_walkthrough.mp4"

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

WAIT_AFTER_NAV = 8
SCROLL_PAUSE = 2
SCROLL_AMOUNT = 350
INITIAL_LOAD = 12


def wait_for_content(page):
    """wait until streamlit spinners are gone"""
    for _ in range(40):
        spinners = page.query_selector_all('[data-testid="stStatusWidget"]')
        if not spinners:
            break
        time.sleep(1)
    time.sleep(3)


def scroll_page(page):
    """scroll down slowly so viewer can see everything"""
    height = page.evaluate("document.body.scrollHeight")
    current = 0
    while current < height:
        current += SCROLL_AMOUNT
        page.evaluate(f"window.scrollTo(0, {current})")
        time.sleep(SCROLL_PAUSE)
    time.sleep(2)
    page.evaluate("window.scrollTo(0, 0)")
    time.sleep(1)


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
        page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=120000)
        print(f"  waiting for initial load...")
        time.sleep(INITIAL_LOAD)
        wait_for_content(page)

        for pg in PAGES:
            print(f"  {pg}")
            try:
                page.get_by_text(pg, exact=True).click()
                time.sleep(3)
                wait_for_content(page)
                time.sleep(WAIT_AFTER_NAV)
                scroll_page(page)
                time.sleep(2)
            except Exception as e:
                print(f"    skipped ({e})")
                continue

        print("saving video...")
        time.sleep(3)
        page.close()
        context.close()
        browser.close()

    # playwright saves webm, convert to mp4
    videos = glob.glob("*.webm")
    if not videos:
        print("no video file found")
        return

    latest = max(videos, key=lambda f: os.path.getmtime(f))
    webm_file = "demo_walkthrough.webm"
    shutil.move(latest, webm_file)

    if shutil.which("ffmpeg"):
        print("converting to mp4...")
        subprocess.run([
            "ffmpeg", "-y", "-i", webm_file,
            "-c:v", "libx264", "-preset", "fast",
            "-crf", "23", "-pix_fmt", "yuv420p",
            OUTPUT_FILE
        ], capture_output=True)
        if os.path.exists(OUTPUT_FILE):
            os.remove(webm_file)
            print(f"saved: {OUTPUT_FILE}")
        else:
            print(f"ffmpeg failed, kept: {webm_file}")
    else:
        print(f"saved: {webm_file} (install ffmpeg for mp4)")


if __name__ == "__main__":
    run()
