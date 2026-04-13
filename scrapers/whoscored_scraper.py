"""WhoScored scraper — uses Selenium (headless Chrome) to bypass anti-bot.

Extracts match incident JSON embedded in the rendered page source.
All raw data is saved to ``data/raw/whoscored/`` before any DB write.

Usage:
    python -m scrapers.whoscored_scraper --match-ids 1657100 1657101
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import re
import time
from pathlib import Path
from typing import List, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from db.models import Base, FactEvents, get_engine, session_scope
from utils.helpers import normalize_coords, parse_date
from utils.player_matcher import resolve_player

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/whoscored")
BASE_URL = "https://www.whoscored.com"


def _build_driver() -> webdriver.Chrome:
    opts = Options()
    # Run without headless first — WhoScored blocks headless Chrome reliably
    # opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    # Mask navigator.webdriver flag
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver


def _save_raw(match_id: int, data) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{match_id}.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, default=str)
    log.debug("Saved raw → %s", path)


def _extract_match_data(page_source: str) -> Optional[dict]:
    """Extract embedded matchCentreData JSON from WhoScored page source."""
    pattern = r"matchCentreData\s*=\s*(\{.+?\});\s*\n"
    match = re.search(pattern, page_source, re.DOTALL)
    if not match:
        # Fallback: try requirejs data
        pattern2 = r"matchCentreData\s*=\s*(\{.+?\})\s*;"
        match = re.search(pattern2, page_source, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        log.warning("Failed to parse matchCentreData JSON: %s", exc)
        return None


def fetch_match(driver: webdriver.Chrome, match_id: int) -> Optional[dict]:
    """Navigate to a WhoScored match centre and extract incident data."""
    url = f"{BASE_URL}/Matches/{match_id}/Live"
    log.info("Selenium GET %s", url)
    driver.get(url)
    # Wait for page to render JS content
    WebDriverWait(driver, 20).until(
        lambda d: "matchCentreData" in d.page_source or "matchHeader" in d.page_source
    )
    return _extract_match_data(driver.page_source)


def load_matches(match_ids: List[int]) -> None:
    engine = get_engine()
    Base.metadata.create_all(engine)

    driver = _build_driver()
    try:
        with session_scope() as session:
            for match_id in match_ids:
                log.info("Processing WhoScored match %s", match_id)

                try:
                    # 9.2 — navigate and extract
                    match_data = fetch_match(driver, match_id)
                except Exception as exc:
                    log.warning("Failed to fetch match %s: %s", match_id, exc)
                    time.sleep(random.uniform(2, 5))
                    continue

                if not match_data:
                    log.warning("No matchCentreData found for match %s", match_id)
                    time.sleep(random.uniform(2, 5))
                    continue

                # 9.4 — save raw before processing
                _save_raw(match_id, match_data)

                # 9.3 — rate limit between navigations
                time.sleep(random.uniform(2, 5))

                # Build player cache from playerIdNameDictionary
                player_name_map = match_data.get("playerIdNameDictionary", {})
                player_cache: dict[int, Optional[int]] = {}

                for ws_pid_str, player_name in player_name_map.items():
                    ws_pid = int(ws_pid_str)
                    source_player = {
                        "name": player_name,
                        "birth_date": None,
                        "nationality": None,
                        "position": None,
                        "source_id": ws_pid,
                        "source_system": "whoscored",
                    }
                    canonical_id = resolve_player(source_player, session, "id_whoscored")
                    player_cache[ws_pid] = canonical_id

                # 9.5 — process events
                events = match_data.get("events", [])
                for ev in events:
                    ws_pid = ev.get("playerId")
                    canonical_id = player_cache.get(ws_pid) if ws_pid else None

                    raw_x = ev.get("x")
                    raw_y = ev.get("y")
                    x_m = y_m = None
                    if raw_x is not None and raw_y is not None:
                        x_m, y_m = normalize_coords(raw_x, raw_y, "whoscored")

                    end_x = ev.get("endX")
                    end_y = ev.get("endY")
                    end_x_m = end_y_m = None
                    if end_x is not None and end_y is not None:
                        end_x_m, end_y_m = normalize_coords(end_x, end_y, "whoscored")

                    ev_type = ev.get("type", {}).get("displayName") if isinstance(ev.get("type"), dict) else str(ev.get("type", ""))
                    outcome = ev.get("outcomeType", {}).get("displayName") if isinstance(ev.get("outcomeType"), dict) else None

                    session.add(FactEvents(
                        match_id=None,
                        player_id=canonical_id,
                        event_type=ev_type,
                        minute=ev.get("minute"),
                        second=ev.get("second"),
                        x=x_m,
                        y=y_m,
                        end_x=end_x_m,
                        end_y=end_y_m,
                        outcome=outcome,
                        source="whoscored",
                    ))
    finally:
        driver.quit()

    log.info("WhoScored load complete for %d matches", len(match_ids))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape WhoScored match events via Selenium")
    parser.add_argument("--match-ids", nargs="+", type=int, required=True, help="WhoScored match IDs")
    args = parser.parse_args()
    load_matches(args.match_ids)
