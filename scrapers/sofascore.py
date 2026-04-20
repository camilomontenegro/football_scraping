from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
import json
import time


def create_driver(headless=True):
    options = Options()

    if headless:
        options.add_argument("--headless")

    options.add_argument('--disable-gpu')
    options.add_argument('--disable-images')
    options.add_argument('--disable-extensions')
    options.add_argument('--no-sandbox')
    options.page_load_strategy = 'eager'

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )


def get_json(driver, url, timeout=2):
    driver.get(url)

    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element('tag name', 'body').text.strip()) > 0
        )
    except:
        pass

    time.sleep(0.3)

    return json.loads(driver.find_element('tag name', 'body').text)


# ─────────────────────────────────────────────
# API METHODS
# ─────────────────────────────────────────────

def get_season_id(driver, tournament_id, season_name):
    data = get_json(driver, f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons")

    for s in data.get("seasons", []):
        if season_name in s["name"]:
            return s["id"], s["name"]

    return None, None


def get_matches(driver, tournament_id, season_id):
    events = []
    page = 0

    while True:
        url = f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/season/{season_id}/events/last/{page}"
        data = get_json(driver, url)

        batch = data.get("events", [])
        if not batch:
            break

        events.extend(batch)

        if not data.get("hasNextPage"):
            break

        page += 1

    return events


def get_match_shots(driver, match_id):
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/shotmap")


def get_match_events(driver, match_id):
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/incidents")


def get_match_lineups(driver, match_id):
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/lineups")


def get_player_details(driver, player_id):
    return get_json(driver, f"https://api.sofascore.com/api/v1/player/{player_id}")