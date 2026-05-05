import sys
import json
from pathlib import Path
import urllib.parse
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scrapers.sofascore_scraper import create_driver, get_json

def search_tournament(driver, query):
    try:
        url = f"https://api.sofascore.com/api/v1/search/all?q={urllib.parse.quote(query)}"
        data = get_json(driver, url)
        for res in data.get("results", []):
            if res.get("type") == "uniqueTournament":
                t = res.get("entity", {})
                if t and t.get("name") == query:
                    print(f"Match for '{query}': ID={t['id']} -> {t['name']} ({t.get('category', {}).get('name')})")
                    break
        else:
            # Try again without exact match constraint
            for res in data.get("results", []):
                if res.get("type") == "uniqueTournament":
                    t = res.get("entity", {})
                    if t:
                        print(f"Best for '{query}': ID={t['id']} -> {t['name']} ({t.get('category', {}).get('name')})")
                        break
    except Exception as e:
        print(f"Error searching {query}: {e}")

driver = create_driver()
try:
    leagues = [
        "LaLiga", "LaLiga 2", "Premier League", "Championship", 
        "Bundesliga", "Serie A", "Ligue 1", "Primeira Liga", 
        "Eredivisie", "UEFA Champions League", "UEFA Europa League", "UEFA Europa Conference League"
    ]
    for l in leagues:
        search_tournament(driver, l)
finally:
    driver.quit()
