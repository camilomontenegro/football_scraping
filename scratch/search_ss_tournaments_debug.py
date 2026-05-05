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
        print(f"--- Search for {query} ---")
        for res in data.get("results", []):
            if res.get("type") == "uniqueTournament":
                for t in res.get("entity", []) if "entity" in res else [res.get("entity")] if res.get("entity") else []:
                    print(t)
            print(res.get("type"), [e.get("name") for e in res.get("entities", [])] if "entities" in res else res.get("entity", {}).get("name") if "entity" in res else None)
    except Exception as e:
        print(f"Error searching {query}: {e}")

driver = create_driver()
try:
    search_tournament(driver, "Premier League")
finally:
    driver.quit()
