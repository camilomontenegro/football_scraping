import sys
import json
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scrapers.sofascore_scraper import create_driver, get_json

def check_tournament(driver, t_id):
    try:
        data = get_json(driver, f"https://api.sofascore.com/api/v1/unique-tournament/{t_id}")
        t = data.get("uniqueTournament", {})
        print(f"Tournament {t_id}: {t.get('name')} ({t.get('category', {}).get('name')})")
    except Exception as e:
        print(f"Tournament {t_id}: Error - {e}")

driver = create_driver()
try:
    for t_id in [2, 3, 4, 5, 8, 35, 39]:
        check_tournament(driver, t_id)
finally:
    driver.quit()
