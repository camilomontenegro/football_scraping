"""
Debug: Ver nombres exactos de temporadas disponibles para un torneo de SofaScore.
Uso: python scratch/check_ss_seasons.py <tournament_id>
"""
import sys
import json
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from scrapers.sofascore_scraper import create_driver, get_json

tournament_id = int(sys.argv[1]) if len(sys.argv) > 1 else 3

driver = create_driver()
try:
    data = get_json(driver, f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons")
    seasons = data.get("seasons", [])
    print(f"\nTemporadas disponibles para torneo {tournament_id}:")
    for s in seasons[:10]:
        print(f"  id={s['id']:6}  name='{s['name']}'")
finally:
    driver.quit()
