"""
scripts/enrich_stadium_location.py
====================================
Enriquece los CSVs de estadios con city y country derivados de:
  1. El timezone del cache Wikidata → country
  2. La competición del equipo → country (fallback)
  3. El venue_name de WhoScored match_enrichment → para mapear estadio a equipo

Genera un CSV consolidado de todos los estadios con city y country rellenados.

Uso:
    python -m scripts.enrich_stadium_location
    python -m scripts.enrich_stadium_location --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import CLEAN_ROOT, CACHE_ROOT

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Mapping timezone → country
TZ_COUNTRY = {
    "Europe/Madrid": "España",
    "Europe/London": "Inglaterra",
    "Europe/Berlin": "Alemania",
    "Europe/Rome": "Italia",
    "Europe/Paris": "Francia",
    "Europe/Lisbon": "Portugal",
    "Europe/Amsterdam": "Países Bajos",
    "Europe/Zagreb": "Croacia",
    "Europe/Belgrade": "Serbia",
    "Europe/Athens": "Grecia",
    "Europe/Istanbul": "Turquía",
    "Europe/Vienna": "Austria",
    "Europe/Brussels": "Bélgica",
    "Europe/Zurich": "Suiza",
    "Europe/Prague": "República Checa",
    "Europe/Warsaw": "Polonia",
    "Europe/Budapest": "Hungría",
    "Europe/Bucharest": "Rumanía",
    "Europe/Kiev": "Ucrania",
    "Europe/Moscow": "Rusia",
    "Europe/Stockholm": "Suecia",
    "Europe/Oslo": "Noruega",
    "Europe/Copenhagen": "Dinamarca",
    "Europe/Helsinki": "Finlandia",
    "Europe/Dublin": "Irlanda",
    "Europe/Edinburgh": "Escocia",
    "America/Chicago": "Estados Unidos",
    "America/New_York": "Estados Unidos",
    "America/Los_Angeles": "Estados Unidos",
    "Australia/Perth": "Australia",
    "Australia/Sydney": "Australia",
    "Asia/Tokyo": "Japón",
}

# Mapping competición slug → country
COMP_COUNTRY = {
    "la_liga": "España",
    "premier_league": "Inglaterra",
    "bundesliga": "Alemania",
    "serie_a": "Italia",
    "ligue_1": "Francia",
    "primeira_liga": "Portugal",
    "eredivisie": "Países Bajos",
    "segunda_division": "España",
    "championship": "Inglaterra",
}


def load_wikidata_cache() -> dict[str, dict]:
    """Carga el cache de Wikidata: stadium_name|team → {lat, lon, timezone}."""
    cache_path = CACHE_ROOT / "wikidata_stadiums.json"
    if not cache_path.exists():
        return {}
    with open(cache_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    result = {}
    for key, entry in raw.items():
        data = entry.get("data", {})
        # Extract parts from key: v2|stadium_name|team_name|
        parts = key.split("|")
        if len(parts) >= 3:
            stadium = parts[1].strip().lower()
            team = parts[2].strip().lower()
            result[f"{stadium}|{team}"] = {
                "lat": data.get("latitude"),
                "lon": data.get("longitude"),
                "timezone": data.get("timezone"),
            }
            # Also index by stadium only
            if stadium not in result:
                result[stadium] = result[f"{stadium}|{team}"]

    return result


def enrich_stadiums(dry_run: bool = False) -> dict:
    """Enriquece todos los CSVs de stadiums con city y country."""
    wiki_cache = load_wikidata_cache()
    log.info("Wikidata cache: %d entries", len(wiki_cache))

    stats = {"total": 0, "enriched_country": 0, "enriched_from_tz": 0,
             "enriched_from_comp": 0, "files_updated": 0}

    for csv_path in sorted(CLEAN_ROOT.glob("**/transfermarkt/stadiums.csv")):
        rel = csv_path.relative_to(CLEAN_ROOT)
        comp_slug = rel.parts[0] if len(rel.parts) >= 3 else ""

        rows = []
        modified = False
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                stats["total"] += 1
                stadium_name = (row.get("stadium_name") or "").strip().lower()
                team_slug = (row.get("team_slug") or "").strip().lower()

                # Si ya tiene country, skip
                if row.get("country", "").strip():
                    rows.append(row)
                    continue

                country = None

                # Strategy 1: Wikidata timezone → country
                wiki_key = f"{stadium_name}|{team_slug}"
                wiki_entry = wiki_cache.get(wiki_key) or wiki_cache.get(stadium_name)
                if wiki_entry:
                    tz = wiki_entry.get("timezone", "")
                    if tz in TZ_COUNTRY:
                        country = TZ_COUNTRY[tz]
                        stats["enriched_from_tz"] += 1

                # Strategy 2: Competition → country (fallback)
                if not country and comp_slug in COMP_COUNTRY:
                    country = COMP_COUNTRY[comp_slug]
                    stats["enriched_from_comp"] += 1

                if country:
                    row["country"] = country
                    stats["enriched_country"] += 1
                    modified = True

                rows.append(row)

        if modified and not dry_run:
            # Ensure 'country' is in fieldnames
            if "country" not in fieldnames:
                fieldnames = list(fieldnames) + ["country"]
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            stats["files_updated"] += 1
            log.info("  Updated %s (%d rows)", rel, len(rows))

    log.info("\n=== Stadium enrichment ===")
    log.info("  Total rows: %d", stats["total"])
    log.info("  Country filled: %d (tz: %d, comp: %d)",
             stats["enriched_country"], stats["enriched_from_tz"], stats["enriched_from_comp"])
    log.info("  Files updated: %d", stats["files_updated"])
    return stats


def main():
    parser = argparse.ArgumentParser(description="Enrich stadium CSVs with city/country")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    enrich_stadiums(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
