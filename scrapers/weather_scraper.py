"""
scrapers/weather_scraper.py
============================
Enrich dim_match with historical weather data from Open-Meteo Archive API.

Requires lat/lon in dim_stadium (populated via wikidata_stadium_enricher).
Updates dim_match directly — no separate loader needed.

Usage:
    python -m scrapers.weather_scraper              # all matches missing weather
    python -m scrapers.weather_scraper --limit 50   # first 50
    python -m scrapers.weather_scraper --dry-run     # preview without writing
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine

log = logging.getLogger(__name__)

OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
DEFAULT_KICKOFF_HOUR = 20
REQUEST_DELAY = 3.0
RETRY_429_BACKOFFS = [15, 30, 60]
USER_AGENT = "football-scraping-wizard/1.0 (weather enrichment)"

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": USER_AGENT})

MATCHES_QUERY = """
    SELECT m.match_id, m.match_date,
           s.latitude, s.longitude
    FROM dim_match m
    JOIN LATERAL (
        SELECT latitude, longitude
        FROM dim_stadium
        WHERE canonical_team_id = m.home_team_id
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY valid_to_season DESC
        LIMIT 1
    ) s ON TRUE
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
"""


def fetch_weather(lat: float, lon: float, date_str: str,
                  kickoff_hour: int = DEFAULT_KICKOFF_HOUR) -> Optional[dict]:
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": date_str,
        "end_date": date_str,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "auto",
    }
    for attempt, backoff in enumerate([0] + RETRY_429_BACKOFFS):
        if backoff:
            log.info("  429 rate-limited — waiting %ds before retry %d…", backoff, attempt)
            time.sleep(backoff)
        try:
            resp = _SESSION.get(OPEN_METEO_ARCHIVE, params=params, timeout=15)
            if resp.status_code == 429:
                if attempt < len(RETRY_429_BACKOFFS):
                    continue
                log.warning("Open-Meteo 429 after %d retries for %s", attempt, date_str)
                return None
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as exc:
            log.warning("Open-Meteo request failed for %s lat=%.4f lon=%.4f: %s",
                        date_str, lat, lon, exc)
            return None
    else:
        return None

    hourly = data.get("hourly")
    if not hourly or not hourly.get("time"):
        return None

    times = hourly["time"]
    best_idx = min(range(len(times)), key=lambda i: abs(int(times[i][11:13]) - kickoff_hour))

    def _val(key: str, idx: int):
        arr = hourly.get(key)
        if arr and idx < len(arr):
            return arr[idx]
        return None

    return {
        "temperature_c": _val("temperature_2m", best_idx),
        "humidity_pct": _val("relative_humidity_2m", best_idx),
        "precipitation_mm": _val("precipitation", best_idx),
        "wind_speed_kmh": _val("wind_speed_10m", best_idx),
        "weather_code": _val("weather_code", best_idx),
    }


def enrich_weather(dry_run: bool = False, limit: Optional[int] = None) -> int:
    sql = MATCHES_QUERY
    if limit:
        sql += " LIMIT :limit"

    updates = 0
    with engine.begin() as conn:
        rows = conn.execute(text(sql), {"limit": limit} if limit else {}).mappings().fetchall()
        total = len(rows)
        if total == 0:
            log.info("No matches pending weather enrichment.")
            return 0

        log.info("Found %d matches to enrich with weather data.", total)

        for idx, row in enumerate(rows, start=1):
            if idx > 1:
                time.sleep(REQUEST_DELAY)

            match_id = row["match_id"]
            date_str = str(row["match_date"])
            lat, lon = float(row["latitude"]), float(row["longitude"])

            log.info("[%d/%d] match_id=%s date=%s lat=%.4f lon=%.4f",
                     idx, total, match_id, date_str, lat, lon)

            weather = fetch_weather(lat, lon, date_str)
            if not weather:
                continue

            updates += 1
            if dry_run:
                log.info("  dry-run: %s", weather)
                continue

            conn.execute(text("""
                UPDATE dim_match
                SET temperature_c    = :temperature_c,
                    humidity_pct     = :humidity_pct,
                    precipitation_mm = :precipitation_mm,
                    wind_speed_kmh   = :wind_speed_kmh,
                    weather_code     = :weather_code
                WHERE match_id = :match_id
            """), {**weather, "match_id": match_id})

    log.info("Weather enrichment complete: %d/%d matches updated.", updates, total)
    return updates


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Enrich dim_match with Open-Meteo weather.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    updated = enrich_weather(dry_run=args.dry_run, limit=args.limit)
    print(f"\nDone. {updated} matches {'would be' if args.dry_run else ''} updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
