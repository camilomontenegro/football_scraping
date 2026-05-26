"""
scrapers/weather_scraper.py
============================
Enrich dim_match with historical weather data.

Default provider: NASA POWER (no API key, generous limits).
Optional provider: Open-Meteo archive (often rate-limited on the free tier).

Requires lat/lon in dim_stadium (populated via wikidata_stadium_enricher).
Updates dim_match directly — no separate loader needed.

Usage:
    python -m scrapers.weather_scraper
    python -m scrapers.weather_scraper --provider openmeteo --delay 2
    python -m scrapers.weather_scraper --dry-run --limit 50
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Optional

import requests
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine

log = logging.getLogger(__name__)

Provider = Literal["nasa", "openmeteo"]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = PROJECT_ROOT / "data" / ".cache" / "weather_cache.json"

NASA_POWER_HOURLY = "https://power.larc.nasa.gov/api/temporal/hourly/point"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_CUSTOMER_ARCHIVE = "https://customer-archive-api.open-meteo.com/v1/archive"

DEFAULT_KICKOFF_HOUR = 20
DEFAULT_PROVIDER = "nasa"
REQUEST_DELAY_NASA = 1.0
REQUEST_DELAY_OPENMETEO = 2.0
CHUNK_DAYS_OPENMETEO = 400
COMMIT_BATCH = 200
RETRY_429_BACKOFFS = [15, 30, 60, 120]
COOLDOWN_AFTER_429 = 300
NASA_MISSING = -999.0
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

UPDATE_SQL = text("""
    UPDATE dim_match
    SET temperature_c    = :temperature_c,
        humidity_pct     = :humidity_pct,
        precipitation_mm = :precipitation_mm,
        wind_speed_kmh   = :wind_speed_kmh,
        weather_code     = :weather_code
    WHERE match_id = :match_id
""")


def _location_key(lat: float, lon: float) -> str:
    return f"{round(lat, 4)},{round(lon, 4)}"


def _cache_key(provider: Provider, lat: float, lon: float, date_str: str) -> str:
    return f"{provider}:{_location_key(lat, lon)},{date_str}"


def _load_cache() -> dict[str, dict]:
    legacy = PROJECT_ROOT / "data" / ".cache" / "open_meteo_weather.json"
    for path in (CACHE_PATH, legacy):
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
    return {}


def _save_cache(cache: dict[str, dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _clean_numeric(value: Optional[float]) -> Optional[float]:
    if value is None or value == NASA_MISSING:
        return None
    return float(value)


def _approx_weather_code(precip_mm_h: Optional[float], humidity_pct: Optional[float]) -> int:
    p = precip_mm_h or 0.0
    if p >= 3.0:
        return 63
    if p >= 1.0:
        return 61
    if p >= 0.2:
        return 53
    if p > 0.0:
        return 51
    if humidity_pct is not None and humidity_pct >= 90:
        return 3
    if humidity_pct is not None and humidity_pct >= 75:
        return 2
    return 0


def _extract_openmeteo_for_dates(
    hourly: dict,
    dates: set[str],
    kickoff_hour: int = DEFAULT_KICKOFF_HOUR,
) -> dict[str, dict]:
    times = hourly.get("time") or []
    if not times:
        return {}

    by_date: dict[str, list[int]] = defaultdict(list)
    for idx, stamp in enumerate(times):
        day = stamp[:10]
        if day in dates:
            by_date[day].append(idx)

    result: dict[str, dict] = {}
    for day, indices in by_date.items():
        best_idx = min(indices, key=lambda i: abs(int(times[i][11:13]) - kickoff_hour))

        def _val(key: str) -> Optional[float | int]:
            arr = hourly.get(key)
            if arr and best_idx < len(arr):
                return arr[best_idx]
            return None

        result[day] = {
            "temperature_c": _val("temperature_2m"),
            "humidity_pct": _val("relative_humidity_2m"),
            "precipitation_mm": _val("precipitation"),
            "wind_speed_kmh": _val("wind_speed_10m"),
            "weather_code": _val("weather_code"),
        }
    return result


def _extract_nasa_for_dates(
    parameters: dict,
    dates: set[str],
    kickoff_hour: int = DEFAULT_KICKOFF_HOUR,
) -> dict[str, dict]:
    t2m = parameters.get("T2M", {})
    rh2m = parameters.get("RH2M", {})
    precip = parameters.get("PRECTOTCORR", {})
    ws10m = parameters.get("WS10M", {})

    result: dict[str, dict] = {}
    for day in dates:
        day_key = day.replace("-", "")
        hour_keys = [f"{day_key}{hour:02d}" for hour in range(24)]
        best_key = min(hour_keys, key=lambda key: abs(int(key[-2:]) - kickoff_hour))

        temp = _clean_numeric(t2m.get(best_key))
        if temp is None:
            continue

        humidity = _clean_numeric(rh2m.get(best_key))
        precip_mm = _clean_numeric(precip.get(best_key)) or 0.0
        wind_ms = _clean_numeric(ws10m.get(best_key))
        wind_kmh = round(wind_ms * 3.6, 1) if wind_ms is not None else None

        result[day] = {
            "temperature_c": round(temp, 1),
            "humidity_pct": int(round(humidity)) if humidity is not None else None,
            "precipitation_mm": round(precip_mm, 2),
            "wind_speed_kmh": wind_kmh,
            "weather_code": _approx_weather_code(precip_mm, humidity),
        }
    return result


def _openmeteo_base_url() -> str:
    api_key = os.getenv("OPEN_METEO_API_KEY", "").strip()
    if api_key:
        return OPEN_METEO_CUSTOMER_ARCHIVE
    return OPEN_METEO_ARCHIVE


def _429_reason(resp: requests.Response) -> str:
    try:
        return resp.json().get("reason", resp.text[:200])
    except Exception:
        return resp.text[:200]


def fetch_openmeteo_range(
    lat: float,
    lon: float,
    start: date,
    end: date,
    needed_dates: set[str],
    kickoff_hour: int = DEFAULT_KICKOFF_HOUR,
) -> tuple[dict[str, dict], bool]:
    params = {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
        "timezone": "auto",
    }
    api_key = os.getenv("OPEN_METEO_API_KEY", "").strip()
    if api_key:
        params["apikey"] = api_key

    for attempt, backoff in enumerate([0] + RETRY_429_BACKOFFS):
        if backoff:
            log.info("  429 rate-limited — waiting %ds before retry %d…", backoff, attempt)
            time.sleep(backoff)
        try:
            resp = _SESSION.get(_openmeteo_base_url(), params=params, timeout=60)
            if resp.status_code == 429:
                reason = _429_reason(resp)
                log.warning("Open-Meteo 429: %s", reason)
                if attempt < len(RETRY_429_BACKOFFS):
                    continue
                return {}, True
            resp.raise_for_status()
            hourly = resp.json().get("hourly")
            if not hourly:
                return {}, False
            return _extract_openmeteo_for_dates(hourly, needed_dates, kickoff_hour), False
        except Exception as exc:
            log.warning(
                "Open-Meteo request failed for %s..%s lat=%.4f lon=%.4f: %s",
                start, end, lat, lon, exc,
            )
            return {}, False
    return {}, False


def fetch_nasa_range(
    lat: float,
    lon: float,
    start: date,
    end: date,
    needed_dates: set[str],
    kickoff_hour: int = DEFAULT_KICKOFF_HOUR,
) -> tuple[dict[str, dict], bool]:
    params = {
        "parameters": "T2M,RH2M,PRECTOTCORR,WS10M",
        "community": "AG",
        "longitude": round(lon, 4),
        "latitude": round(lat, 4),
        "start": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "format": "JSON",
    }
    for attempt, backoff in enumerate([0, 5, 15, 30]):
        if backoff:
            log.info("  NASA POWER retry %d — waiting %ds…", attempt, backoff)
            time.sleep(backoff)
        try:
            resp = _SESSION.get(NASA_POWER_HOURLY, params=params, timeout=180)
            if resp.status_code in (429, 503):
                if attempt < 3:
                    continue
                log.warning("NASA POWER %s after retries for %s..%s", resp.status_code, start, end)
                return {}, True
            resp.raise_for_status()
            parameters = resp.json().get("properties", {}).get("parameter", {})
            if not parameters:
                return {}, False
            return _extract_nasa_for_dates(parameters, needed_dates, kickoff_hour), False
        except Exception as exc:
            log.warning(
                "NASA POWER request failed for %s..%s lat=%.4f lon=%.4f: %s",
                start, end, lat, lon, exc,
            )
            if attempt < 3:
                continue
            return {}, False
    return {}, False


def _flush_updates(batch: list[tuple[int, dict]]) -> None:
    if not batch:
        return
    with engine.begin() as conn:
        for match_id, weather in batch:
            conn.execute(UPDATE_SQL, {**weather, "match_id": match_id})


def enrich_weather(
    dry_run: bool = False,
    limit: Optional[int] = None,
    use_cache: bool = True,
    commit_batch: int = COMMIT_BATCH,
    request_delay: Optional[float] = None,
    chunk_days: int = CHUNK_DAYS_OPENMETEO,
    provider: Provider = DEFAULT_PROVIDER,
    kickoff_hour: int = DEFAULT_KICKOFF_HOUR,
) -> int:
    if request_delay is None:
        request_delay = REQUEST_DELAY_NASA if provider == "nasa" else REQUEST_DELAY_OPENMETEO

    sql = MATCHES_QUERY
    if limit:
        sql += " LIMIT :limit"

    with engine.connect() as conn:
        rows = conn.execute(
            text(sql),
            {"limit": limit} if limit else {},
        ).mappings().fetchall()

    total = len(rows)
    if total == 0:
        log.info("No matches pending weather enrichment.")
        return 0

    by_location: dict[str, dict] = defaultdict(lambda: {
        "lat": 0.0,
        "lon": 0.0,
        "matches": defaultdict(list),
    })
    for row in rows:
        lat, lon = float(row["latitude"]), float(row["longitude"])
        day = str(row["match_date"])[:10]
        loc = _location_key(lat, lon)
        entry = by_location[loc]
        entry["lat"] = lat
        entry["lon"] = lon
        entry["matches"][day].append(int(row["match_id"]))

    cache = _load_cache() if use_cache else {}
    cache_dirty = False
    api_calls = 0
    cache_hits = 0
    updates = 0
    pending_writes: list[tuple[int, dict]] = []
    fetch_range = fetch_nasa_range if provider == "nasa" else fetch_openmeteo_range

    log.info(
        "Provider: %s | %d matches across %d stadium locations.",
        provider, total, len(by_location),
    )
    if provider == "openmeteo" and not os.getenv("OPEN_METEO_API_KEY"):
        log.warning(
            "Open-Meteo free tier may return 429 if your daily quota is exhausted. "
            "Use --provider nasa (default) or set OPEN_METEO_API_KEY."
        )

    try:
        for loc_idx, (loc, entry) in enumerate(by_location.items(), start=1):
            lat, lon = entry["lat"], entry["lon"]
            dates_by_match: dict[str, list[int]] = entry["matches"]
            needed_dates = set(dates_by_match)

            missing_dates = {
                day for day in needed_dates
                if _cache_key(provider, lat, lon, day) not in cache
            }
            cache_hits += len(needed_dates) - len(missing_dates)

            if missing_dates:
                sorted_missing = sorted(_parse_date(d) for d in missing_dates)
                if provider == "nasa":
                    chunks = [(sorted_missing[0], sorted_missing[-1])]
                else:
                    chunks = _date_chunks(sorted_missing[0], sorted_missing[-1], chunk_days)

                log.info(
                    "[%d/%d] %s — fetching %d dates in %d API chunk(s)",
                    loc_idx, len(by_location), loc, len(missing_dates), len(chunks),
                )
                for chunk_start, chunk_end in chunks:
                    chunk_dates = {
                        day for day in missing_dates
                        if chunk_start <= _parse_date(day) <= chunk_end
                    }
                    if not chunk_dates:
                        continue
                    if api_calls and request_delay:
                        time.sleep(request_delay)
                    api_calls += 1
                    fetched, rate_limited = fetch_range(
                        lat, lon, chunk_start, chunk_end, chunk_dates, kickoff_hour,
                    )
                    for day, weather in fetched.items():
                        cache[_cache_key(provider, lat, lon, day)] = weather
                        cache_dirty = True
                    missing_dates -= set(fetched)
                    if rate_limited:
                        if provider == "openmeteo":
                            log.info("  cooling down %ds after rate limit…", COOLDOWN_AFTER_429)
                            time.sleep(COOLDOWN_AFTER_429)
                        break
            else:
                log.info("[%d/%d] %s — all dates already cached", loc_idx, len(by_location), loc)

            for day, match_ids in dates_by_match.items():
                weather = cache.get(_cache_key(provider, lat, lon, day))
                if not weather:
                    continue
                for match_id in match_ids:
                    updates += 1
                    if dry_run:
                        log.info("  dry-run match_id=%s date=%s %s", match_id, day, weather)
                        continue
                    pending_writes.append((match_id, weather))
                    if len(pending_writes) >= commit_batch:
                        _flush_updates(pending_writes)
                        pending_writes.clear()
                        log.info("  committed batch — %d matches updated so far", updates)

            if cache_dirty and use_cache:
                _save_cache(cache)
                cache_dirty = False

    except KeyboardInterrupt:
        log.warning("Interrupted — saving progress collected so far…")
        if not dry_run:
            _flush_updates(pending_writes)
        if use_cache and cache_dirty:
            _save_cache(cache)
        raise

    if not dry_run:
        _flush_updates(pending_writes)

    if use_cache and cache_dirty:
        _save_cache(cache)

    log.info(
        "Weather enrichment complete: %d/%d matches updated "
        "(%d API calls, %d cache hits).",
        updates, total, api_calls, cache_hits,
    )
    return updates


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Enrich dim_match with historical weather.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--provider",
        choices=("nasa", "openmeteo"),
        default=DEFAULT_PROVIDER,
        help="Weather source (default: nasa).",
    )
    parser.add_argument("--no-cache", action="store_true", help="Ignore and do not write disk cache.")
    parser.add_argument("--commit-batch", type=int, default=COMMIT_BATCH)
    parser.add_argument("--delay", type=float, default=None,
                        help="Seconds between API calls (default: 1.0 nasa, 2.0 openmeteo).")
    parser.add_argument("--chunk-days", type=int, default=CHUNK_DAYS_OPENMETEO,
                        help="Open-Meteo only: max days per request.")
    parser.add_argument("--kickoff-hour", type=int, default=DEFAULT_KICKOFF_HOUR,
                        help="Local hour used to sample hourly weather (default: 20).")
    args = parser.parse_args()

    updated = enrich_weather(
        dry_run=args.dry_run,
        limit=args.limit,
        use_cache=not args.no_cache,
        commit_batch=args.commit_batch,
        request_delay=args.delay,
        chunk_days=args.chunk_days,
        provider=args.provider,
        kickoff_hour=args.kickoff_hour,
    )
    print(f"\nDone. {updated} matches {'would be' if args.dry_run else ''} updated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
