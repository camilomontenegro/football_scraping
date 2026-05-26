"""
Enrich dim_stadium with Wikidata stadium metadata.

The module is intentionally optional: it only calls Wikidata when invoked from
the CLI or through the pipeline --enrich-wikidata flag.

Uses the MediaWiki Action API (wbsearchentities + wbgetentities) instead of
SPARQL — much faster and less prone to read timeouts on query.wikidata.org.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import requests
from requests.exceptions import RequestException, Timeout
from sqlalchemy import text

try:
    from timezonefinder import TimezoneFinder
except Exception:  # pragma: no cover - optional dependency at import time
    TimezoneFinder = None

from loaders.common import engine
from utils.retry import retry

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = PROJECT_ROOT / "data" / ".cache" / "wikidata_stadiums.json"
CACHE_TTL_DAYS = 90
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "football-scraping-wizard/1.0 (local data enrichment; contact: local)"

_last_wikidata_request = 0.0
_MIN_REQUEST_INTERVAL = 2.5
_429_BACKOFF_SECONDS = 15.0
_STADIUM_HINTS = ("stadium", "estadio", "stade", "stadion", "arena", "ground", "venue")
_BAD_HINTS = (
    "disambiguation", "human", "person", "president", "player", "manager",
    "referee", "coach", "politician",
)
_STADIUM_INSTANCE_QIDS = {
    "Q483110",   # association football stadium
    "Q1154710",  # multi-purpose stadium
    "Q2339034",  # indoor arena
    "Q1076486",  # sports venue
    "Q811979",   # architectural structure
}
_timezone_finder = TimezoneFinder() if TimezoneFinder else None
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": USER_AGENT})


def _load_cache() -> dict[str, Any]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")


def _cache_get(cache: dict[str, Any], key: str) -> Optional[dict]:
    entry = cache.get(key)
    if not entry:
        return None
    fetched_at = entry.get("fetched_at")
    try:
        ts = datetime.fromisoformat(fetched_at)
    except Exception:
        return None
    if datetime.now(timezone.utc) - ts > timedelta(days=CACHE_TTL_DAYS):
        return None
    return entry.get("data")


def _cache_set(cache: dict[str, Any], key: str, data: dict) -> None:
    cache[key] = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }


def _rate_limit() -> None:
    global _last_wikidata_request
    elapsed = time.monotonic() - _last_wikidata_request
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _last_wikidata_request = time.monotonic()


@retry(max_attempts=4, delay=2.0, backoff=2.0)
def _api_get(params: dict) -> dict:
    _rate_limit()
    try:
        resp = _SESSION.get(
            WIKIDATA_API,
            params={**params, "format": "json"},
            timeout=(5, 25),
        )
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else _429_BACKOFF_SECONDS
            log.warning("Wikidata rate limit (429); waiting %.0fs", wait)
            time.sleep(wait)
            resp.raise_for_status()
        resp.raise_for_status()
    except Timeout as exc:
        raise Timeout(f"Wikidata API timeout for {params.get('action')}") from exc

    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(payload["error"].get("info", "Wikidata API error"))
    return payload


def _search_hits(name: str, language: str = "en", limit: int = 8) -> list[dict]:
    if not name or not name.strip():
        return []
    try:
        data = _api_get({
            "action": "wbsearchentities",
            "search": name.strip(),
            "language": language,
            "limit": limit,
            "type": "item",
        })
    except (Timeout, RequestException, RuntimeError) as exc:
        log.warning("Wikidata search failed for %r (%s): %s", name, language, exc)
        return []
    return data.get("search") or []


def _search_entity_id(name: str, language: str = "en") -> Optional[str]:
    for hit in _search_hits(name, language=language, limit=5):
        qid = hit.get("id")
        if qid and qid.startswith("Q"):
            return qid
    return None


def _score_search_hit(hit: dict) -> int:
    desc = (hit.get("description") or "").lower()
    label = (hit.get("label") or "").lower()
    score = 0
    if any(hint in desc for hint in _STADIUM_HINTS):
        score += 12
    if "football" in desc or "soccer" in desc:
        score += 4
    if any(hint in desc for hint in _BAD_HINTS):
        score -= 25
    if "wikimedia disambiguation" in desc:
        score -= 30
    if "stadio" in label or "estadio" in label or "stade" in label:
        score += 6
    return score


def _instance_of_qids(entity: dict) -> set[str]:
    claims = entity.get("claims") or {}
    values = claims.get("P31") or []
    qids: set[str] = set()
    for claim in values:
        value = claim.get("mainsnak", {}).get("datavalue", {}).get("value") or {}
        qid = value.get("id")
        if qid:
            qids.add(qid)
    return qids


def _entity_is_stadium_like(entity: dict) -> bool:
    instances = _instance_of_qids(entity)
    if instances & _STADIUM_INSTANCE_QIDS:
        return True
    labels = " ".join(
        (entity.get("labels") or {}).get(lang, {}).get("value", "")
        for lang in ("en", "es", "it")
    ).lower()
    return any(hint in labels for hint in ("stadio", "estadio", "stade", "stadium", "arena"))


def _build_search_queries(stadium_name: str, team: str = "") -> list[str]:
    name = (stadium_name or "").strip()
    club = (team or "").strip()
    queries: list[str] = []
    seen: set[str] = set()

    def add(query: str) -> None:
        q = " ".join(query.split())
        if q and q.lower() not in seen:
            seen.add(q.lower())
            queries.append(q)

    if club:
        add(f"{club} stadium")
        add(f"{club} home stadium")
    if name:
        add(name)
        if "stadio" not in name.lower() and "estadio" not in name.lower():
            add(f"Stadio {name}")
            add(f"Estadio {name}")
        if "stadium" not in name.lower():
            add(f"{name} stadium")
    return queries


def _club_search_names(club_name: str) -> list[str]:
    name = (club_name or "").strip()
    if not name:
        return []
    names = [name]
    for suffix in (" CF", " FC", " SC", " SSC", " AC", " AS", " US", " UD"):
        if name.endswith(suffix):
            names.append(name[: -len(suffix)].strip())
    return list(dict.fromkeys(names))


def _claim_value(claims: dict, prop: str) -> Optional[str]:
    values = claims.get(prop, [])
    if not values:
        return None
    mainsnak = values[0].get("mainsnak", {})
    datavalue = mainsnak.get("datavalue", {})
    value = datavalue.get("value")
    if isinstance(value, dict):
        if "latitude" in value and "longitude" in value:
            return value
        if "time" in value:
            return value["time"]
        entity = value.get("id")
        return entity
    return value


def _entity_label(entity: dict) -> Optional[str]:
    labels = entity.get("labels") or {}
    for lang in ("es", "en"):
        if lang in labels:
            return labels[lang].get("value")
    if labels:
        return next(iter(labels.values())).get("value")
    return None


def _commons_image_url(file_name: Optional[str]) -> Optional[str]:
    if not file_name:
        return None
    return "https://commons.wikimedia.org/wiki/Special:FilePath/" + quote(file_name)


def _wikipedia_url_from_entity(entity: dict, lang: str = "es") -> Optional[str]:
    sitelinks = entity.get("sitelinks") or {}
    key = f"{lang}wiki"
    title = sitelinks.get(key, {}).get("title")
    if not title:
        title = sitelinks.get("enwiki", {}).get("title")
        lang = "en"
    if not title:
        return None
    return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"


def _fetch_entities(qids: list[str]) -> dict[str, dict]:
    ids = [q for q in qids if q and q.startswith("Q")]
    if not ids:
        return {}
    try:
        data = _api_get({
            "action": "wbgetentities",
            "ids": "|".join(dict.fromkeys(ids)),
            "props": "claims|labels|sitelinks",
            "languages": "es|en",
        })
    except (Timeout, RequestException, RuntimeError) as exc:
        log.warning("Wikidata entity fetch failed for %s: %s", ids, exc)
        return {}

    entities = data.get("entities") or {}
    return {
        qid: ent for qid, ent in entities.items()
        if ent and not ent.get("missing")
    }


def _fetch_entity(qid: str) -> Optional[dict]:
    return _fetch_entities([qid]).get(qid)


def _row_from_entity(qid: str, entity: dict, related: Optional[dict[str, dict]] = None) -> dict:
    related = related or {}
    claims = entity.get("claims") or {}
    coord = _claim_value(claims, "P625")
    lat = lon = None
    if isinstance(coord, dict):
        lat = coord.get("latitude")
        lon = coord.get("longitude")

    architect_id = _claim_value(claims, "P84")
    operator_id = _claim_value(claims, "P137")
    architect = operator = None
    if architect_id:
        arch_ent = related.get(str(architect_id)) or _fetch_entity(str(architect_id))
        if arch_ent:
            architect = _entity_label(arch_ent)
    if operator_id:
        op_ent = related.get(str(operator_id)) or _fetch_entity(str(operator_id))
        if op_ent:
            operator = _entity_label(op_ent)

    image = _claim_value(claims, "P18")
    if isinstance(image, str):
        image_url = _commons_image_url(image)
    else:
        image_url = None

    return {
        "wikidata_qid": qid,
        "latitude": float(lat) if lat is not None else None,
        "longitude": float(lon) if lon is not None else None,
        "architect": architect,
        "operator": operator,
        "image_url": image_url,
        "wikipedia_url": _wikipedia_url_from_entity(entity),
    }


def query_wikidata_by_qid(qid: str) -> dict:
    if not qid or not str(qid).startswith("Q"):
        return {}
    entity = _fetch_entity(str(qid))
    if not entity:
        return {}
    claims = entity.get("claims") or {}
    related_ids = [
        str(x) for x in (
            _claim_value(claims, "P84"),
            _claim_value(claims, "P137"),
        ) if x
    ]
    related = _fetch_entities([str(qid), *related_ids])
    entity = related.get(str(qid), entity)
    return _row_from_entity(str(qid), entity, related)


def query_wikidata_by_stadium_name(name: str, team: str = "") -> dict:
    best_row: dict = {}
    best_score = -999

    for query in _build_search_queries(name, team):
        seen_qids: set[str] = set()
        for lang in ("en", "es", "it"):
            for hit in _search_hits(query, language=lang, limit=8):
                qid = hit.get("id")
                if not qid or qid in seen_qids:
                    continue
                seen_qids.add(qid)

                entity = _fetch_entity(qid)
                if not entity:
                    continue

                row = _row_from_entity(qid, entity)
                score = _score_search_hit(hit)
                if row.get("latitude") is not None and row.get("longitude") is not None:
                    score += 50
                if _entity_is_stadium_like(entity):
                    score += 8

                if score > best_score:
                    best_score = score
                    best_row = row

                if (
                    row.get("latitude") is not None
                    and row.get("longitude") is not None
                    and score >= 50
                ):
                    return row

    return best_row


def query_wikidata_by_club(club_name: str) -> dict:
    for club in _club_search_names(club_name):
        qid = (
            _search_entity_id(club, language="en")
            or _search_entity_id(club, language="es")
            or _search_entity_id(club, language="it")
        )
        if not qid:
            continue

        entity = _fetch_entity(qid)
        if not entity:
            continue

        venue_id = _claim_value(entity.get("claims") or {}, "P115")
        if not venue_id:
            continue

        venue_id = str(venue_id)
        claims_preview = _fetch_entity(venue_id)
        if not claims_preview:
            continue
        claims = claims_preview.get("claims") or {}
        related_ids = [
            str(x) for x in (
                _claim_value(claims, "P84"),
                _claim_value(claims, "P137"),
            ) if x
        ]
        related = _fetch_entities([venue_id, *related_ids])
        venue = related.get(venue_id)
        if not venue:
            continue
        row = _row_from_entity(venue_id, venue, related)
        if row.get("latitude") is not None and row.get("longitude") is not None:
            return row
    return {}


def _derive_timezone(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    if lat is None or lon is None or _timezone_finder is None:
        return None
    try:
        return _timezone_finder.timezone_at(lat=float(lat), lng=float(lon))
    except Exception:
        return None


def _derive_altitude(lat: Optional[float], lon: Optional[float]) -> Optional[int]:
    if lat is None or lon is None:
        return None
    try:
        resp = _SESSION.get(
            "https://api.open-meteo.com/v1/elevation",
            params={"latitude": lat, "longitude": lon},
            timeout=(5, 10),
        )
        if not resp.ok:
            return None
        elevation = resp.json().get("elevation", [None])[0]
        return int(round(float(elevation))) if elevation is not None else None
    except (Timeout, RequestException, ValueError, TypeError):
        return None


def resolve_stadium_coords(
    stadium_name: str,
    team: str = "",
    existing_qid: Optional[str] = None,
) -> dict:
    """Resolve stadium coordinates using club venue, search, and existing QID."""
    if existing_qid:
        row = query_wikidata_by_qid(existing_qid)
        if row.get("latitude") is not None and row.get("longitude") is not None:
            return row
        if row.get("wikidata_qid") and not row.get("latitude"):
            log.info(
                "Stored QID %s has no coordinates (%r) — re-searching",
                existing_qid, stadium_name,
            )

    if team:
        row = query_wikidata_by_club(team)
        if row.get("latitude") is not None and row.get("longitude") is not None:
            return row

    row = query_wikidata_by_stadium_name(stadium_name, team=team)
    if row.get("latitude") is not None and row.get("longitude") is not None:
        return row

    return row


def enrich_stadium(
    stadium_row: dict,
    cache: Optional[dict[str, Any]] = None,
    require_coords: bool = False,
) -> dict:
    cache = cache if cache is not None else _load_cache()
    name = stadium_row.get("stadium_name") or ""
    team = stadium_row.get("team") or stadium_row.get("team_slug") or ""
    existing_qid = stadium_row.get("wikidata_qid")
    cache_key = f"{name}|{team}|{existing_qid or ''}".lower()

    cached = _cache_get(cache, cache_key)
    if cached is not None:
        has_coords = cached.get("latitude") is not None and cached.get("longitude") is not None
        if not require_coords or has_coords:
            return cached

    try:
        data = resolve_stadium_coords(name, team=team, existing_qid=existing_qid)
    except Exception as exc:
        log.warning(
            "Wikidata enrichment failed for stadium=%r team=%r: %s",
            name, team, exc,
        )
        data = {}

    lat, lon = data.get("latitude"), data.get("longitude")
    if lat is not None and lon is not None:
        data["timezone"] = _derive_timezone(lat, lon)
        data["altitude_m"] = _derive_altitude(lat, lon)

    data = {k: v for k, v in data.items() if v not in (None, "")}
    if data:
        _cache_set(cache, cache_key, data)
    return data


MISSING_COORDS_SQL = """
    SELECT s.stadium_id, s.stadium_name, s.team_slug, s.wikidata_qid,
           COALESCE(t.canonical_name, s.team_slug) AS team,
           COUNT(m.match_id) AS blocked_matches
    FROM dim_stadium s
    LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    JOIN dim_match m ON m.home_team_id = s.canonical_team_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
      AND (s.latitude IS NULL OR s.longitude IS NULL)
    GROUP BY s.stadium_id, s.stadium_name, s.team_slug, s.wikidata_qid,
             t.canonical_name, s.team_slug
    ORDER BY blocked_matches DESC, s.stadium_name NULLS LAST
"""


def enrich_stadiums_missing_coords(
    conn_or_engine=engine,
    dry_run: bool = False,
    limit: Optional[int] = None,
    weather_gaps_only: bool = True,
) -> int:
    """Enrich stadiums missing lat/lon, prioritising weather-blocked venues."""
    sql = MISSING_COORDS_SQL if weather_gaps_only else """
        SELECT s.stadium_id, s.stadium_name, s.team_slug, s.wikidata_qid,
               COALESCE(t.canonical_name, s.team_slug) AS team,
               0 AS blocked_matches
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE COALESCE(s.is_current, TRUE) = TRUE
          AND (s.latitude IS NULL OR s.longitude IS NULL)
        ORDER BY s.stadium_name NULLS LAST
    """
    if limit:
        sql += " LIMIT :limit"

    cache = _load_cache()
    connectable = conn_or_engine
    context = (
        connectable.begin()
        if hasattr(connectable, "begin") and not hasattr(connectable, "execute")
        else None
    )

    if context is not None:
        with context as conn:
            updates = _enrich_coords_with_connection(conn, sql, cache, dry_run, limit)
    else:
        updates = _enrich_coords_with_connection(connectable, sql, cache, dry_run, limit)

    _save_cache(cache)
    return updates


def _enrich_coords_with_connection(
    conn, sql: str, cache: dict[str, Any], dry_run: bool, limit: Optional[int],
) -> int:
    rows = conn.execute(text(sql), {"limit": limit} if limit else {}).mappings().fetchall()
    total = len(rows)
    updates = 0
    coords_found = 0

    for idx, row in enumerate(rows, start=1):
        log.info(
            "Coords [%d/%d] stadium_id=%s team=%r name=%r blocked=%s qid=%s",
            idx, total, row["stadium_id"], row.get("team"),
            row.get("stadium_name"), row.get("blocked_matches"), row.get("wikidata_qid"),
        )
        try:
            data = enrich_stadium(dict(row), cache=cache, require_coords=True)
        except Exception as exc:
            log.warning("Skipping stadium_id=%s: %s", row["stadium_id"], exc)
            continue

        if not data.get("latitude") or not data.get("longitude"):
            log.warning("  no coordinates found")
            continue

        coords_found += 1
        update_cols = [
            c for c in (
                "latitude", "longitude", "timezone", "altitude_m",
                "image_url", "wikipedia_url", "wikidata_qid",
                "architect", "operator",
            )
            if data.get(c) not in (None, "")
        ]
        if not update_cols:
            continue

        updates += 1
        if dry_run:
            log.info(
                "  dry-run lat=%.4f lon=%.4f qid=%s fields=%s",
                data["latitude"], data["longitude"], data.get("wikidata_qid"), update_cols,
            )
            continue

        conn.execute(text(f"""
            UPDATE dim_stadium
            SET {", ".join(f"{col} = :{col}" for col in update_cols)},
                updated_at = NOW()
            WHERE stadium_id = :stadium_id
        """), {**data, "stadium_id": row["stadium_id"]})

    log.info(
        "Coordinate enrichment complete: %d/%d stadiums updated (%d with coords found).",
        updates, total, coords_found,
    )
    return updates


def enrich_all_stadiums(conn_or_engine=engine, dry_run: bool = False,
                        limit: Optional[int] = None) -> int:
    sql = """
        SELECT s.stadium_id, s.stadium_name, s.team_slug,
               COALESCE(t.canonical_name, s.team_slug) AS team
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE COALESCE(s.is_current, TRUE) = TRUE
          AND (s.wikidata_qid IS NULL OR s.latitude IS NULL OR s.longitude IS NULL)
        ORDER BY s.stadium_name NULLS LAST
    """
    if limit:
        sql += " LIMIT :limit"

    cache = _load_cache()
    updates = 0
    connectable = conn_or_engine
    context = (
        connectable.begin()
        if hasattr(connectable, "begin") and not hasattr(connectable, "execute")
        else None
    )

    if context is not None:
        with context as conn:
            updates = _enrich_with_connection(conn, sql, cache, dry_run, limit)
    else:
        updates = _enrich_with_connection(connectable, sql, cache, dry_run, limit)

    _save_cache(cache)
    return updates


def _enrich_with_connection(conn, sql: str, cache: dict[str, Any],
                            dry_run: bool, limit: Optional[int]) -> int:
    rows = conn.execute(text(sql), {"limit": limit} if limit else {}).mappings().fetchall()
    total = len(rows)
    updates = 0
    for idx, row in enumerate(rows, start=1):
        log.info(
            "Wikidata [%d/%d] stadium_id=%s name=%r",
            idx, total, row["stadium_id"], row.get("stadium_name"),
        )
        try:
            data = enrich_stadium(dict(row), cache=cache, require_coords=False)
        except Exception as exc:
            log.warning(
                "Skipping stadium_id=%s after enrichment error: %s",
                row["stadium_id"], exc,
            )
            continue
        if not data:
            continue
        update_cols = [
            c for c in (
                "latitude", "longitude", "timezone", "altitude_m",
                "image_url", "wikipedia_url", "wikidata_qid",
                "architect", "operator",
            )
            if data.get(c) not in (None, "")
        ]
        if not update_cols:
            continue
        updates += 1
        if dry_run:
            log.info("dry-run stadium_id=%s fields=%s", row["stadium_id"], update_cols)
            continue
        conn.execute(text(f"""
            UPDATE dim_stadium
            SET {", ".join(f"{col} = COALESCE(:{col}, {col})" for col in update_cols)},
                updated_at = NOW()
            WHERE stadium_id = :stadium_id
        """), {**data, "stadium_id": row["stadium_id"]})
    return updates


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(description="Enrich dim_stadium from Wikidata.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--missing-coords",
        action="store_true",
        help="Only stadiums missing lat/lon that block weather enrichment.",
    )
    args = parser.parse_args()

    if args.missing_coords:
        total = enrich_stadiums_missing_coords(
            engine, dry_run=args.dry_run, limit=args.limit, weather_gaps_only=True,
        )
        print(f"Stadium coords repaired: {total}" + (" (dry-run)" if args.dry_run else ""))
    else:
        total = enrich_all_stadiums(engine, dry_run=args.dry_run, limit=args.limit)
        print(f"Stadiums enriched: {total}" + (" (dry-run)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
