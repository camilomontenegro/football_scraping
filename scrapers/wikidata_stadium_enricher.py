"""
Enrich dim_stadium with Wikidata stadium metadata.

The module is intentionally optional: it only calls Wikidata when invoked from
the CLI or through the pipeline --enrich-wikidata flag.

Uses the MediaWiki Action API (wbsearchentities + wbgetentities) instead of
SPARQL — much faster and less prone to read timeouts on query.wikidata.org.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import re
import time
import unicodedata
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
STADIUM_OVERRIDES_PATH = PROJECT_ROOT / "data" / "stadium_overrides.json"
CACHE_TTL_DAYS = 90
CACHE_SCHEMA_VERSION = "v3"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "football-scraping-wizard/1.0 (local data enrichment; contact: local)"

_last_wikidata_request = 0.0
_MIN_REQUEST_INTERVAL = 6.0
_429_BACKOFF_SECONDS = 65.0
_MAX_429_RETRIES = 3
_NOMINATIM_SEARCH = "https://nominatim.openstreetmap.org/search"
_last_geocode_request = 0.0
_MIN_GEOCODE_INTERVAL = 1.1
_COUNTRY_TO_ISO2 = {
    "spain": "es", "españa": "es", "espana": "es",
    "england": "gb", "united kingdom": "gb", "uk": "gb",
    "germany": "de", "deutschland": "de",
    "italy": "it", "italia": "it",
    "france": "fr",
    "portugal": "pt",
    "netherlands": "nl", "holanda": "nl",
    "belgium": "be", "belgique": "be",
    "turkey": "tr", "türkiye": "tr",
    "scotland": "gb",
    "wales": "gb",
    "switzerland": "ch", "schweiz": "ch",
    "austria": "at", "österreich": "at",
    "greece": "gr", "czech republic": "cz", "czechia": "cz",
    "poland": "pl", "denmark": "dk", "sweden": "se", "norway": "no",
    "croatia": "hr", "serbia": "rs", "romania": "ro", "ukraine": "ua",
    "israel": "il", "cyprus": "cy", "ireland": "ie",
}
_SPONSOR_PREFIXES = (
    "spotify ", "riyadh air ", "visit ", "onetime ", "civitas ",
    "allianz ", "emirates ", "etihad ", "signal iduna ",
    "chobani ", "decathlon ", "groupama ", "nordic wellness ",
    "zondacrypto ", "lotte ", "cegeka ", "bosuil", "epet ",
)
_INVALID_GEO_COUNTRIES = frozenset({
    "", "europe", "world", "international", "uefa", "global",
})
_STADIUM_HINTS = ("stadium", "estadio", "stade", "stadion", "arena", "ground", "venue")
_BAD_HINTS = (
    "disambiguation", "human", "person", "president", "player", "manager",
    "referee", "coach", "politician",
)
# Palabras de relleno que no aportan a la identidad de un estadio. Se ignoran
# al comparar el nombre buscado con la etiqueta de la entidad de Wikidata.
_NAME_FILLER = {
    "stadium", "stadio", "estadio", "stade", "stadion", "arena", "ground",
    "park", "field", "de", "del", "la", "el", "of", "the", "il", "lo",
}
# Orden de preferencia de rank de Wikidata (menor = mejor).
_RANK_ORDER = {"preferred": 0, "normal": 1, "deprecated": 2}
_END_TIME_QUALIFIER = "P582"  # marca un valor como histórico/terminado
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


@retry(max_attempts=5, delay=10.0, backoff=2.0)
def _api_get(params: dict) -> dict:
    _rate_limit()
    try:
        resp = _SESSION.get(
            WIKIDATA_API,
            params={**params, "format": "json"},
            timeout=(5, 25),
        )
        # Retry loop for 429 — backs off exponentially within this call
        for attempt_429 in range(_MAX_429_RETRIES):
            if resp.status_code != 429:
                break
            retry_after = resp.headers.get("Retry-After")
            wait = (
                float(retry_after)
                if retry_after and retry_after.replace(".", "", 1).isdigit()
                else _429_BACKOFF_SECONDS * (attempt_429 + 1)
            )
            log.warning(
                "Wikidata rate limit (429), attempt %d/%d; waiting %.0fs",
                attempt_429 + 1, _MAX_429_RETRIES, wait,
            )
            time.sleep(wait)
            global _last_wikidata_request
            _last_wikidata_request = time.monotonic()
            resp = _SESSION.get(
                WIKIDATA_API,
                params={**params, "format": "json"},
                timeout=(5, 25),
            )
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


def _extract_snak_value(mainsnak: dict):
    """Extrae el valor 'útil' de un mainsnak de Wikidata.

    - coordenadas → dict {latitude, longitude}
    - tiempo      → string ISO
    - entidad     → QID (string)
    - resto       → valor literal
    """
    datavalue = (mainsnak or {}).get("datavalue", {})
    value = datavalue.get("value")
    if isinstance(value, dict):
        if "latitude" in value and "longitude" in value:
            return value
        if "time" in value:
            return value["time"]
        return value.get("id")
    return value


def _claim_value(claims: dict, prop: str):
    """Primer valor de una propiedad (compatibilidad: P625/P84/P137…)."""
    values = claims.get(prop, [])
    if not values:
        return None
    return _extract_snak_value(values[0].get("mainsnak", {}))


def _has_end_time(claim: dict) -> bool:
    """True si el claim tiene cualificador 'end time' (P582) → valor histórico."""
    return bool((claim.get("qualifiers") or {}).get(_END_TIME_QUALIFIER))


def _best_claim(claims: dict, prop: str, *, skip_ended: bool = True):
    """Mejor valor de una propiedad respetando rank y cualificadores.

    A diferencia de ``_claim_value`` (que coge ciegamente el primero), aquí:
      1. Se descartan los claims con rank ``deprecated``.
      2. Si ``skip_ended``, se prefieren los que NO tienen 'end time' (P582);
         esto evita, p.ej., devolver un estadio antiguo en P115 (sede del club)
         o una imagen marcada como histórica en P18.
      3. Entre los candidatos restantes se elige por rank (preferred > normal).

    Es clave para que P18 (imagen) y P115 (sede) apunten al valor vigente.
    """
    values = claims.get(prop) or []
    if not values:
        return None

    candidates = [c for c in values if c.get("rank") != "deprecated"] or list(values)
    if skip_ended:
        current = [c for c in candidates if not _has_end_time(c)]
        if current:
            candidates = current

    candidates = sorted(candidates, key=lambda c: _RANK_ORDER.get(c.get("rank"), 1))
    return _extract_snak_value(candidates[0].get("mainsnak", {}))


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

    image = _best_claim(claims, "P18")
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


def _image_url_from_entity(entity: dict) -> Optional[str]:
    image = _best_claim((entity.get("claims") or {}), "P18")
    if isinstance(image, str):
        return _commons_image_url(image)
    return None


def resolve_stadium_image(
    qid: Optional[str] = None,
    team: str = "",
    stadium_name: str = "",
) -> dict:
    """Resolve stadium photo URL (P18).

    Many override QIDs point at clubs, not venues — if the entity is not
    stadium-like we follow P115 (home venue) and read P18 from the stadium.
    """
    if qid and str(qid).startswith("Q"):
        entity = _fetch_entity(str(qid))
        if entity:
            if _entity_is_stadium_like(entity):
                image_url = _image_url_from_entity(entity)
                if image_url:
                    row = _row_from_entity(str(qid), entity)
                    row["image_url"] = image_url
                    return row
            venue_id = _best_claim(entity.get("claims") or {}, "P115")
            if venue_id:
                venue_id = str(venue_id)
                venue = _fetch_entity(venue_id)
                if venue:
                    image_url = _image_url_from_entity(venue)
                    if image_url:
                        row = _row_from_entity(venue_id, venue)
                        row["image_url"] = image_url
                        return row

    if team:
        row = query_wikidata_by_club(team)
        if row.get("image_url"):
            return row

    if stadium_name or team:
        row = query_wikidata_by_stadium_name(stadium_name, team=team)
        if row.get("image_url"):
            return row

    return {}


def batch_resolve_stadium_images(
    rows: list[dict],
    *,
    chunk_size: int = 40,
) -> dict[int, dict]:
    """Batch Wikidata lookup for dim_stadium rows. Returns stadium_id -> enrichment."""
    results: dict[int, dict] = {}
    pending_fallback: list[dict] = []

    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        qids = [str(r["wikidata_qid"]) for r in chunk if r.get("wikidata_qid")]
        entities = _fetch_entities(qids)
        venue_ids: list[str] = []

        for row in chunk:
            sid = row["stadium_id"]
            qid = str(row.get("wikidata_qid") or "")
            entity = entities.get(qid)
            if not entity:
                pending_fallback.append(row)
                continue

            if _entity_is_stadium_like(entity):
                image_url = _image_url_from_entity(entity)
                if image_url:
                    results[sid] = {
                        "image_url": image_url,
                        "wikidata_qid": qid,
                        "wikipedia_url": _wikipedia_url_from_entity(entity),
                    }
                    continue

            venue_id = _best_claim(entity.get("claims") or {}, "P115")
            if venue_id:
                venue_ids.append(str(venue_id))
                row["_venue_qid"] = str(venue_id)
                pending_fallback.append(row)
            else:
                pending_fallback.append(row)

        if venue_ids:
            venues = _fetch_entities(list(dict.fromkeys(venue_ids)))
            still_pending: list[dict] = []
            for row in pending_fallback:
                if row.get("_venue_qid"):
                    venue = venues.get(row["_venue_qid"])
                    if venue:
                        image_url = _image_url_from_entity(venue)
                        if image_url:
                            results[row["stadium_id"]] = {
                                "image_url": image_url,
                                "wikidata_qid": row["_venue_qid"],
                                "wikipedia_url": _wikipedia_url_from_entity(venue),
                            }
                            continue
                still_pending.append(row)
            pending_fallback = still_pending

    for row in pending_fallback:
        sid = row["stadium_id"]
        if sid in results:
            continue
        team = row.get("team") or row.get("team_slug") or ""
        stadium_name = row.get("stadium_name") or ""
        extra = resolve_stadium_image(
            qid=row.get("wikidata_qid"),
            team=team,
            stadium_name=stadium_name,
        )
        if extra.get("image_url"):
            results[sid] = extra

    return results


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
        for lang in ("en", "es"):
            for hit in _search_hits(query, language=lang, limit=4):
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

        venue_id = _best_claim(entity.get("claims") or {}, "P115")
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


def _geocode_rate_limit() -> None:
    global _last_geocode_request
    elapsed = time.monotonic() - _last_geocode_request
    if elapsed < _MIN_GEOCODE_INTERVAL:
        time.sleep(_MIN_GEOCODE_INTERVAL - elapsed)
    _last_geocode_request = time.monotonic()


def _country_iso2(country: str) -> Optional[str]:
    key = (country or "").strip().lower()
    if not key or key in _INVALID_GEO_COUNTRIES:
        return None
    if len(key) == 2:
        return key
    return _COUNTRY_TO_ISO2.get(key)


# Keywords in team / stadium names → country for Nominatim (UCL-only teams in DB).
_TEAM_COUNTRY_PATTERNS: list[tuple[str, str]] = [
    (r"brugge|antwerp|genk|standard liège|union saint|anderlecht|mechelen|charleroi", "Belgium"),
    (r"moscow|moscú|moskva|zenit|krasnodar|lokomotiv|spartak|rubin|rostov|sochi", "Russia"),
    (r"kyiv|kiev|shakhtar|dynamo kyiv|dnipro|chornomorets", "Ukraine"),
    (r"olympiacos|paok|panathinaikos|aek athens|aris |ofi ", "Greece"),
    (r"salzburg|austria wien|rapid wien|sturm graz|lask", "Austria"),
    (r"celtic|rangers|aberdeen|hearts|hibernian", "United Kingdom"),
    (r"københavn|kobenhavn|midtjylland|nordsjælland|aalborg|brøndby", "Denmark"),
    (r"plze[nň]|sparta praha|slavia|baník|bohemians", "Czech Republic"),
    (r"haifa|tel aviv|maccabi|hapoel|beitar", "Israel"),
    (r"galatasaray|be[sş]ikta[sş]|ba[sş]ak[sş]ehir|fenerbah|trabzon|sivasspor", "Turkey"),
    (r"ferencv|debrecen|puskas|fehervar", "Hungary"),
    (r"young boys|basel|servette|lugano|zürich|zurich|sion", "Switzerland"),
    (r"malmö|malmo|göteborg|goteborg|aik |hammarby|elfsborg|häcken|hacken|norrköping", "Sweden"),
    (r"sheriff|tiraspol", "Moldova"),
    (r"crvena zvezda|partizan|red star", "Serbia"),
    (r"monaco|nice|marseille|lyon|lille|rennes|nantes|strasbourg|toulouse", "France"),
    (r"sparta rotterdam|feyenoord|ajax|psv|utrecht|twente|heerenveen|groningen", "Netherlands"),
    (r"slovan bratislava|dac dunajsk", "Slovakia"),
    (r"qarabag|baku|neftchi", "Azerbaijan"),
    (r"bodø|bodo|molde|rosenborg|brann|viking stavanger", "Norway"),
    (r"rijeka|dinamo zagreb|hajduk|osijek", "Croatia"),
    (r"rapid wien|wolfsberg", "Austria"),
    (r"nîmes|dijon|auxerre|metz|lens|reims", "France"),
    (r"sampdoria|genoa|torino|bologna|fiorentina|atalanta", "Italy"),
    (r"moreirense|famalicão|gil vicente|portimonense", "Portugal"),
    (r"celta|betis|sevilla|villarreal|getafe|osasuna", "Spain"),
    (r"legia|lech pozna|wisła|raków|czestochowa", "Poland"),
    (r"celtic|ibrox|parken", "United Kingdom"),
]


def _infer_country_from_team(team: str, slug: str = "") -> str:
    blob = f"{team} {slug}".lower()
    blob = unicodedata.normalize("NFKD", blob)
    blob = "".join(c for c in blob if not unicodedata.combining(c))
    for pattern, country in _TEAM_COUNTRY_PATTERNS:
        if re.search(pattern, blob, re.IGNORECASE):
            return country
    return ""


def _resolve_geocode_country(stadium_row: dict) -> str:
    """Pick a real country for geocoding (never 'Europe' / UEFA placeholders)."""
    for key in ("country", "domestic_country", "competition_country"):
        raw = (stadium_row.get(key) or "").strip()
        if raw and raw.lower() not in _INVALID_GEO_COUNTRIES:
            return raw
    return _infer_country_from_team(
        stadium_row.get("team") or "",
        stadium_row.get("team_slug") or "",
    )


def _simplify_stadium_name(name: str) -> str:
    lower = (name or "").strip().lower()
    for prefix in _SPONSOR_PREFIXES:
        if lower.startswith(prefix):
            return name.strip()[len(prefix):].strip()
    return name.strip()


def _nominatim_geocode(
    query: str,
    country_code: Optional[str] = None,
    *,
    structured: bool = False,
) -> Optional[dict]:
    _geocode_rate_limit()
    if structured:
        params: dict[str, Any] = {
            "q": query,
            "format": "json",
            "limit": 3,
            "class": "leisure",
        }
    else:
        params = {"q": query, "format": "json", "limit": 3}
    if country_code:
        params["countrycodes"] = country_code
    try:
        resp = _SESSION.get(_NOMINATIM_SEARCH, params=params, timeout=(5, 20))
        if not resp.ok:
            return None
        hits = resp.json()
        if not hits:
            return None
        hit = hits[0]
        for candidate in hits:
            typ = (candidate.get("type") or "").lower()
            clazz = (candidate.get("class") or "").lower()
            if typ in ("stadium", "arena", "sports_centre") or clazz in ("leisure", "building"):
                hit = candidate
                break
        lat, lon = float(hit["lat"]), float(hit["lon"])
        return {
            "latitude": lat,
            "longitude": lon,
            "timezone": _derive_timezone(lat, lon),
        }
    except (Timeout, RequestException, ValueError, TypeError, KeyError) as exc:
        log.debug("Nominatim geocode failed for %r: %s", query, exc)
        return None


# Manual fallbacks when Nominatim fails (sponsor names, Spanish TM labels, etc.).
_TEAM_STADIUM_COORDS: dict[str, tuple[float, float]] = {
    "basaksehir": (41.0941, 28.7839),
    "besiktas": (41.0392, 29.0066),
    "sheriff": (46.8378, 29.5928),
    "kobenhavn": (55.7023, 12.5723),
    "københavn": (55.7023, 12.5723),
    "galatasaray": (41.1033, 28.9912),
    "feyenoord": (51.8939, 4.5232),
    "monaco": (43.7277, 7.4194),
    "slovan bratislava": (48.1667, 17.1367),
    "qarabag": (40.3739, 49.8539),
    "nimes": (43.8333, 4.3500),
    "dijon": (47.3297, 5.0634),
    "auxerre": (47.7869, 3.5857),
    "sampdoria": (44.4064, 8.8993),
    "sparta rotterdam": (51.8934, 4.5208),
    "moreirense": (41.3622, -8.3857),
    "b-sad": (38.7526, -9.1840),
    "standard liege": (50.6092, 5.5434),
    "kaa gent": (51.0244, 3.7354),
    "cska sofia": (42.6866, 23.4171),
    "fcsb": (44.4372, 26.1526),
    "omonia": (35.1147, 33.3619),
    "hapoel beer": (31.2742, 34.7784),
    "ludogorets": (43.2186, 27.8889),
    "rfs": (56.9489, 24.1211),
    "tsc backa": (45.3500, 19.7000),
    "zorya": (47.8478, 35.1383),
    "brondby": (55.6492, 12.4175),
    "brøndby": (55.6492, 12.4175),
    "hjk helsinki": (60.2055, 24.9314),
    "aek larnaca": (34.9261, 33.5979),
    "aris limassol": (34.7006, 33.0317),
    "hamburger": (53.5874, 9.8986),
    "volkspark": (53.5874, 9.8986),
    "real oviedo": (43.3619, -5.8234),
    "carlos tartiere": (43.3619, -5.8234),
    "telstar": (52.4632, 4.6157),
    "buko": (52.4632, 4.6157),
    "st. jakob": (47.5418, 7.6203),
    "jakob-park": (47.5418, 7.6203),
    "brann stadion": (60.3119, 5.3572),
    "sk brann": (60.3119, 5.3572),
    "kairat": (43.2389, 76.8897),
    "almaty": (43.2389, 76.8897),
    "pafos": (34.7694, 32.4297),
    "kyriakidis": (34.7694, 32.4297),
    "pisa": (43.7264, 10.4024),
    "cetilar": (43.7264, 10.4024),
    "paris fc": (48.8412, 2.2530),
    "jean bouin": (48.8412, 2.2530),
    "alverca": (38.8979, -9.0352),
}

# Campos opcionales por clave (substring en team/stadium): lat/lon, wikidata_qid, image_url.
_STADIUM_OVERRIDE_EXTRAS: dict[str, dict[str, Any]] = {}

_json_overrides_loaded = False
_json_override_index: dict[str, dict[str, Any]] = {}


def _normalize_override_key(value: str) -> str:
    blob = (value or "").lower()
    blob = unicodedata.normalize("NFKD", blob)
    return "".join(c for c in blob if not unicodedata.combining(c))


def _load_json_stadium_overrides() -> None:
    """Carga data/stadium_overrides.json (editable sin tocar código)."""
    global _json_overrides_loaded, _json_override_index
    if _json_overrides_loaded:
        return
    _json_overrides_loaded = True
    _json_override_index = {}
    if not STADIUM_OVERRIDES_PATH.is_file():
        return
    try:
        payload = json.loads(STADIUM_OVERRIDES_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("No se pudo leer %s: %s", STADIUM_OVERRIDES_PATH, exc)
        return
    for entry in payload.get("overrides") or []:
        if not isinstance(entry, dict):
            continue
        keys = [
            entry.get("match"),
            entry.get("team_slug"),
            entry.get("team"),
            entry.get("stadium_name"),
        ]
        data = {
            k: entry.get(k)
            for k in (
                "latitude", "longitude", "wikidata_qid",
                "image_url", "wikipedia_url",
            )
            if entry.get(k) not in (None, "")
        }
        if not data:
            continue
        for raw_key in keys:
            norm = _normalize_override_key(str(raw_key or ""))
            if norm:
                _json_override_index[norm] = {**_json_override_index.get(norm, {}), **data}


def _coords_tuple_to_override(lat: float, lon: float) -> dict[str, Any]:
    out: dict[str, Any] = {"latitude": lat, "longitude": lon}
    tz = _derive_timezone(lat, lon)
    if tz:
        out["timezone"] = tz
    return out


def _enrich_override_from_wikidata(data: dict) -> dict:
    """Si hay QID pero falta image_url, intenta obtenerla de Wikidata."""
    qid = data.get("wikidata_qid")
    if not qid or data.get("image_url"):
        return data
    try:
        entity_row = query_wikidata_by_qid(str(qid))
        for key in ("image_url", "wikipedia_url", "latitude", "longitude"):
            if entity_row.get(key) and not data.get(key):
                data[key] = entity_row[key]
    except Exception as exc:
        log.debug("Override QID image fetch failed for %s: %s", qid, exc)
    return data


def _lookup_stadium_override(team: str, stadium_name: str) -> dict:
    """Manual coords/metadata: JSON file + dicts embebidos en este módulo."""
    _load_json_stadium_overrides()
    blob = _normalize_override_key(f"{team} {stadium_name}")

    for entry_key, fields in _json_override_index.items():
        if entry_key and entry_key in blob:
            data = dict(fields)
            lat, lon = data.get("latitude"), data.get("longitude")
            if lat is not None and lon is not None and not data.get("timezone"):
                data["timezone"] = _derive_timezone(float(lat), float(lon))
            return _enrich_override_from_wikidata(data)

    for key, extra in _STADIUM_OVERRIDE_EXTRAS.items():
        if key in blob:
            data = dict(extra)
            if "latitude" not in data and key in _TEAM_STADIUM_COORDS:
                data.update(_coords_tuple_to_override(*_TEAM_STADIUM_COORDS[key]))
            lat, lon = data.get("latitude"), data.get("longitude")
            if lat is not None and lon is not None and not data.get("timezone"):
                data["timezone"] = _derive_timezone(float(lat), float(lon))
            return _enrich_override_from_wikidata(data)

    for key, (lat, lon) in _TEAM_STADIUM_COORDS.items():
        if key in blob:
            return _enrich_override_from_wikidata(_coords_tuple_to_override(lat, lon))
    return {}


def _lookup_coord_override(team: str, stadium_name: str) -> dict:
    """Alias retrocompatible."""
    return _lookup_stadium_override(team, stadium_name)


def geocode_stadium_fallback(
    stadium_name: str,
    team: str = "",
    city: str = "",
    country: str = "",
) -> dict:
    """Resolve lat/lon via Nominatim when Wikidata is unavailable or rate-limited."""
    override = _lookup_stadium_override(team, stadium_name)
    if override.get("latitude") is not None and override.get("longitude") is not None:
        return override

    iso2 = _country_iso2(country)
    names = [stadium_name.strip()] if stadium_name else []
    simple = _simplify_stadium_name(stadium_name)
    if simple and simple not in names:
        names.append(simple)

    queries: list[str] = []
    seen_q: set[str] = set()

    def add(q: str) -> None:
        q = " ".join(q.split())
        if q and q.lower() not in seen_q:
            seen_q.add(q.lower())
            queries.append(q)

    for nm in names:
        if city and country:
            add(f"{nm}, {city}, {country}")
        if country:
            add(f"{nm}, {country}")
        if city:
            add(f"{nm}, {city}")
        if team and country:
            add(f"{nm}, {team}, {country}")
        if team:
            add(f"{team} stadium, {country}" if country else f"{team} stadium")

    for query in queries:
        row = _nominatim_geocode(query, country_code=iso2)
        if row:
            return row
        row = _nominatim_geocode(query, country_code=iso2, structured=True)
        if row:
            return row

    # Last resort: no country filter (UEFA-only teams with country='Europe').
    if iso2:
        for query in queries[:4]:
            row = _nominatim_geocode(query, country_code=None)
            if row:
                return row
    return {}


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


def _name_looks_like_club(stadium_name: str, team: str) -> bool:
    """True when stadium_name is just the club label (common bad scrape)."""
    name = (stadium_name or "").strip().lower()
    club = (team or "").strip().lower()
    if not name or not club:
        return False
    if name == club:
        return True
    for suffix in (" cf", " fc", " sc", " ssc", " ac", " as", " us", " ud"):
        if name.endswith(suffix) and name[: -len(suffix)].strip() == club:
            return True
        if club.endswith(suffix) and club[: -len(suffix)].strip() == name:
            return True
    return False


def resolve_stadium_coords(
    stadium_name: str,
    team: str = "",
    existing_qid: Optional[str] = None,
) -> dict:
    """Resolve stadium coordinates using club venue, search, and existing QID."""
    # Club home venue (P115) is the most reliable path; try it first.
    if team:
        row = query_wikidata_by_club(team)
        if row.get("latitude") is not None and row.get("longitude") is not None:
            return row

    if existing_qid:
        row = query_wikidata_by_qid(existing_qid)
        if row.get("latitude") is not None and row.get("longitude") is not None:
            return row
        if row.get("wikidata_qid") and not row.get("latitude"):
            log.info(
                "Stored QID %s has no coordinates (%r) — re-searching",
                existing_qid, stadium_name,
            )

    search_name = "" if _name_looks_like_club(stadium_name, team) else stadium_name
    row = query_wikidata_by_stadium_name(search_name, team=team)
    if row.get("latitude") is not None and row.get("longitude") is not None:
        return row

    return row


def resolve_stadium_coords_with_fallback(
    stadium_name: str,
    team: str = "",
    existing_qid: Optional[str] = None,
    city: str = "",
    country: str = "",
    *,
    use_wikidata: bool = True,
) -> dict:
    """Wikidata first; Open-Meteo geocoding if Wikidata fails or is disabled."""
    manual = _lookup_stadium_override(team, stadium_name)
    if manual.get("latitude") is not None and manual.get("longitude") is not None:
        return manual

    row: dict = {}
    if use_wikidata:
        try:
            row = resolve_stadium_coords(stadium_name, team=team, existing_qid=existing_qid)
        except Exception as exc:
            log.warning("Wikidata coords failed for %r / %r: %s", stadium_name, team, exc)
    if row.get("latitude") is not None and row.get("longitude") is not None:
        return row
    fallback = geocode_stadium_fallback(stadium_name, team=team, city=city, country=country)
    if fallback:
        log.info(
            "Nominatim geocode %r / %r -> lat=%.4f lon=%.4f",
            stadium_name, team, fallback["latitude"], fallback["longitude"],
        )
        if not fallback.get("timezone"):
            fallback["timezone"] = _derive_timezone(
                fallback["latitude"], fallback["longitude"],
            )
        return {**row, **fallback}
    return row


def enrich_stadium(
    stadium_row: dict,
    cache: Optional[dict[str, Any]] = None,
    require_coords: bool = False,
    require_image: bool = False,
    use_wikidata: bool = True,
) -> dict:
    cache = cache if cache is not None else _load_cache()
    name = stadium_row.get("stadium_name") or ""
    team = stadium_row.get("team") or stadium_row.get("team_slug") or ""
    existing_qid = stadium_row.get("wikidata_qid")
    cache_key = f"{CACHE_SCHEMA_VERSION}|{name}|{team}|{existing_qid or ''}".lower()

    cached = _cache_get(cache, cache_key)
    if cached is not None:
        has_coords = cached.get("latitude") is not None and cached.get("longitude") is not None
        has_image = bool(cached.get("image_url"))
        if (not require_coords or has_coords) and (not require_image or has_image):
            return cached

    city = stadium_row.get("city") or ""
    address = stadium_row.get("address") or ""
    if not city and address and address != stadium_row.get("stadium_name"):
        city = address
    country = _resolve_geocode_country(stadium_row)
    try:
        data = resolve_stadium_coords_with_fallback(
            name,
            team=team,
            existing_qid=existing_qid,
            city=city,
            country=country,
            use_wikidata=use_wikidata,
        )
    except Exception as exc:
        log.warning(
            "Stadium enrichment failed for stadium=%r team=%r: %s",
            name, team, exc,
        )
        data = {}

    lat, lon = data.get("latitude"), data.get("longitude")
    if lat is not None and lon is not None:
        if not data.get("timezone"):
            data["timezone"] = _derive_timezone(lat, lon)
        if use_wikidata:
            data["altitude_m"] = _derive_altitude(lat, lon)

    if use_wikidata and require_image and not data.get("image_url"):
        image_data = resolve_stadium_image(
            qid=data.get("wikidata_qid") or existing_qid,
            team=team,
            stadium_name=name,
        )
        if image_data.get("image_url"):
            data.update({
                k: v for k, v in image_data.items()
                if k in ("image_url", "wikipedia_url", "wikidata_qid") and v not in (None, "")
            })

    data = {k: v for k, v in data.items() if v not in (None, "")}
    if data:
        _cache_set(cache, cache_key, data)
    return data


MISSING_COORDS_SQL = """
    SELECT DISTINCT ON (s.canonical_team_id)
           s.stadium_id,
           s.canonical_team_id,
           s.stadium_name,
           s.address,
           s.city,
           s.country,
           s.team_slug,
           s.wikidata_qid,
           COALESCE(t.canonical_name, s.team_slug) AS team,
           sub.blocked_matches,
           sub.competition_country,
           sub.domestic_country
    FROM dim_stadium s
    LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    JOIN (
        SELECT m.home_team_id AS canonical_team_id,
               COUNT(m.match_id) AS blocked_matches,
               MODE() WITHIN GROUP (ORDER BY c.country) AS competition_country,
               (
                   SELECT c2.country
                   FROM dim_match m2
                   JOIN dim_competition c2 ON c2.canonical_id = m2.competition_id
                   WHERE m2.home_team_id = m.home_team_id
                     AND c2.country IS NOT NULL
                     AND TRIM(c2.country) <> ''
                     AND LOWER(TRIM(c2.country)) NOT IN (
                         'europe', 'world', 'international', 'uefa', 'global'
                     )
                   GROUP BY c2.country
                   ORDER BY COUNT(*) DESC
                   LIMIT 1
               ) AS domestic_country
        FROM dim_match m
        JOIN dim_competition c ON c.canonical_id = m.competition_id
        WHERE m.match_date IS NOT NULL
          AND m.temperature_c IS NULL
        GROUP BY m.home_team_id
    ) sub ON sub.canonical_team_id = s.canonical_team_id
    WHERE s.canonical_team_id IS NOT NULL
      AND (s.latitude IS NULL OR s.longitude IS NULL)
    ORDER BY s.canonical_team_id,
             (CASE WHEN LOWER(COALESCE(s.stadium_name, '')) ~ '(stadio|estadio|stade|stadion|arena|park|ground|stadium|camp nou|bernab)'
                   THEN 0 ELSE 1 END),
             (CASE WHEN LOWER(TRIM(COALESCE(s.stadium_name, ''))) =
                        LOWER(TRIM(COALESCE(t.canonical_name, s.team_slug, '')))
                   THEN 1 ELSE 0 END),
             LENGTH(COALESCE(s.stadium_name, '')) DESC,
             s.stadium_id
"""


def enrich_stadiums_missing_coords(
    conn_or_engine=engine,
    dry_run: bool = False,
    limit: Optional[int] = None,
    weather_gaps_only: bool = True,
    use_wikidata: bool = True,
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
    if hasattr(connectable, "connect") and not hasattr(connectable, "execute"):
        with connectable.connect() as conn:
            rows = conn.execute(
                text(sql), {"limit": limit} if limit else {},
            ).mappings().fetchall()
    else:
        rows = connectable.execute(
            text(sql), {"limit": limit} if limit else {},
        ).mappings().fetchall()

    updates = _enrich_coords_rows(
        rows, cache, dry_run, use_wikidata=use_wikidata, write_engine=connectable,
    )
    _save_cache(cache)
    return updates


def _propagate_coords_to_team(
    conn, canonical_team_id: int, data: dict, update_cols: list[str],
) -> int:
    """Copy coords/QID to sibling dim_stadium rows for the same home team."""
    if not canonical_team_id or not update_cols:
        return 0
    result = conn.execute(text(f"""
        UPDATE dim_stadium
        SET {", ".join(f"{col} = :{col}" for col in update_cols)},
            updated_at = NOW()
        WHERE canonical_team_id = :canonical_team_id
          AND (latitude IS NULL OR longitude IS NULL)
    """), {**data, "canonical_team_id": canonical_team_id})
    return result.rowcount or 0


def _enrich_coords_rows(
    rows: list,
    cache: dict[str, Any],
    dry_run: bool,
    *,
    use_wikidata: bool = True,
    write_engine=engine,
) -> int:
    total = len(rows)
    updates = 0
    coords_found = 0
    rows_updated = 0

    for idx, row in enumerate(rows, start=1):
        log.info(
            "Coords [%d/%d] stadium_id=%s team=%r name=%r blocked=%s qid=%s",
            idx, total, row["stadium_id"], row.get("team"),
            row.get("stadium_name"), row.get("blocked_matches"), row.get("wikidata_qid"),
        )
        try:
            data = enrich_stadium(
                dict(row), cache=cache, require_coords=True, use_wikidata=use_wikidata,
            )
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

        team_id = row.get("canonical_team_id")
        with write_engine.begin() as conn:
            rows_updated += _propagate_coords_to_team(conn, team_id, data, update_cols)

    log.info(
        "Coordinate enrichment complete: %d/%d teams with coords (%d dim_stadium rows updated).",
        updates, total, rows_updated,
    )
    return rows_updated


def _enrich_coords_with_connection(
    conn,
    sql: str,
    cache: dict[str, Any],
    dry_run: bool,
    limit: Optional[int],
    *,
    use_wikidata: bool = True,
) -> int:
    rows = conn.execute(text(sql), {"limit": limit} if limit else {}).mappings().fetchall()
    return _enrich_coords_rows(
        rows, cache, dry_run, use_wikidata=use_wikidata, write_engine=engine,
    )


def enrich_all_stadiums(conn_or_engine=engine, dry_run: bool = False,
                        limit: Optional[int] = None) -> int:
    sql = """
        SELECT s.stadium_id, s.stadium_name, s.team_slug,
               s.wikidata_qid, s.image_url,
               COALESCE(t.canonical_name, s.team_slug) AS team
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE COALESCE(s.is_current, TRUE) = TRUE
          AND (
              s.wikidata_qid IS NULL
              OR s.latitude IS NULL
              OR s.longitude IS NULL
              OR s.image_url IS NULL
              OR TRIM(s.image_url) = ''
          )
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
            require_image = not bool((row.get("image_url") or "").strip())
            data = enrich_stadium(
                dict(row),
                cache=cache,
                require_coords=False,
                require_image=require_image,
            )
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
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Seconds between Wikidata API calls (default 4.0).",
    )
    parser.add_argument(
        "--geocode-only",
        action="store_true",
        help="Skip Wikidata; use Nominatim geocoding only (~1 req/s, no 429).",
    )
    args = parser.parse_args()

    if args.delay is not None:
        global _MIN_REQUEST_INTERVAL
        _MIN_REQUEST_INTERVAL = max(1.0, float(args.delay))

    use_wikidata = not args.geocode_only

    if args.missing_coords:
        total = enrich_stadiums_missing_coords(
            engine,
            dry_run=args.dry_run,
            limit=args.limit,
            weather_gaps_only=True,
            use_wikidata=use_wikidata,
        )
        print(f"Stadium coords repaired: {total}" + (" (dry-run)" if args.dry_run else ""))
    else:
        total = enrich_all_stadiums(engine, dry_run=args.dry_run, limit=args.limit)
        print(f"Stadiums enriched: {total}" + (" (dry-run)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
