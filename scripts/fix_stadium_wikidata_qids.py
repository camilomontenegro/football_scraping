"""
Corrige wikidata_qid erróneos en dim_stadium (p. ej. QID de club o entidad ajena).

Validación: coordenadas cercanas, etiqueta similar al stadium_name, o tipo estadio.
Resolución: P115 del club, búsqueda por nombre, sede vía query_wikidata_by_club, manual.

    python -m scripts.fix_stadium_wikidata_qids --dry-run
    python -m scripts.fix_stadium_wikidata_qids
    python -m scripts.fix_stadium_wikidata_qids --audit-only
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import time
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    STADIUM_OVERRIDES_PATH,
    _BAD_HINTS,
    _best_claim,
    _claim_value,
    _entity_is_stadium_like,
    _fetch_entities,
    _fetch_entity,
    _search_entity_id,
    query_wikidata_by_club,
)

log = logging.getLogger(__name__)

_NAME_FILLER = {
    "stadium", "stadio", "estadio", "stade", "stadion", "arena", "ground",
    "park", "field", "de", "del", "la", "el", "of", "the", "il", "lo",
    "me", "bari", "sintetik", "city", "municipal", "home", "group",
}

# QIDs verificados manualmente (búsqueda WD + coords/capacity).
MANUAL_QID_BY_SLUG: dict[str, str] = {
    "atalanta-bergamo": "Q428200",
    "benfica-lissabon": "Q190147",
    "fc-alverca": "Q7378305",
    "fc-famalicao": "Q10278212",
    "fc-girondins-bordeaux": "Q2945071",
    "fenerbahce-istanbul": "Q519368",
    "moreirense-fc": "Q10278226",
    "cd-santa-clara": "Q3514858",
    "atletic-club-escaldes": "Q546027",
    "inter-club-d-escaldes": "Q546027",
    "fc-farul-constanta": "Q28195235",
    "fc-struga-trim-lum": "Q5591979",
    "ilves": "Q5493026",
    "kf-egnatia": "Q24915950",
    "kilmarnock": "Q1637255",
    "levski-sofia": "Q368232",
    "motherwell": "Q252696",
    "progres-niederkorn": "Q3495660",
    "rc-sporting-charleroi": "Q1147565",
    "sabah-fk": "Q48837444",
    "sp-tre-fiori": "Q28064234",
    "ss-folgore-falciano": "Q728131",
    "valmiera-fc": "Q12320948",
    "vikingur-g-ta": "Q1287265",
    "agf": "Q1961383",
    "fc-astana": "Q746559",
    "apoel-nikosia": "Q592491",
    "gd-chaves": "Q5402128",
    "afc-sunderland": "Q31969",
    "differdange-fc-03": "Q16632623",
    "dynamo-brest": "Q7309159",
    "fc-toulouse": "Q738044",
    "legia-warschau": "Q928642",
}

# QID correcto aunque coords/etiqueta en BD no coincidan con la validación automática.
QID_TRUSTED_BY_SLUG: set[str] = {
    "benfica-lissabon",
    "legia-warschau",
    "fc-toulouse",
}

# Sin entidad de estadio fiable en Wikidata.
CLEAR_QID_BY_SLUG: set[str] = {
    "kf-ballkani",
    "fc-drita-gjilan",
}


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    tokens = [t for t in s.split() if t and t not in _NAME_FILLER and len(t) > 1]
    return " ".join(tokens)


def _sim(a: str, b: str) -> float:
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    if a in b or b in a:
        return 0.85
    return SequenceMatcher(None, a, b).ratio()


def _labels(entity: dict) -> list[str]:
    return [
        (v.get("value") or "")
        for v in (entity.get("labels") or {}).values()
        if v.get("value")
    ]


def _best_label_sim(entity: dict, name: str) -> float:
    return max((_sim(label, name) for label in _labels(entity)), default=0.0)


def _bad_description(entity: dict) -> bool:
    for lang in ("en", "es", "de", "it", "fr"):
        desc = (entity.get("descriptions") or {}).get(lang, {}).get("value", "").lower()
        if desc and any(h in desc for h in _BAD_HINTS):
            return True
    return False


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p = math.pi / 180.0
    a = (
        math.sin((lat2 - lat1) * p / 2) ** 2
        + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def _coords_match(entity: dict, lat: float | None, lon: float | None, km: float = 8.0) -> bool:
    if lat is None or lon is None:
        return False
    coord = _claim_value(entity.get("claims") or {}, "P625")
    if not isinstance(coord, dict):
        return False
    try:
        return _haversine_km(
            float(lat), float(lon),
            float(coord["latitude"]), float(coord["longitude"]),
        ) <= km
    except (KeyError, TypeError, ValueError):
        return False


def qid_valid(entity: dict | None, stadium_name: str, lat: float | None, lon: float | None, *, team_slug: str = "") -> bool:
    if team_slug in QID_TRUSTED_BY_SLUG:
        return bool(entity)
    if not entity:
        return False
    if _bad_description(entity):
        return False
    if _coords_match(entity, lat, lon):
        return True
    sim = _best_label_sim(entity, stadium_name)
    if _entity_is_stadium_like(entity) and sim >= 0.35:
        return True
    return sim >= 0.55


def _venue_qid_from_club_entity(entity: dict) -> str | None:
    venue = _best_claim(entity.get("claims") or {}, "P115")
    return str(venue) if venue else None


def resolve_stadium_qid(
    *,
    team_slug: str,
    stadium_name: str,
    team: str,
    current_qid: str,
    lat: float | None,
    lon: float | None,
    entities: dict[str, dict],
) -> str | None:
    manual = MANUAL_QID_BY_SLUG.get(team_slug)

    current_ent = entities.get(current_qid)
    if current_ent:
        venue = _venue_qid_from_club_entity(current_ent)
        if venue and venue != current_qid:
            ent = entities.get(venue) or _fetch_entity(venue)
        if ent and qid_valid(ent, stadium_name, lat, lon, team_slug=team_slug):
            return venue

    for query in (stadium_name, f"{stadium_name} stadium", f"{team} stadium"):
        q = (query or "").strip()
        if not q:
            continue
        qid = _search_entity_id(q, language="en") or _search_entity_id(q, language="es")
        if not qid or qid == current_qid:
            continue
        ent = entities.get(qid) or _fetch_entity(qid)
        if ent and qid_valid(ent, stadium_name, lat, lon, team_slug=team_slug):
            return qid
        time.sleep(0.12)

    row = query_wikidata_by_club(team)
    qid = row.get("wikidata_qid")
    if qid and qid != current_qid:
        ent = entities.get(str(qid)) or _fetch_entity(str(qid))
        if ent and qid_valid(ent, stadium_name, lat, lon, team_slug=team_slug):
            return str(qid)

    if manual and manual != current_qid:
        return manual
    return None


def _load_rows():
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT s.stadium_id, s.team_slug, s.stadium_name, s.wikidata_qid,
                   s.latitude, s.longitude,
                   COALESCE(t.canonical_name, s.team_slug) AS team
            FROM dim_stadium s
            LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
            WHERE s.wikidata_qid IS NOT NULL AND TRIM(s.wikidata_qid) <> ''
        """)).mappings().all()


def audit_qids() -> tuple[list[dict], dict[str, dict]]:
    rows = _load_rows()
    qids = sorted({str(r["wikidata_qid"]).strip() for r in rows})
    entities: dict[str, dict] = {}
    for i in range(0, len(qids), 40):
        entities.update(_fetch_entities(qids[i : i + 40]))

    bad = [
        r for r in rows
        if not qid_valid(
            entities.get(str(r["wikidata_qid"]).strip()),
            r["stadium_name"] or "",
            r["latitude"],
            r["longitude"],
            team_slug=r["team_slug"],
        )
    ]
    return bad, entities


def _sync_overrides(updates: dict[str, str], dry_run: bool) -> int:
    if not STADIUM_OVERRIDES_PATH.is_file():
        return 0
    payload = json.loads(STADIUM_OVERRIDES_PATH.read_text(encoding="utf-8"))
    changed = 0
    for entry in payload.get("overrides") or []:
        slug = entry.get("match") or entry.get("team_slug")
        if not slug or slug not in updates:
            continue
        new_qid = updates[slug]
        if entry.get("wikidata_qid") != new_qid:
            if not dry_run:
                entry["wikidata_qid"] = new_qid
            changed += 1
            log.info("override %s qid -> %s", slug, new_qid)
    if changed and not dry_run:
        STADIUM_OVERRIDES_PATH.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    return changed


def apply_manual_and_clear(dry_run: bool) -> int:
    """Aplica MANUAL_QID_BY_SLUG y CLEAR_QID_BY_SLUG sin llamadas WD."""
    updated = 0
    updates: dict[str, str] = {}

    with engine.connect() as conn:
        for slug, qid in MANUAL_QID_BY_SLUG.items():
            row = conn.execute(
                text("""
                    SELECT stadium_id, wikidata_qid
                    FROM dim_stadium
                    WHERE team_slug = :slug AND COALESCE(wikidata_qid, '') <> :qid
                """),
                {"slug": slug, "qid": qid},
            ).mappings().first()
            if not row:
                continue
            updated += 1
            updates[slug] = qid
            log.info("manual qid %s: %s -> %s", slug, row["wikidata_qid"], qid)
            if not dry_run:
                with engine.begin() as tx:
                    tx.execute(
                        text("""
                            UPDATE dim_stadium
                            SET wikidata_qid = :qid, updated_at = NOW()
                            WHERE stadium_id = :id
                        """),
                        {"qid": qid, "id": row["stadium_id"]},
                    )

        for slug in CLEAR_QID_BY_SLUG:
            row = conn.execute(
                text("""
                    SELECT stadium_id, wikidata_qid
                    FROM dim_stadium
                    WHERE team_slug = :slug AND wikidata_qid IS NOT NULL
                """),
                {"slug": slug},
            ).mappings().first()
            if not row:
                continue
            updated += 1
            log.info("clear qid %s (was %s)", slug, row["wikidata_qid"])
            if not dry_run:
                with engine.begin() as tx:
                    tx.execute(
                        text("""
                            UPDATE dim_stadium
                            SET wikidata_qid = NULL, updated_at = NOW()
                            WHERE stadium_id = :id
                        """),
                        {"id": row["stadium_id"]},
                    )

    if updates:
        _sync_overrides(updates, dry_run)

    if not dry_run and updated:
        from scripts.compact_dim_stadium import backfill_hashes
        with engine.begin() as conn:
            backfill_hashes(conn, dry_run=False, force=False)
    return updated


def fix_qids(dry_run: bool) -> int:
    bad, entities = audit_qids()
    log.info("QIDs inválidos: %d", len(bad))
    if not bad:
        return 0

    updates: dict[str, str] = {}
    updated = 0
    for row in bad:
        current = str(row["wikidata_qid"]).strip()
        new_qid = resolve_stadium_qid(
            team_slug=row["team_slug"],
            stadium_name=row["stadium_name"] or "",
            team=row["team"] or "",
            current_qid=current,
            lat=row["latitude"],
            lon=row["longitude"],
            entities=entities,
        )
        if not new_qid or new_qid == current:
            log.warning("sin corrección: %s (%s) qid=%s", row["team_slug"], row["stadium_name"], current)
            continue

        new_ent = entities.get(new_qid) or _fetch_entity(new_qid)
        if new_ent:
            entities[new_qid] = new_ent
        trusted = row["team_slug"] in MANUAL_QID_BY_SLUG and MANUAL_QID_BY_SLUG[row["team_slug"]] == new_qid
        if not trusted and not qid_valid(
            new_ent, row["stadium_name"] or "", row["latitude"], row["longitude"],
            team_slug=row["team_slug"],
        ):
            log.warning("propuesta rechazada: %s %s -> %s", row["team_slug"], current, new_qid)
            continue

        updated += 1
        updates[row["team_slug"]] = new_qid
        log.info(
            "fix qid stadium_id=%s %s: %s -> %s (%s)",
            row["stadium_id"], row["team_slug"], current, new_qid, row["stadium_name"],
        )
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE dim_stadium
                        SET wikidata_qid = :qid, updated_at = NOW()
                        WHERE stadium_id = :id
                    """),
                    {"qid": new_qid, "id": row["stadium_id"]},
                )
        time.sleep(0.15)

    if updates:
        _sync_overrides(updates, dry_run)

    if not dry_run and updated:
        from scripts.compact_dim_stadium import backfill_hashes
        with engine.begin() as conn:
            backfill_hashes(conn, dry_run=False, force=False)

    return updated


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description="Corrige wikidata_qid erróneos en dim_stadium.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--audit-only", action="store_true")
    ap.add_argument(
        "--manual-only",
        action="store_true",
        help="Solo MANUAL_QID_BY_SLUG y CLEAR_QID_BY_SLUG.",
    )
    args = ap.parse_args()

    if args.audit_only:
        bad, _ = audit_qids()
        print(f"invalid_qids: {len(bad)}")
        for r in bad:
            print(r["team_slug"], r["stadium_name"], r["wikidata_qid"])
        return 0

    if args.manual_only:
        n = apply_manual_and_clear(args.dry_run)
        print(f"manual_fixed: {n}")
        return 0

    n_manual = apply_manual_and_clear(args.dry_run)
    n = fix_qids(args.dry_run)
    print(f"manual_fixed: {n_manual}  fixed: {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
