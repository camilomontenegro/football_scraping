"""
scripts/repair_dim_stadium.py
=============================
Repara dim_stadium tras cargas fragmentadas:

  1. Backfill canonical_team_id desde dim_team.id_transfermarkt.
  2. Corrige stadium_name desde wikipedia_url cuando TM guardó el nombre del club.
  3. Fusiona filas fragmentadas del mismo equipo (mismo nombre o mismo wikidata_qid).
  4. Recalcula data_hash y compacta adyacentes (reusa compact_dim_stadium).

Uso:
    python -m scripts.repair_dim_stadium --dry-run
    python -m scripts.repair_dim_stadium
"""

from __future__ import annotations

import argparse
import logging
import re
from urllib.parse import unquote, urlparse

from sqlalchemy import text

from loaders.common import engine
from loaders.stadium_loader import (
    address_looks_valid,
    city_looks_invalid,
    _looks_like_sponsor_label,
    resolve_canonical_team_id_by_slug,
)
from scrapers.wikidata_stadium_enricher import (
    _lookup_stadium_override,
    reverse_geocode_address,
    reverse_geocode_city,
)
from scripts.compact_dim_stadium import (
    backfill_hashes,
    merge_adjacent,
    merge_overlapping_duplicates,
)

log = logging.getLogger(__name__)

_STADIUM_WORDS = re.compile(
    r"\b(stadium|stadiums|estadio|arena|park|field|ground|stadion|stade|stadio|"
    r"metropolitano|nou|olympic|olimpico)\b",
    re.I,
)
_TEAM_PREFIX = re.compile(r"^(fc |sc |ac |cd |ud |rcd |real |sl |ss )", re.I)


def _name_score(name: str | None) -> int:
    if not name:
        return -100
    n = name.strip()
    score = 0
    if _STADIUM_WORDS.search(n):
        score += 20
    if _TEAM_PREFIX.match(n):
        score -= 15
    if "@" in n or "balompi" in n.lower():
        score -= 10
    score += min(len(n), 40) // 4
    return score


def _pick_best_name(names: list[str | None]) -> str | None:
    candidates = [n.strip() for n in names if n and str(n).strip()]
    if not candidates:
        return None
    # Descartar títulos Wikipedia erróneos (p. ej. políticos, estaciones de tren).
    bad = re.compile(r"\b(politician|railway station|fútbol\)|\(fútbol)\b", re.I)
    filtered = [n for n in candidates if not bad.search(n)]
    pool = filtered or candidates
    return max(pool, key=_name_score)


def _wikipedia_title_to_name(url: str | None) -> str | None:
    if not url:
        return None
    path = urlparse(url).path.rstrip("/")
    if not path:
        return None
    slug = unquote(path.split("/")[-1])
    if not slug or slug.lower() in ("wiki", "wikipedia"):
        return None
    slug = slug.split("#")[0]
    name = slug.replace("_", " ").strip()
    if not name or len(name) < 3:
        return None
    return name


def backfill_canonical_team_id(conn, dry_run: bool = False) -> int:
    row = conn.execute(text("""
        SELECT COUNT(*) AS n FROM dim_stadium ds
        JOIN dim_team t ON t.id_transfermarkt = ds.id_transfermarkt_team
        WHERE ds.canonical_team_id IS NULL
    """)).one()
    pending_tm = row.n
    log.info("backfill_canonical_team_id (TM id): %d filas pendientes.", pending_tm)
    if not dry_run and pending_tm:
        conn.execute(text("""
            UPDATE dim_stadium ds
            SET canonical_team_id = t.canonical_id,
                updated_at = NOW()
            FROM dim_team t
            WHERE t.id_transfermarkt = ds.id_transfermarkt_team
              AND ds.canonical_team_id IS NULL
        """))

    missing = conn.execute(text("""
        SELECT stadium_id, team_slug FROM dim_stadium
        WHERE canonical_team_id IS NULL AND team_slug IS NOT NULL
    """)).mappings().all()
    slug_fixes: list[tuple[int, int]] = []
    for r in missing:
        cid = resolve_canonical_team_id_by_slug(conn, r["team_slug"])
        if cid:
            slug_fixes.append((r["stadium_id"], cid))

    log.info("backfill_canonical_team_id (slug): %d filas pendientes.", len(slug_fixes))
    if not dry_run:
        for sid, cid in slug_fixes:
            conn.execute(text("""
                UPDATE dim_stadium
                SET canonical_team_id = :cid, updated_at = NOW()
                WHERE stadium_id = :sid
            """), {"cid": cid, "sid": sid})

    return pending_tm + len(slug_fixes)


_TEAM_ID_REMAPS: dict[str, int] = {
    "fc-basel-1893": 1529,  # partidos en dim_match usan "Basel", no "FC Basel 1893"
}


def remap_known_duplicate_teams(conn, dry_run: bool = False) -> int:
    """Reasigna canonical_team_id cuando TM y dim_match usan filas distintas en dim_team."""
    fixed = 0
    for slug, cid in _TEAM_ID_REMAPS.items():
        row = conn.execute(text("""
            SELECT stadium_id, canonical_team_id FROM dim_stadium
            WHERE team_slug = :slug LIMIT 1
        """), {"slug": slug}).fetchone()
        if not row or row[1] == cid:
            continue
        fixed += 1
        if not dry_run:
            conn.execute(text("""
                UPDATE dim_stadium
                SET canonical_team_id = :cid, updated_at = NOW()
                WHERE team_slug = :slug
            """), {"cid": cid, "slug": slug})
    log.info("remap_known_duplicate_teams: %d filas.", fixed)
    return fixed


def ensure_dim_team_for_orphan_stadiums(conn, dry_run: bool = False) -> int:
    """Crea filas mínimas en dim_team para estadios CL sin equipo enlazado."""
    rows = conn.execute(text("""
        SELECT DISTINCT team_slug, id_transfermarkt_team
        FROM dim_stadium
        WHERE canonical_team_id IS NULL
          AND team_slug IS NOT NULL
          AND id_transfermarkt_team > 0
    """)).mappings().all()

    created = 0
    for r in rows:
        slug = r["team_slug"]
        name = slug.replace("-", " ").title().replace(" Fc", " FC")
        if dry_run:
            log.info("ensure_dim_team: crear %r para slug=%s", name, slug)
            created += 1
            continue
        cid = conn.execute(
            text("INSERT INTO dim_team (canonical_name) VALUES (:n) RETURNING canonical_id"),
            {"n": name},
        ).scalar()
        conn.execute(text("""
            UPDATE dim_stadium
            SET canonical_team_id = :cid, updated_at = NOW()
            WHERE team_slug = :slug AND canonical_team_id IS NULL
        """), {"cid": cid, "slug": slug})
        created += 1
    log.info("ensure_dim_team_for_orphan_stadiums: %d equipos.", created)
    return created


def _looks_like_team_name(name: str, team_slug: str) -> bool:
    if _TEAM_PREFIX.match(name):
        return True
    if _STADIUM_WORDS.search(name):
        return False
    slug = team_slug.replace("-", " ").lower()
    norm = name.lower()
    if slug in norm.replace(" ", "-") or norm in slug:
        return True
    slug_words = set(slug.split())
    if slug_words and len(slug_words & set(norm.split())) >= max(1, len(slug_words) - 1):
        return True
    return _name_score(name) < 8


_CLUB_AS_NAME_FIXES: dict[str, str] = {
    "rio-ave-fc": "Estádio dos Arcos",
    "fc-alverca": "Complexo Desportivo do Alverca",
    "vikingur-reykjavik": "Víkingsvöllur",
}


def fix_club_as_stadium_names(conn, dry_run: bool = False) -> int:
    """Corrige nombres donde TM puso el club en lugar del estadio."""
    rows = conn.execute(text("""
        SELECT stadium_id, team_slug, stadium_name
        FROM dim_stadium
        WHERE team_slug = ANY(:slugs)
    """), {"slugs": list(_CLUB_AS_NAME_FIXES.keys())}).mappings().all()

    fixes: list[tuple[int, str]] = []
    for r in rows:
        target = _CLUB_AS_NAME_FIXES.get(r["team_slug"] or "")
        if not target:
            continue
        current = (r["stadium_name"] or "").strip()
        if current.lower() == target.lower():
            continue
        if _looks_like_team_name(current, r["team_slug"] or ""):
            fixes.append((r["stadium_id"], target))

    log.info("fix_club_as_stadium_names: %d correcciones.", len(fixes))
    if dry_run:
        for sid, name in fixes:
            log.info("  stadium_id=%s → %s", sid, name)
        return len(fixes)

    for sid, name in fixes:
        conn.execute(text("""
            UPDATE dim_stadium
            SET stadium_name = :name, updated_at = NOW()
            WHERE stadium_id = :id
        """), {"name": name, "id": sid})
    return len(fixes)


def fix_null_stadium_names(conn, dry_run: bool = False) -> tuple[int, int]:
    """Rellena stadium_name vacío desde fila TM hermana o elimina sintéticos redundantes."""
    from scripts.finalize_dim_stadium import _has_tm_duplicate, _merge_synthetic_geo, _slug_variants

    rows = conn.execute(text("""
        SELECT stadium_id, team_slug, canonical_team_id, data_source
        FROM dim_stadium
        WHERE stadium_name IS NULL OR TRIM(stadium_name) = ''
    """)).mappings().all()

    filled = deleted = 0
    for r in rows:
        sid = r["stadium_id"]
        slug = r["team_slug"] or ""
        variants = list(_slug_variants(slug)) or [slug]

        if r["data_source"] == "synthetic-geocode" and _has_tm_duplicate(
            conn, slug, r["canonical_team_id"],
        ):
            log.info("fix_null_stadium_names: eliminar sintético stadium_id=%s slug=%s", sid, slug)
            if not dry_run:
                _merge_synthetic_geo(conn, sid)
                conn.execute(text(
                    "UPDATE dim_match SET stadium_id = NULL WHERE stadium_id = :id"
                ), {"id": sid})
                conn.execute(text("DELETE FROM dim_stadium WHERE stadium_id = :id"), {"id": sid})
            deleted += 1
            continue

        donor = conn.execute(text("""
            SELECT stadium_name FROM dim_stadium
            WHERE data_source = 'transfermarkt'
              AND team_slug = ANY(:variants)
              AND stadium_name IS NOT NULL AND TRIM(stadium_name) <> ''
            ORDER BY valid_from_season DESC
            LIMIT 1
        """), {"variants": variants}).fetchone()
        if not donor:
            continue

        log.info("fix_null_stadium_names: stadium_id=%s ← %r", sid, donor[0])
        if not dry_run:
            conn.execute(text("""
                UPDATE dim_stadium
                SET stadium_name = :name, updated_at = NOW()
                WHERE stadium_id = :id
            """), {"name": donor[0], "id": sid})
        filled += 1

    log.info("fix_null_stadium_names: %d rellenados, %d sintéticos eliminados.", filled, deleted)
    return filled, deleted


def fix_names_from_wikipedia(conn, dry_run: bool = False) -> int:
    rows = conn.execute(text("""
        SELECT stadium_id, stadium_name, wikipedia_url, team_slug
        FROM dim_stadium
        WHERE wikipedia_url IS NOT NULL AND TRIM(wikipedia_url) <> ''
    """)).mappings().all()

    fixes: list[tuple[int, str]] = []
    for r in rows:
        wiki_name = _wikipedia_title_to_name(r["wikipedia_url"])
        if not wiki_name:
            continue
        current = (r["stadium_name"] or "").strip()
        if not current:
            fixes.append((r["stadium_id"], wiki_name))
            continue
        if current.lower() == wiki_name.lower():
            continue
        # No pisar nombres históricos SCD2 válidos (p. ej. Ramón de Carranza).
        if _STADIUM_WORDS.search(current) and _name_score(current) >= 15:
            continue
        wiki_slug = (r["wikipedia_url"] or "").split("/")[-1].lower()
        url_is_stadium = bool(
            _STADIUM_WORDS.search(wiki_name)
            or any(k in wiki_slug for k in (
                "stadium", "stade", "stadio", "estadio", "stadion", "arena",
                "park", "field", "ground", "trafford", "nou", "metropolitano",
            ))
        )
        if not url_is_stadium and not _looks_like_team_name(current, r["team_slug"] or ""):
            continue
        if not url_is_stadium and _STADIUM_WORDS.search(current):
            continue
        if not url_is_stadium and _name_score(wiki_name) <= _name_score(current):
            continue
        fixes.append((r["stadium_id"], wiki_name))

    log.info("fix_names_from_wikipedia: %d correcciones.", len(fixes))
    if dry_run:
        for sid, new_name in fixes[:25]:
            log.info("  stadium_id=%s → %s", sid, new_name)
        if len(fixes) > 25:
            log.info("  ... y %d más", len(fixes) - 25)
        return len(fixes)

    for sid, new_name in fixes:
        conn.execute(text("""
            UPDATE dim_stadium
            SET stadium_name = :name, updated_at = NOW()
            WHERE stadium_id = :id
        """), {"name": new_name, "id": sid})
    return len(fixes)


def _parse_city_from_address(address: str) -> str | None:
    from loaders.stadium_loader import parse_city_from_address
    return parse_city_from_address(address)


def fix_city_and_address(conn, dry_run: bool = False) -> int:
    """Corrige city/address cuando TM copió el nombre del estadio o un patrocinador."""
    rows = conn.execute(text("""
        SELECT s.stadium_id, s.team_slug, s.stadium_name, s.city, s.address,
               s.latitude, s.longitude, s.country,
               COALESCE(t.canonical_name, s.team_slug) AS team
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    """)).mappings().all()

    fixed = 0
    for r in rows:
        city = (r["city"] or "").strip()
        name = (r["stadium_name"] or "").strip()
        addr = (r["address"] or "").strip()
        updates: dict = {}

        bad_city = city_looks_invalid(city, name, addr)
        bad_addr = bool(addr) and (
            _looks_like_sponsor_label(addr)
            or not address_looks_valid(addr, name, city)
        )

        if not bad_city and not bad_addr:
            continue

        override = _lookup_stadium_override(r["team"] or "", name, enrich=False)
        new_city = (override.get("city") or "").strip() or None
        new_addr = (override.get("address") or "").strip() or None

        if bad_addr and not bad_city and addr and not _looks_like_sponsor_label(addr):
            bad_addr = False

        if bad_addr and not new_addr and name and not _looks_like_sponsor_label(name):
            if re.search(r"\d", addr) or "," in addr:
                new_addr = addr
            else:
                new_addr = None

        if bad_city:
            if not new_city and addr and re.search(r"\d", addr):
                new_city = _parse_city_from_address(addr)
            if (
                not new_city
                and not dry_run
                and r["latitude"] is not None
                and r["longitude"] is not None
            ):
                new_city = reverse_geocode_city(float(r["latitude"]), float(r["longitude"]))
            if bad_city and new_city and not city_looks_invalid(new_city, name, new_addr or addr):
                updates["city"] = new_city
            elif bad_city:
                updates["city"] = None

        if bad_addr:
            if new_addr and address_looks_valid(new_addr, name, updates.get("city") or city):
                updates["address"] = new_addr
            else:
                updates["address"] = None

        if not updates:
            continue

        fixed += 1
        if dry_run:
            log.info(
                "fix_city id=%s %s city=%r -> %r addr=%r -> %r",
                r["stadium_id"], r["team_slug"], city, updates.get("city"),
                addr, updates.get("address"),
            )
            continue

        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        conn.execute(
            text(f"UPDATE dim_stadium SET {set_clause}, updated_at = NOW() WHERE stadium_id = :id"),
            {**updates, "id": r["stadium_id"]},
        )

    log.info("fix_city_and_address: %d filas.", fixed)
    return fixed


def fix_addresses(conn, dry_run: bool = False, limit: int | None = None) -> int:
    """Rellena address vía overrides, filas hermanas o Nominatim reverse geocoding."""
    rows = conn.execute(text("""
        SELECT s.stadium_id, s.team_slug, s.canonical_team_id, s.stadium_name,
               s.city, s.address, s.latitude, s.longitude,
               COALESCE(t.canonical_name, s.team_slug) AS team
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        ORDER BY s.team_slug, s.valid_from_season
    """)).mappings().all()

    geo_cache: dict[tuple[float, float], str | None] = {}
    pending: list[tuple[dict, str | None]] = []
    fixed = 0

    for r in rows:
        addr = (r["address"] or "").strip()
        name = (r["stadium_name"] or "").strip()
        city = (r["city"] or "").strip()
        if address_looks_valid(addr, name, city):
            continue

        override = _lookup_stadium_override(r["team"] or "", name, enrich=False)
        new_addr = (override.get("address") or "").strip() or None
        if new_addr and not address_looks_valid(new_addr, name, city):
            new_addr = None

        if not new_addr and r["canonical_team_id"]:
            donor = conn.execute(text("""
                SELECT address FROM dim_stadium
                WHERE canonical_team_id = :cid
                  AND address IS NOT NULL AND TRIM(address) <> ''
                ORDER BY valid_from_season DESC
            """), {"cid": r["canonical_team_id"]}).fetchall()
            for (candidate,) in donor:
                caddr = (candidate or "").strip()
                if address_looks_valid(caddr, name, city):
                    new_addr = caddr
                    break

        if not new_addr and r["latitude"] is not None and r["longitude"] is not None:
            key = (round(float(r["latitude"]), 4), round(float(r["longitude"]), 4))
            if key not in geo_cache:
                geo_cache[key] = None
            if geo_cache[key] is None and key in geo_cache:
                pass  # resolved later in batch
            new_addr = geo_cache.get(key)

        pending.append((dict(r), new_addr))

    coord_keys = [k for k, v in geo_cache.items() if v is None]
    for i, key in enumerate(coord_keys):
        if limit is not None and i >= limit:
            break
        lat, lon = key
        if dry_run:
            geo_cache[key] = f"(geocode {lat},{lon})"
        else:
            geo_cache[key] = reverse_geocode_address(lat, lon)
            if (i + 1) % 25 == 0:
                log.info("fix_addresses: geocodificadas %d/%d coords únicas.", i + 1, len(coord_keys))

    for r, preset in pending:
        if limit is not None and fixed >= limit:
            break
        addr = (r["address"] or "").strip()
        name = (r["stadium_name"] or "").strip()
        city = (r["city"] or "").strip()
        new_addr = preset
        if not new_addr and r["latitude"] is not None and r["longitude"] is not None:
            key = (round(float(r["latitude"]), 4), round(float(r["longitude"]), 4))
            candidate = geo_cache.get(key)
            if candidate and not str(candidate).startswith("(geocode"):
                new_addr = candidate

        if dry_run and isinstance(new_addr, str) and new_addr.startswith("(geocode"):
            fixed += 1
            continue
        if new_addr and address_looks_valid(new_addr, name, city):
            target: str | None = new_addr
        elif addr:
            target = None
        else:
            continue

        if (addr or "") == (target or ""):
            continue

        fixed += 1
        if dry_run:
            log.info(
                "fix_addresses id=%s %s %r -> %r",
                r["stadium_id"], r["team_slug"], addr or None, target,
            )
            continue

        conn.execute(text("""
            UPDATE dim_stadium
            SET address = :address, updated_at = NOW()
            WHERE stadium_id = :id
        """), {"address": target, "id": r["stadium_id"]})

    log.info("fix_addresses: %d filas (%d coords únicas).", fixed, len(coord_keys))
    return fixed


def _merge_group(conn, rows, dry_run: bool) -> int:
    """Fusiona un grupo de filas en una sola. Devuelve filas borradas."""
    if len(rows) <= 1:
        return 0

    enriched = []
    for r in rows:
        d = dict(r._mapping)
        filled = sum(
            1 for k, v in d.items()
            if k not in ("stadium_id", "created_at", "updated_at", "data_hash")
            and v is not None and str(v).strip() != ""
        )
        enriched.append((filled, r))
    enriched.sort(key=lambda x: (-x[0], x[1].stadium_id))
    keeper_row = enriched[0][1]

    new_from = min(r.valid_from_season for r in rows)
    new_to = max(r.valid_to_season for r in rows)
    best_name = _pick_best_name([r.stadium_name for r in rows])

    delete_ids = [r.stadium_id for r in rows if r.stadium_id != keeper_row.stadium_id]
    if not delete_ids:
        return 0

    log.debug(
        "merge keeper=%s delete=%s name=%r range=%s..%s",
        keeper_row.stadium_id, delete_ids, best_name, new_from, new_to,
    )

    tm_id = conn.execute(text(
        "SELECT id_transfermarkt_team FROM dim_stadium WHERE stadium_id = :id"
    ), {"id": keeper_row.stadium_id}).scalar()

    # Filas con el mismo valid_from bloquean el UPDATE del keeper (índice único).
    conflicts = conn.execute(text("""
        SELECT stadium_id, stadium_name, valid_from_season, valid_to_season
        FROM dim_stadium
        WHERE id_transfermarkt_team = :tid
          AND stadium_id != :keeper
          AND valid_from_season = :nf
    """), {"tid": tm_id, "keeper": keeper_row.stadium_id, "nf": new_from}).fetchall()
    for c in conflicts:
        if c.stadium_id not in delete_ids:
            delete_ids.append(c.stadium_id)
            new_to = max(new_to, c.valid_to_season)
            extra = _pick_best_name([best_name, c.stadium_name])
            if extra:
                best_name = extra

    if dry_run:
        return len(delete_ids)

    for old_id in delete_ids:
        conn.execute(text("""
            UPDATE dim_match SET stadium_id = :new_id
            WHERE stadium_id = :old_id
        """), {"new_id": keeper_row.stadium_id, "old_id": old_id})
        conn.execute(text(
            "DELETE FROM dim_stadium WHERE stadium_id = :id"
        ), {"id": old_id})

    conn.execute(text("""
        UPDATE dim_stadium
        SET valid_from_season = :nf,
            valid_to_season = :nt,
            stadium_name = COALESCE(:name, stadium_name),
            updated_at = NOW()
        WHERE stadium_id = :id
    """), {
        "nf": new_from,
        "nt": new_to,
        "name": best_name,
        "id": keeper_row.stadium_id,
    })

    return len(delete_ids)


def merge_by_stadium_name(conn, dry_run: bool = False) -> int:
    groups = conn.execute(text("""
        SELECT id_transfermarkt_team, LOWER(TRIM(stadium_name)) AS norm_name, COUNT(*) AS n
        FROM dim_stadium
        WHERE stadium_name IS NOT NULL AND TRIM(stadium_name) <> ''
        GROUP BY 1, 2 HAVING COUNT(*) > 1
    """)).fetchall()

    deleted = 0
    for g in groups:
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, valid_from_season, valid_to_season,
                   wikidata_qid, capacity, city, country, latitude, longitude
            FROM dim_stadium
            WHERE id_transfermarkt_team = :tid
              AND LOWER(TRIM(stadium_name)) = :norm
            ORDER BY stadium_id
        """), {"tid": g.id_transfermarkt_team, "norm": g.norm_name}).fetchall()
        deleted += _merge_group(conn, rows, dry_run)

    log.info("merge_by_stadium_name: %d filas eliminadas.", deleted)
    return deleted


def merge_by_wikidata_qid(conn, dry_run: bool = False) -> int:
    groups = conn.execute(text("""
        SELECT id_transfermarkt_team, wikidata_qid, COUNT(*) AS n
        FROM dim_stadium
        WHERE wikidata_qid IS NOT NULL AND TRIM(wikidata_qid) <> ''
        GROUP BY 1, 2 HAVING COUNT(*) > 1
    """)).fetchall()

    deleted = 0
    for g in groups:
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, valid_from_season, valid_to_season,
                   wikidata_qid, capacity, city, country, latitude, longitude
            FROM dim_stadium
            WHERE id_transfermarkt_team = :tid AND wikidata_qid = :qid
            ORDER BY stadium_id
        """), {"tid": g.id_transfermarkt_team, "qid": g.wikidata_qid}).fetchall()
        deleted += _merge_group(conn, rows, dry_run)

    log.info("merge_by_wikidata_qid: %d filas eliminadas.", deleted)
    return deleted


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Repara dim_stadium (FK, nombres, fusión).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--addresses-only",
        action="store_true",
        help="Solo ejecuta fix_addresses (Nominatim; puede tardar varios minutos).",
    )
    parser.add_argument(
        "--address-limit",
        type=int,
        default=None,
        help="Límite de filas en fix_addresses (pruebas).",
    )
    args = parser.parse_args()

    with engine.begin() as conn:
        if args.addresses_only:
            n_addr = fix_addresses(conn, dry_run=args.dry_run, limit=args.address_limit)
            print(f"{'(dry-run) ' if args.dry_run else ''}address mejoradas: {n_addr}")
            return

        before = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        n_fk = backfill_canonical_team_id(conn, dry_run=args.dry_run)
        n_remap = remap_known_duplicate_teams(conn, dry_run=args.dry_run)
        n_team = ensure_dim_team_for_orphan_stadiums(conn, dry_run=args.dry_run)
        n_names = fix_club_as_stadium_names(conn, dry_run=args.dry_run)
        n_null, n_null_del = fix_null_stadium_names(conn, dry_run=args.dry_run)
        n_geo = fix_city_and_address(conn, dry_run=args.dry_run)
        n_addr = fix_addresses(conn, dry_run=args.dry_run, limit=args.address_limit)
        n_wiki = fix_names_from_wikipedia(conn, dry_run=args.dry_run)
        n_qid = merge_by_wikidata_qid(conn, dry_run=args.dry_run)
        n_name = merge_by_stadium_name(conn, dry_run=args.dry_run)

        if not args.dry_run:
            n_hash = backfill_hashes(conn, dry_run=False, force=True)
            n_overlap = merge_overlapping_duplicates(conn, dry_run=False)
            n_adj = merge_adjacent(conn, dry_run=False)
        else:
            n_hash = backfill_hashes(conn, dry_run=True, force=True)
            n_overlap = merge_overlapping_duplicates(conn, dry_run=True)
            n_adj = merge_adjacent(conn, dry_run=True)

        after = before if args.dry_run else conn.execute(
            text("SELECT COUNT(*) FROM dim_stadium")
        ).scalar()

    verb = "(dry-run) " if args.dry_run else ""
    print(f"\n{verb}Filas antes: {before} → después: {after}")
    print(f"{verb}canonical_team_id rellenados: {n_fk}")
    print(f"{verb}equipos duplicados remapeados: {n_remap}")
    print(f"{verb}dim_team creados: {n_team}")
    print(f"{verb}nombres club→estadio: {n_names}")
    print(f"{verb}stadium_name nulo rellenados/eliminados: {n_null}/{n_null_del}")
    print(f"{verb}city/address corregidos: {n_geo}")
    print(f"{verb}address geocodificadas: {n_addr}")
    print(f"{verb}nombres desde Wikipedia: {n_wiki}")
    print(f"{verb}fusionadas por nombre: {n_name}")
    print(f"{verb}fusionadas por wikidata_qid: {n_qid}")
    print(f"{verb}data_hash recalculados: {n_hash}")
    print(f"{verb}grupos hash duplicados: {n_overlap}")
    print(f"{verb}adyacentes fusionados: {n_adj}")


if __name__ == "__main__":
    main()
