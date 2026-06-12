"""
scripts/finalize_dim_stadium.py
================================
Limpieza final de dim_stadium:

  1. Copia coords de sintéticos redundantes a filas TM del mismo equipo.
  2. Elimina sintéticos si ya hay fila Transfermarkt (por slug / canonical_id).
  3. Corrige nombres basura en sintéticos que quedan (auto geocoded, nombre=club).
  4. (Opcional) Fusiona pares SCD2 con mismo nombre normalizado.

Uso:
    python -m scripts.finalize_dim_stadium --dry-run
    python -m scripts.finalize_dim_stadium
    python -m scripts.finalize_dim_stadium --merge-similar
"""

from __future__ import annotations

import argparse
import logging
import re

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    _entity_label,
    _fetch_entity,
    _lookup_stadium_override,
    _name_looks_like_club,
    query_wikidata_by_club,
)
from scripts.compact_dim_stadium import backfill_hashes
from scripts.repair_dim_stadium import _merge_group, _pick_best_name
from wizard.competitions import WORKING_COMPETITION_NAMES

log = logging.getLogger(__name__)

_AUTO_GEO = re.compile(r"\(auto geocoded\)", re.I)
_SLUG_PREFIX = re.compile(
    r"^(?:fc-|sc-|ac-|cd-|ud-|rcd-|rc-|real-|cf-|fk-|sk-|1-)",
    re.I,
)


def _slug_variants(slug: str) -> set[str]:
    s = (slug or "").lower().strip()
    if not s:
        return set()
    variants = {s}
    stripped = _SLUG_PREFIX.sub("", s)
    if stripped and stripped != s:
        variants.add(stripped)
        variants.add(f"fc-{stripped}")
    return variants

_GEO_COLS = (
    "latitude", "longitude", "timezone", "altitude_m",
    "wikidata_qid", "wikipedia_url", "image_url", "city", "country",
)

_MANUAL_MERGES: list[tuple[int, list[int], str | None]] = [
    (506, [62], "Estadio Ennio Tardini"),
    (346, [67], "Stadio Olimpico Grande Torino"),
    (350, [70], "New Balance Arena"),
    (389, [113], "Estádio Municipal 22 de Junho"),
    (431, [167], "Agia Sofia Stadium"),
]

_TM_MATCH_SQL = """
    SELECT 1 FROM dim_stadium tm
    WHERE tm.data_source = 'transfermarkt'
      AND (
        tm.team_slug = :slug
        OR tm.team_slug LIKE :slug || '-%'
        OR :slug LIKE tm.team_slug || '-%'
        OR (
          :cid IS NOT NULL
          AND tm.canonical_team_id IS NOT NULL
          AND tm.canonical_team_id = :cid
        )
      )
    LIMIT 1
"""


def _working_comp_ids(conn) -> list[int]:
    return [
        r.canonical_id
        for r in conn.execute(text("""
            SELECT canonical_id FROM dim_competition
            WHERE canonical_name = ANY(:names)
        """), {"names": sorted(WORKING_COMPETITION_NAMES)}).fetchall()
    ]


def _team_match_count(conn, canonical_team_id: int, comp_ids: list[int]) -> int:
    if not comp_ids:
        return 0
    return conn.execute(text("""
        SELECT COUNT(*) FROM dim_match m
        WHERE m.competition_id = ANY(:cids)
          AND :tid IN (m.home_team_id, m.away_team_id)
    """), {"cids": comp_ids, "tid": canonical_team_id}).scalar() or 0


def _has_tm_duplicate(conn, team_slug: str, canonical_team_id: int | None) -> bool:
    if conn.execute(
        text(_TM_MATCH_SQL),
        {"slug": team_slug, "cid": canonical_team_id},
    ).fetchone() is not None:
        return True
    want = _slug_variants(team_slug)
    if not want:
        return False
    tm_slugs = conn.execute(text("""
        SELECT team_slug FROM dim_stadium
        WHERE data_source = 'transfermarkt' AND team_slug IS NOT NULL
    """)).fetchall()
    for (tm_slug,) in tm_slugs:
        if want & _slug_variants(tm_slug):
            return True
    return False


def _merge_synthetic_geo(conn, stadium_id: int) -> int:
    sets = ", ".join(f"{col} = COALESCE(tm.{col}, sg.{col})" for col in _GEO_COLS)
    result = conn.execute(text(f"""
        UPDATE dim_stadium tm SET
            {sets},
            updated_at = NOW()
        FROM dim_stadium sg
        WHERE sg.stadium_id = :sg_id
          AND tm.data_source = 'transfermarkt'
          AND (
            tm.team_slug = sg.team_slug
            OR tm.team_slug LIKE sg.team_slug || '-%'
            OR sg.team_slug LIKE tm.team_slug || '-%'
          )
          AND (tm.latitude IS NULL OR tm.longitude IS NULL)
          AND sg.latitude IS NOT NULL
    """), {"sg_id": stadium_id})
    return result.rowcount or 0


def _collect_synthetics_to_delete(conn) -> list[int]:
    comp_ids = _working_comp_ids(conn)
    synthetics = conn.execute(text("""
        SELECT stadium_id, canonical_team_id, team_slug
        FROM dim_stadium
        WHERE data_source = 'synthetic-geocode'
           OR id_transfermarkt_team < 0
    """)).fetchall()

    to_delete: list[int] = []
    for s in synthetics:
        if _has_tm_duplicate(conn, s.team_slug, s.canonical_team_id):
            to_delete.append(s.stadium_id)
            continue
        if s.canonical_team_id is None:
            to_delete.append(s.stadium_id)
            continue
        if _team_match_count(conn, s.canonical_team_id, comp_ids) == 0:
            to_delete.append(s.stadium_id)
    return to_delete


def delete_synthetic(conn, dry_run: bool) -> tuple[int, int]:
    to_delete = _collect_synthetics_to_delete(conn)
    merged_rows = 0
    log.info("delete_synthetic: %d filas candidatas.", len(to_delete))

    if dry_run:
        return len(to_delete), 0

    for sid in to_delete:
        merged_rows += _merge_synthetic_geo(conn, sid)

    if to_delete:
        conn.execute(text(
            "UPDATE dim_match SET stadium_id = NULL WHERE stadium_id = ANY(:ids)"
        ), {"ids": to_delete})
        conn.execute(text(
            "DELETE FROM dim_stadium WHERE stadium_id = ANY(:ids)"
        ), {"ids": to_delete})

    return len(to_delete), merged_rows


def _bad_synthetic_name(stadium_name: str | None, team: str) -> bool:
    name = (stadium_name or "").strip()
    if not name:
        return True
    if _AUTO_GEO.search(name):
        return True
    return _name_looks_like_club(name, team)


def _resolve_synthetic_name(team: str, current_name: str) -> dict:
    override = _lookup_stadium_override(team, current_name)
    if override.get("stadium_name") and not _name_looks_like_club(
        override["stadium_name"], team,
    ):
        return override

    row = query_wikidata_by_club(team)
    if row.get("wikidata_qid"):
        ent = _fetch_entity(str(row["wikidata_qid"]))
        if ent:
            label = _entity_label(ent)
            if label and not _name_looks_like_club(label, team):
                return {**row, "stadium_name": label}
    return row


def fix_synthetic_names(conn, dry_run: bool) -> int:
    rows = conn.execute(text("""
        SELECT s.stadium_id, s.stadium_name, s.team_slug,
               COALESCE(t.canonical_name, s.team_slug) AS team
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        WHERE s.data_source = 'synthetic-geocode'
    """)).mappings().all()

    fixed = 0
    for row in rows:
        if not _bad_synthetic_name(row["stadium_name"], row["team"]):
            continue

        data = _resolve_synthetic_name(row["team"], row["stadium_name"] or "")
        updates: dict = {}
        new_name = data.get("stadium_name")
        if new_name and not _name_looks_like_club(new_name, row["team"]):
            updates["stadium_name"] = new_name
        elif _AUTO_GEO.search(row["stadium_name"] or ""):
            updates["stadium_name"] = None

        for col in _GEO_COLS:
            if col in updates:
                continue
            val = data.get(col)
            if val not in (None, ""):
                updates[col] = val

        if not updates:
            continue

        fixed += 1
        log.info(
            "fix_synthetic_names id=%s %r -> %r",
            row["stadium_id"], row["stadium_name"], updates.get("stadium_name"),
        )
        if dry_run:
            continue

        set_clause = ", ".join(f"{k} = :{k}" for k in updates)
        conn.execute(
            text(f"UPDATE dim_stadium SET {set_clause}, updated_at = NOW() "
                 f"WHERE stadium_id = :id"),
            {**updates, "id": row["stadium_id"]},
        )

    log.info("fix_synthetic_names: %d filas.", fixed)
    return fixed


def apply_manual_merges(conn, dry_run: bool) -> int:
    deleted = 0
    for keeper_id, drop_ids, forced_name in _MANUAL_MERGES:
        ids = [keeper_id, *drop_ids]
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, valid_from_season, valid_to_season,
                   wikidata_qid, capacity, city, country, latitude, longitude
            FROM dim_stadium WHERE stadium_id = ANY(:ids)
        """), {"ids": ids}).fetchall()
        if len(rows) < 2:
            continue
        new_from = min(r.valid_from_season for r in rows)
        new_to = max(r.valid_to_season for r in rows)
        name = forced_name or _pick_best_name([r.stadium_name for r in rows])
        log.info("manual merge keeper=%s drop=%s name=%r", keeper_id, drop_ids, name)
        if dry_run:
            deleted += len(drop_ids)
            continue
        for old_id in drop_ids:
            conn.execute(text(
                "UPDATE dim_match SET stadium_id = :k WHERE stadium_id = :d"
            ), {"k": keeper_id, "d": old_id})
            conn.execute(text(
                "DELETE FROM dim_stadium WHERE stadium_id = :d"
            ), {"d": old_id})
            deleted += 1
        conn.execute(text("""
            UPDATE dim_stadium
            SET valid_from_season = :nf, valid_to_season = :nt,
                stadium_name = COALESCE(:name, stadium_name),
                updated_at = NOW()
            WHERE stadium_id = :id
        """), {"nf": new_from, "nt": new_to, "name": name, "id": keeper_id})
    log.info("apply_manual_merges: %d filas eliminadas.", deleted)
    return deleted


def merge_similar_names(conn, dry_run: bool) -> int:
    groups = conn.execute(text("""
        SELECT id_transfermarkt_team,
               REGEXP_REPLACE(LOWER(TRIM(stadium_name)), '[^a-z0-9]+', '', 'g') AS norm,
               COUNT(*) AS n
        FROM dim_stadium
        WHERE stadium_name IS NOT NULL
        GROUP BY 1, 2 HAVING COUNT(*) > 1
    """)).fetchall()

    deleted = 0
    for g in groups:
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, valid_from_season, valid_to_season,
                   wikidata_qid, capacity, city, country, latitude, longitude
            FROM dim_stadium
            WHERE id_transfermarkt_team = :tid
              AND REGEXP_REPLACE(LOWER(TRIM(stadium_name)), '[^a-z0-9]+', '', 'g') = :norm
            ORDER BY stadium_id
        """), {"tid": g.id_transfermarkt_team, "norm": g.norm}).fetchall()
        deleted += _merge_group(conn, rows, dry_run)
    log.info("merge_similar_names: %d filas eliminadas.", deleted)
    return deleted


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-wikidata", action="store_true",
                        help="No corregir nombres vía Wikidata.")
    parser.add_argument("--merge-similar", action="store_true",
                        help="Fusionar filas TM duplicadas con mismo nombre normalizado.")
    args = parser.parse_args()

    with engine.begin() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        n_syn, n_merged = delete_synthetic(conn, args.dry_run)
        n_names = 0 if args.skip_wikidata else fix_synthetic_names(conn, args.dry_run)
        n_manual = apply_manual_merges(conn, args.dry_run)
        n_sim = merge_similar_names(conn, args.dry_run) if args.merge_similar else 0
        if not args.dry_run:
            backfill_hashes(conn, dry_run=False, force=True)
        after = before if args.dry_run else conn.execute(
            text("SELECT COUNT(*) FROM dim_stadium")
        ).scalar()

    verb = "(dry-run) " if args.dry_run else ""
    print(f"\n{verb}Antes: {before} → después: {after}")
    print(f"{verb}Sintéticos eliminados: {n_syn} (geo fusionada en TM: {n_merged})")
    print(f"{verb}Nombres sintéticos corregidos: {n_names}")
    print(f"{verb}Fusiones manuales: {n_manual}")
    print(f"{verb}Fusiones por nombre similar: {n_sim}")


if __name__ == "__main__":
    main()
