"""
scripts/fix_stadium_season_ranges.py
====================================
Corrige rangos de temporada irreales en dim_stadium (p. ej. 1900/1901–2099/2100
creados por backfill_synthetic_stadiums).

Para cada fila afectada, usa MIN/MAX de dim_match.season del equipo.
Si no hay partidos, usa el rango global de la BD.

Uso:
    python -m scripts.fix_stadium_season_ranges --dry-run
    python -m scripts.fix_stadium_season_ranges
"""

from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from loaders.common import engine

log = logging.getLogger(__name__)

_INVALID_WHERE = """
    valid_to_season >= '2090'
    OR valid_from_season < '1990'
    OR valid_to_season > '2035'
"""


def fix(dry_run: bool = False) -> dict:
    with engine.begin() as conn:
        global_row = conn.execute(text("""
            SELECT MIN(season) AS min_s, MAX(season) AS max_s
            FROM dim_match WHERE season IS NOT NULL
        """)).one()
        fallback_from = global_row.min_s or "2020/2021"
        fallback_to = global_row.max_s or "2025/2026"

        affected = conn.execute(text(f"""
            SELECT stadium_id, team_slug, canonical_team_id,
                   valid_from_season, valid_to_season, data_source
            FROM dim_stadium
            WHERE {_INVALID_WHERE}
            ORDER BY stadium_id
        """)).fetchall()

        updates: list[dict] = []
        for row in affected:
            bounds = conn.execute(text("""
                SELECT MIN(m.season) AS min_s, MAX(m.season) AS max_s
                FROM dim_match m
                WHERE :tid IN (m.home_team_id, m.away_team_id)
                  AND m.season IS NOT NULL
            """), {"tid": row.canonical_team_id}).one()

            new_from = bounds.min_s or fallback_from
            new_to = bounds.max_s or fallback_to
            if new_from > new_to:
                new_from, new_to = new_to, new_from

            updates.append({
                "id": row.stadium_id,
                "slug": row.team_slug,
                "old": f"{row.valid_from_season}..{row.valid_to_season}",
                "new": f"{new_from}..{new_to}",
                "nf": new_from,
                "nt": new_to,
            })

        if dry_run:
            return {"count": len(updates), "sample": updates[:20], "fallback": (fallback_from, fallback_to)}

        for u in updates:
            conn.execute(text("""
                UPDATE dim_stadium
                SET valid_from_season = :nf,
                    valid_to_season = :nt,
                    updated_at = NOW()
                WHERE stadium_id = :id
            """), {"nf": u["nf"], "nt": u["nt"], "id": u["id"]})

        remaining = conn.execute(text(f"""
            SELECT COUNT(*) FROM dim_stadium WHERE {_INVALID_WHERE}
        """)).scalar()

        return {"updated": len(updates), "remaining": remaining}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = fix(dry_run=args.dry_run)
    if args.dry_run:
        fb = result["fallback"]
        print(f"\n[dry-run] Filas a corregir: {result['count']}")
        print(f"[dry-run] Fallback sin partidos: {fb[0]} .. {fb[1]}")
        for u in result.get("sample", []):
            print(f"  {u['id']:4d}  {u['slug']:<30s}  {u['old']}  ->  {u['new']}")
        if result["count"] > 20:
            print(f"  ... y {result['count'] - 20} más")
    else:
        print(f"\nActualizadas: {result['updated']}")
        print(f"Restantes con rango inválido: {result['remaining']}")


if __name__ == "__main__":
    main()
