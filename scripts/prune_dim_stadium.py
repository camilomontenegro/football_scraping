"""
scripts/prune_dim_stadium.py
============================
Elimina filas de dim_stadium de equipos que no participan en las competiciones
activas del proyecto (WORKING_COMPETITIONS).

Mantiene un estadio si su equipo (canonical_team_id o id_transfermarkt_team)
aparece en al menos un partido de dim_match en una de esas competiciones.

Uso:
    python -m scripts.prune_dim_stadium --dry-run
    python -m scripts.prune_dim_stadium
"""

from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from loaders.common import engine
from wizard.competitions import WORKING_COMPETITION_NAMES

log = logging.getLogger(__name__)


def _resolve_working_competition_ids(conn) -> list[int]:
    names = sorted(WORKING_COMPETITION_NAMES)
    rows = conn.execute(text("""
        SELECT canonical_id, canonical_name
        FROM dim_competition
        WHERE canonical_name = ANY(:names)
    """), {"names": names}).fetchall()
    found = {r.canonical_name for r in rows}
    missing = WORKING_COMPETITION_NAMES - found
    if missing:
        log.warning(
            "Competiciones WORKING sin fila en dim_competition: %s",
            ", ".join(sorted(missing)),
        )
    return [r.canonical_id for r in rows]


def _working_team_ids(conn, comp_ids: list[int]) -> tuple[set[int], set[int]]:
    if not comp_ids:
        return set(), set()

    team_rows = conn.execute(text("""
        SELECT DISTINCT t.canonical_id, t.id_transfermarkt
        FROM dim_match m
        JOIN dim_team t ON t.canonical_id IN (m.home_team_id, m.away_team_id)
        WHERE m.competition_id = ANY(:cids)
    """), {"cids": comp_ids}).fetchall()

    canonical = {r.canonical_id for r in team_rows}
    tm_ids = {r.id_transfermarkt for r in team_rows if r.id_transfermarkt is not None}
    return canonical, tm_ids


def prune(dry_run: bool = False) -> dict:
    with engine.begin() as conn:
        comp_ids = _resolve_working_competition_ids(conn)
        keep_canonical, keep_tm = _working_team_ids(conn, comp_ids)

        total = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()

        to_delete = conn.execute(text("""
            SELECT stadium_id, team_slug, stadium_name, canonical_team_id, id_transfermarkt_team
            FROM dim_stadium
            WHERE (
                canonical_team_id IS NULL
                OR canonical_team_id != ALL(:keep_c)
            )
            AND (
                id_transfermarkt_team IS NULL
                OR id_transfermarkt_team != ALL(:keep_tm)
            )
        """), {
            "keep_c": list(keep_canonical) or [-1],
            "keep_tm": list(keep_tm) or [-1],
        }).fetchall()

        delete_ids = [r.stadium_id for r in to_delete]
        matches_affected = 0
        if delete_ids:
            matches_affected = conn.execute(text("""
                SELECT COUNT(*) FROM dim_match WHERE stadium_id = ANY(:ids)
            """), {"ids": delete_ids}).scalar()

        if dry_run:
            return {
                "total": total,
                "keep_teams": len(keep_canonical),
                "to_delete": len(delete_ids),
                "matches_unlink": matches_affected,
                "sample": [dict(r._mapping) for r in to_delete[:30]],
            }

        if delete_ids:
            conn.execute(text("""
                UPDATE dim_match SET stadium_id = NULL
                WHERE stadium_id = ANY(:ids)
            """), {"ids": delete_ids})
            conn.execute(text("""
                DELETE FROM dim_stadium WHERE stadium_id = ANY(:ids)
            """), {"ids": delete_ids})

        remaining = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        return {
            "total": total,
            "keep_teams": len(keep_canonical),
            "deleted": len(delete_ids),
            "matches_unlink": matches_affected,
            "remaining": remaining,
        }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Poda dim_stadium a equipos de WORKING_COMPETITIONS."
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = prune(dry_run=args.dry_run)
    verb = "[dry-run] " if args.dry_run else ""
    print(f"\n{verb}Competiciones activas: {', '.join(sorted(WORKING_COMPETITION_NAMES))}")
    print(f"{verb}Equipos en partidos de esas ligas: {result['keep_teams']}")
    print(f"{verb}Filas dim_stadium antes: {result['total']}")
    if args.dry_run:
        print(f"{verb}Filas a eliminar: {result['to_delete']}")
        print(f"{verb}Partidos que perderían stadium_id: {result['matches_unlink']}")
        if result.get("sample"):
            print("\nMuestra de estadios a eliminar:")
            for row in result["sample"]:
                print(f"  {row['stadium_id']:4d}  {row['team_slug']:<35s}  {row['stadium_name']}")
    else:
        print(f"{verb}Filas eliminadas: {result['deleted']}")
        print(f"{verb}Partidos desvinculados: {result['matches_unlink']}")
        print(f"{verb}Filas restantes: {result['remaining']}")


if __name__ == "__main__":
    main()
