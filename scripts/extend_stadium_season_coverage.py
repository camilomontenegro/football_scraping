"""
Extiende valid_from/valid_to para cubrir todas las temporadas en las que
el equipo jugó como local en dim_match.
"""

from __future__ import annotations

import argparse
import logging
import re

from sqlalchemy import text

from loaders.common import engine

log = logging.getLogger(__name__)


def _season_year(season: str) -> int:
    return int(season.split("/")[0])


def _season_label(year: int) -> str:
    return f"{year}/{year + 1}"


def _seasons_in_range(frm: str, to: str) -> set[str]:
    a, b = _season_year(frm), _season_year(to)
    if a > b:
        a, b = b, a
    return {_season_label(y) for y in range(a, b + 1)}


def extend(dry_run: bool = False) -> int:
    updated = 0
    with engine.begin() as conn:
        teams = conn.execute(text("""
            SELECT DISTINCT m.home_team_id AS tid
            FROM dim_match m
            WHERE m.season IS NOT NULL
        """)).fetchall()

        for (tid,) in teams:
            home_seasons = {
                r.season
                for r in conn.execute(text("""
                    SELECT DISTINCT season FROM dim_match
                    WHERE home_team_id = :tid AND season IS NOT NULL
                """), {"tid": tid}).fetchall()
            }
            if not home_seasons:
                continue

            rows = conn.execute(text("""
                SELECT stadium_id, valid_from_season, valid_to_season
                FROM dim_stadium
                WHERE canonical_team_id = :tid
                ORDER BY valid_from_season, stadium_id
            """), {"tid": tid}).fetchall()
            if not rows:
                continue

            needed_from = min(home_seasons)
            needed_to = max(home_seasons)

            if len(rows) == 1:
                r = rows[0]
                nf = min(r.valid_from_season, needed_from)
                nt = max(r.valid_to_season, needed_to)
                if nf != r.valid_from_season or nt != r.valid_to_season:
                    if not dry_run:
                        conn.execute(text("""
                            UPDATE dim_stadium
                            SET valid_from_season=:nf, valid_to_season=:nt, updated_at=NOW()
                            WHERE stadium_id=:id
                        """), {"nf": nf, "nt": nt, "id": r.stadium_id})
                    updated += 1
                continue

            # Varias filas SCD2: extender la primera hacia atrás y la última hacia adelante.
            first, last = rows[0], rows[-1]
            if first.valid_from_season > needed_from:
                if not dry_run:
                    conn.execute(text("""
                        UPDATE dim_stadium SET valid_from_season=:nf, updated_at=NOW()
                        WHERE stadium_id=:id
                    """), {"nf": needed_from, "id": first.stadium_id})
                updated += 1

            if last.valid_to_season < needed_to:
                if not dry_run:
                    conn.execute(text("""
                        UPDATE dim_stadium SET valid_to_season=:nt, updated_at=NOW()
                        WHERE stadium_id=:id
                    """), {"nt": needed_to, "id": last.stadium_id})
                updated += 1

            # Huecos entre filas consecutivas: extender valid_to de la anterior.
            for i in range(len(rows) - 1):
                a, b = rows[i], rows[i + 1]
                gap_seasons = {
                    s for s in home_seasons
                    if a.valid_to_season < s < b.valid_from_season
                }
                if not gap_seasons:
                    continue
                bridge_to = max(gap_seasons)
                if bridge_to <= a.valid_to_season:
                    continue
                if not dry_run:
                    conn.execute(text("""
                        UPDATE dim_stadium SET valid_to_season=:nt, updated_at=NOW()
                        WHERE stadium_id=:id
                    """), {"nt": bridge_to, "id": a.stadium_id})
                updated += 1
                rows = conn.execute(text("""
                    SELECT stadium_id, valid_from_season, valid_to_season
                    FROM dim_stadium WHERE canonical_team_id=:tid
                    ORDER BY valid_from_season, stadium_id
                """), {"tid": tid}).fetchall()

    return updated


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    n = extend(dry_run=args.dry_run)
    print(f"{'[dry-run] ' if args.dry_run else ''}Filas actualizadas: {n}")


if __name__ == "__main__":
    main()
