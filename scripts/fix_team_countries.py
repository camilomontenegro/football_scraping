"""
Corrige dim_team.country: país del club, no de la liga TM.

Uso:
    python -m scripts.fix_team_countries --dry-run
    python -m scripts.fix_team_countries
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine
from utils.team_countries import resolve_team_country

log = logging.getLogger(__name__)


def _fetch_teams(conn):
    return conn.execute(
        text("""
            SELECT dt.canonical_id, dt.canonical_name, dt.country,
                   dt.id_transfermarkt,
                   ls.stadium_country
            FROM dim_team dt
            LEFT JOIN LATERAL (
                SELECT country AS stadium_country
                FROM dim_stadium
                WHERE canonical_team_id = dt.canonical_id
                ORDER BY valid_to_season DESC NULLS LAST
                LIMIT 1
            ) ls ON TRUE
            ORDER BY dt.canonical_name
        """)
    ).fetchall()


def run(dry_run: bool) -> None:
    updated = 0
    with engine.begin() as conn:
        for row in _fetch_teams(conn):
            new_country = resolve_team_country(
                row.canonical_name,
                tm_id=row.id_transfermarkt,
                stadium_country=row.stadium_country,
                existing=row.country,
            )
            if not new_country:
                continue
            if row.country == new_country:
                continue
            log.info(
                "[%s] %s: %r → %r",
                row.canonical_id,
                row.canonical_name,
                row.country,
                new_country,
            )
            if not dry_run:
                conn.execute(
                    text("UPDATE dim_team SET country = :c WHERE canonical_id = :id"),
                    {"c": new_country, "id": row.canonical_id},
                )
            updated += 1

    log.info("[%s] %d equipos actualizados", "DRY-RUN" if dry_run else "OK", updated)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
