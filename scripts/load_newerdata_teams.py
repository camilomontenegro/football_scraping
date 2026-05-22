"""
scripts/load_newerdata_teams.py
================================
One-off loader for the three new teams CSVs in DATA_football/newerdata.
Upserts into dim_team matching by id_sofascore first, then by name.
Safe to re-run.

Run from football_scraping/:
    python -m scripts.load_newerdata_teams
"""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from loaders.team_loader import _upsert_team

log = logging.getLogger(__name__)

SOURCE_DIR = Path(r"C:\Users\camil\Escritorio\Desktop\Mercanza\DATA_football\newerdata")

FILES = [
    SOURCE_DIR / "teams_champions_league.csv",
    SOURCE_DIR / "teams_primeira_liga.csv",
    SOURCE_DIR / "teams_seria_A.csv",
]


def load_newerdata_teams() -> int:
    total = 0
    with engine.begin() as conn:
        for filepath in FILES:
            if not filepath.exists():
                log.warning("File not found, skipping: %s", filepath)
                continue

            df = pd.read_csv(filepath)
            count = 0
            for _, row in df.iterrows():
                sid = row.get("id_sofascore")
                name = row.get("canonical_name")
                country = row.get("country") or None

                if not sid or not name:
                    continue

                try:
                    cid = _upsert_team(conn, str(name), "id_sofascore", sid)
                    if country:
                        conn.execute(
                            text("UPDATE dim_team SET country = COALESCE(country, :c) WHERE canonical_id = :cid"),
                            {"c": country, "cid": cid},
                        )
                    count += 1
                except Exception as e:
                    log.error("Error upserting team '%s' (id_sofascore=%s): %s", name, sid, e)

            log.info("%s → %d teams processed", filepath.name, count)
            total += count

    log.info("Done — %d total teams upserted across all files", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    load_newerdata_teams()
