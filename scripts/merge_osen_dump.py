"""
scripts/merge_osen_dump.py
===========================
Merge new dim_player rows from Osen's dump into the main football_db.

Strategy:
    1. Restore dump into a temporary database (_osen_temp)
    2. Compare dim_player: find players in temp that don't exist in football_db
       (by canonical_name match, since IDs will differ)
    3. INSERT new players (canonical_name, nationality, birth_date, position,
       and source IDs where available)
    4. Drop the temp database when done

Usage:
    python -m scripts.merge_osen_dump                  # preview (dry run)
    python -m scripts.merge_osen_dump --execute        # actually import
    python -m scripts.merge_osen_dump --execute --all  # import all tables (players, teams, matches, etc.)
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loaders.common import DB_HOST, DB_PORT_STR, DB_NAME, DB_USER, DB_PASSWORD

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

DUMP_PATH = Path(__file__).resolve().parent.parent / "cambios_osen" / "dump"
TEMP_DB = "_osen_temp"

def _pg_env():
    """Returns env dict with PGPASSWORD set."""
    import os
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD
    return env

def create_temp_db():
    """Create temporary database and restore dump into it."""
    env = _pg_env()
    log.info("Creating temp database '%s'...", TEMP_DB)

    # Drop if exists from a previous failed run
    subprocess.run(
        ["dropdb", "-h", DB_HOST, "-p", DB_PORT_STR, "-U", DB_USER, "--if-exists", TEMP_DB],
        env=env, capture_output=True,
    )
    result = subprocess.run(
        ["createdb", "-h", DB_HOST, "-p", DB_PORT_STR, "-U", DB_USER, TEMP_DB],
        env=env, capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.error("createdb failed: %s", result.stderr)
        raise RuntimeError(f"createdb failed: {result.stderr}")

    log.info("Restoring dump into '%s' (this may take a minute)...", TEMP_DB)
    result = subprocess.run(
        ["pg_restore", "-h", DB_HOST, "-p", DB_PORT_STR, "-U", DB_USER,
         "-d", TEMP_DB, "--no-owner", "--no-privileges", "--no-comments",
         str(DUMP_PATH)],
        env=env, capture_output=True, text=True,
    )
    # pg_restore may return non-zero for warnings (e.g. missing roles) — that's OK
    if result.returncode != 0 and "error" in result.stderr.lower():
        log.warning("pg_restore warnings:\n%s", result.stderr[:500])

    log.info("Temp database restored.")


def drop_temp_db():
    """Drop the temporary database."""
    env = _pg_env()
    subprocess.run(
        ["dropdb", "-h", DB_HOST, "-p", DB_PORT_STR, "-U", DB_USER, "--if-exists", TEMP_DB],
        env=env, capture_output=True,
    )
    log.info("Temp database '%s' dropped.", TEMP_DB)


def _temp_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT_STR}/{TEMP_DB}"
    )


def _main_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT_STR}/{DB_NAME}"
    )


def merge_players(execute: bool = False) -> int:
    """Find and import new players from Osen's dump."""
    temp_eng = _temp_engine()
    main_eng = _main_engine()

    with temp_eng.connect() as tc, main_eng.connect() as mc:
        # Get all players from temp
        temp_players = tc.execute(text("""
            SELECT canonical_name, nationality, birth_date, position,
                   id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
            FROM dim_player
            ORDER BY canonical_id
        """)).mappings().fetchall()

        # Get all existing canonical_names from main (lowercased for comparison)
        existing = set()
        rows = mc.execute(text("SELECT LOWER(canonical_name) FROM dim_player")).fetchall()
        for (name,) in rows:
            existing.add(name)

        new_players = []
        for p in temp_players:
            if p["canonical_name"] and p["canonical_name"].lower() not in existing:
                new_players.append(dict(p))

        log.info("Temp DB has %d players, main DB has %d. Found %d NEW players.",
                 len(temp_players), len(existing), len(new_players))

        if not new_players:
            log.info("No new players to import.")
            return 0

        # Preview first 20
        for i, p in enumerate(new_players[:20]):
            log.info("  NEW: %s | %s | %s | pos=%s",
                     p["canonical_name"], p["nationality"] or "?",
                     p["birth_date"] or "?", p["position"] or "?")
        if len(new_players) > 20:
            log.info("  ... and %d more", len(new_players) - 20)

        if not execute:
            log.info("DRY RUN — use --execute to import these players.")
            return len(new_players)

        # Insert new players
        inserted = 0
        with main_eng.begin() as conn:
            for p in new_players:
                try:
                    conn.execute(text("""
                        INSERT INTO dim_player
                            (canonical_name, nationality, birth_date, position,
                             id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored)
                        VALUES
                            (:name, :nat, :bd, :pos,
                             :ss, :us, :tm, :sb, :ws)
                        ON CONFLICT DO NOTHING
                    """), {
                        "name": p["canonical_name"],
                        "nat":  p["nationality"],
                        "bd":   p["birth_date"],
                        "pos":  p["position"],
                        "ss":   p["id_sofascore"],
                        "us":   p["id_understat"],
                        "tm":   p["id_transfermarkt"],
                        "sb":   p["id_statsbomb"],
                        "ws":   p["id_whoscored"],
                    })
                    inserted += 1
                except Exception as e:
                    log.warning("Error inserting '%s': %s", p["canonical_name"], e)

        log.info("Inserted %d new players into football_db.", inserted)
        return inserted


def show_stats():
    """Show counts from both databases for comparison."""
    temp_eng = _temp_engine()
    main_eng = _main_engine()

    tables = ["dim_player", "dim_team", "dim_match", "dim_competition",
              "fact_shots", "fact_events", "fact_injuries", "player_review"]

    log.info("\n%-20s %10s %10s", "Table", "Main DB", "Osen DB")
    log.info("-" * 42)

    with temp_eng.connect() as tc, main_eng.connect() as mc:
        for table in tables:
            try:
                main_count = mc.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            except Exception:
                main_count = "N/A"
            try:
                temp_count = tc.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            except Exception:
                temp_count = "N/A"
            log.info("%-20s %10s %10s", table, main_count, temp_count)


def main():
    parser = argparse.ArgumentParser(description="Merge new players from Osen's dump.")
    parser.add_argument("--execute", action="store_true",
                        help="Actually insert new players (default is dry run).")
    args = parser.parse_args()

    if not DUMP_PATH.exists():
        log.error("Dump file not found: %s", DUMP_PATH)
        sys.exit(1)

    try:
        create_temp_db()
        show_stats()
        count = merge_players(execute=args.execute)
        print(f"\n{'Imported' if args.execute else 'Would import'} {count} new players.")
    finally:
        drop_temp_db()


if __name__ == "__main__":
    main()
