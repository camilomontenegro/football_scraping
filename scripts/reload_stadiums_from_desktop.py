"""
Recarga dim_stadium desde Desktop/stadiums con SCD2 corregido.

1. Borra filas transfermarkt (conserva synthetic-geocode + Wikidata).
2. Carga todos los CSV en orden cronológico de temporada.
3. Compacta filas adyacentes con mismo hash.
4. Re-enlaza dim_match.stadium_id.

    python -m scripts.reload_stadiums_from_desktop --dry-run
    python -m scripts.reload_stadiums_from_desktop
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from sqlalchemy import text

from loaders.common import engine
from loaders.stadium_loader import load_stadiums

log = logging.getLogger(__name__)

STADIUMS_CLEAN = Path(r"C:\Users\Ivan\Desktop\stadiums\clean")

# Solo competiciones activas del proyecto (excluye Conference League)
WORKING_SLUGS = {
    "la_liga", "premier_league", "bundesliga", "serie_a", "ligue_1",
    "primeira_liga", "eredivisie", "champions_league", "europa_league",
}


def purge_transfermarkt_rows(dry_run: bool) -> int:
    with engine.begin() as conn:
        n = conn.execute(text(
            "SELECT COUNT(*) FROM dim_stadium WHERE data_source = 'transfermarkt'"
        )).scalar()
        if dry_run:
            print(f"  [dry-run] borraría {n} filas transfermarkt")
            return n
        conn.execute(text(
            "DELETE FROM dim_stadium WHERE data_source = 'transfermarkt'"
        ))
        print(f"  eliminadas {n} filas transfermarkt")
        return n


def load_all_csvs(dry_run: bool) -> int:
    if not STADIUMS_CLEAN.exists():
        raise SystemExit(f"No existe {STADIUMS_CLEAN}")

    files = sorted(
        STADIUMS_CLEAN.glob("*/*/transfermarkt/stadiums.csv"),
        key=lambda p: (p.parts[-3], p.parts[-4], str(p)),
    )
    files = [f for f in files if f.parts[-4] in WORKING_SLUGS]
    print(f"  CSV a cargar: {len(files)}")
    if dry_run:
        for f in files:
            print(f"    {f.relative_to(STADIUMS_CLEAN)}")
        return 0

    total = 0
    with engine.begin() as conn:
        total = load_stadiums(conn, tm_path=STADIUMS_CLEAN)
    return total


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-backfill", action="store_true")
    args = ap.parse_args()

    print("=== Recarga dim_stadium desde Desktop/stadiums ===\n")
    print("[1/4] Purga filas transfermarkt")
    purge_transfermarkt_rows(args.dry_run)

    print("\n[2/4] Carga CSV (SCD2 cronológico)")
    n = load_all_csvs(args.dry_run)
    print(f"  filas afectadas: {n}")

    if args.dry_run:
        print("\n[dry-run] omitiendo compact + backfill")
        return 0

    print("\n[3/4] Compact dim_stadium")
    subprocess.run(
        [sys.executable, "-m", "scripts.compact_dim_stadium"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )

    print("\n[3b/4] Extender cobertura temporal + coords")
    subprocess.run(
        [sys.executable, "-m", "scripts.extend_stadium_season_coverage"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    subprocess.run(
        [sys.executable, "-m", "scripts.restore_stadium_coords_from_export"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    subprocess.run(
        [sys.executable, "-m", "scrapers.repair_stadium_coords", "--fix", "--geocode-only"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    subprocess.run(
        [sys.executable, "-m", "scripts.fill_timezone_offline", "--apply"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    subprocess.run(
        [sys.executable, "-m", "scripts.backfill_synthetic_stadiums"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )
    subprocess.run(
        [sys.executable, "-m", "scripts.finalize_dim_stadium"],
        check=False,
        cwd=str(Path(__file__).resolve().parents[1]),
    )

    if not args.skip_backfill:
        print("\n[4/4] Backfill stadium_id en partidos")
        subprocess.run(
            [sys.executable, "-m", "scripts.backfill_stadium_match", "--force"],
            check=False,
            cwd=str(Path(__file__).resolve().parents[1]),
        )

    with engine.connect() as c:
        rows = c.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        multi = c.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT canonical_team_id FROM dim_stadium
                GROUP BY canonical_team_id HAVING COUNT(*) > 1
            ) x
        """)).scalar()
        tm = c.execute(text(
            "SELECT COUNT(*) FROM dim_stadium WHERE data_source = 'transfermarkt'"
        )).scalar()
        cap = c.execute(text(
            "SELECT COUNT(*) FROM dim_stadium WHERE capacity IS NOT NULL"
        )).scalar()
        print(f"\n=== Resultado: {rows} filas | TM={tm} | multi-SCD2={multi} | con aforo={cap} ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
