"""
scrapers/repair_stadium_coords.py
==================================
Resolve missing stadium lat/lon via Wikidata for venues blocking weather data.

The main failure mode is ambiguous stadium names (e.g. "Artemio Franchi" resolves
to a person, not the Fiorentina ground). This script uses club home venue (P115),
stadium-specific search queries, and entity scoring to find the correct QID + P625.

Usage:
    python -m scrapers.repair_stadium_coords --analyze
    python -m scrapers.repair_stadium_coords --fix
    python -m scrapers.repair_stadium_coords --fix --limit 5
    python -m scrapers.repair_stadium_coords --fix --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    MISSING_COORDS_SQL,
    enrich_stadiums_missing_coords,
    resolve_stadium_coords,
)

log = logging.getLogger(__name__)


def analyze_missing_coords() -> None:
    with engine.connect() as conn:
        rows = conn.execute(text(MISSING_COORDS_SQL)).mappings().fetchall()

    if not rows:
        print("No stadiums missing coordinates for weather-blocked matches.")
        return

    total_blocked = sum(int(r["blocked_matches"]) for r in rows)
    print(f"\n=== Stadiums missing coordinates ({len(rows)} venues, {total_blocked} matches) ===\n")
    print(f"{'Team':<24} {'Blocked':>7}  {'QID':<10} Stadium")
    print("-" * 72)
    for row in rows[:40]:
        qid = row.get("wikidata_qid") or "-"
        name = row.get("stadium_name") or "?"
        team = row.get("team") or row.get("team_slug") or "?"
        print(f"{team:<24} {int(row['blocked_matches']):>7}  {qid:<10} {name}")
    if len(rows) > 40:
        print(f"... and {len(rows) - 40} more")


def preview_resolution(limit: int = 5) -> None:
    with engine.connect() as conn:
        rows = conn.execute(
            text(MISSING_COORDS_SQL + " LIMIT :limit"),
            {"limit": limit},
        ).mappings().fetchall()

    print(f"\n=== Resolution preview ({len(rows)} stadiums) ===\n")
    for row in rows:
        data = resolve_stadium_coords(
            row.get("stadium_name") or "",
            team=row.get("team") or row.get("team_slug") or "",
            existing_qid=row.get("wikidata_qid"),
        )
        lat, lon = data.get("latitude"), data.get("longitude")
        status = f"lat={lat:.4f} lon={lon:.4f}" if lat and lon else "NOT FOUND"
        print(
            f"{row.get('team')}: {row.get('stadium_name')}  "
            f"old_qid={row.get('wikidata_qid')} -> {data.get('wikidata_qid')}  {status}"
        )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Repair stadium coordinates from Wikidata for weather-blocked venues.",
    )
    parser.add_argument("--analyze", action="store_true", help="List stadiums missing coords.")
    parser.add_argument("--preview", action="store_true", help="Test Wikidata resolution without DB writes.")
    parser.add_argument("--fix", action="store_true", help="Fetch coords and update dim_stadium.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if not args.analyze and not args.preview and not args.fix:
        args.analyze = True

    if args.analyze:
        analyze_missing_coords()

    if args.preview:
        preview_resolution(limit=args.limit or 5)

    if args.fix:
        updated = enrich_stadiums_missing_coords(
            engine,
            dry_run=args.dry_run,
            limit=args.limit,
            weather_gaps_only=True,
        )
        print(f"\nStadiums updated: {updated}" + (" (dry-run)" if args.dry_run else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
