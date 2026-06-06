"""
scrapers/repair_stadium_coords.py
==================================
Resolve missing stadium lat/lon via Wikidata for venues blocking weather data.

The main failure mode is ambiguous stadium names (e.g. "Artemio Franchi" resolves
to a person, not the Fiorentina ground). This script uses club home venue (P115),
stadium-specific search queries, and entity scoring to find the correct QID + P625.

Usage:
    python -m scrapers.repair_stadium_coords --analyze
    python -m scrapers.repair_stadium_coords --export data/.cache/stadiums_missing_coords.csv
    python -m scrapers.repair_stadium_coords --fix --geocode-only
    python -m scrapers.repair_stadium_coords --fix --limit 5
    python -m scrapers.repair_stadium_coords --fix --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    MISSING_COORDS_SQL,
    _resolve_geocode_country,
    enrich_stadiums_missing_coords,
    geocode_stadium_fallback,
    resolve_stadium_coords_with_fallback,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EXPORT = PROJECT_ROOT / "data" / ".cache" / "stadiums_missing_coords.csv"

log = logging.getLogger(__name__)


def analyze_missing_coords() -> None:
    with engine.connect() as conn:
        rows = conn.execute(text(MISSING_COORDS_SQL)).mappings().fetchall()

    if not rows:
        print("No stadiums missing coordinates for weather-blocked matches.")
        return

    total_blocked = sum(int(r["blocked_matches"]) for r in rows)
    print(f"\n=== Stadiums missing coordinates ({len(rows)} venues, {total_blocked} matches) ===\n")
    print(f"{'Team':<24} {'Blocked':>7}  {'Country':<12} Stadium")
    print("-" * 80)
    for row in rows[:40]:
        name = row.get("stadium_name") or "?"
        team = row.get("team") or row.get("team_slug") or "?"
        geo_country = _resolve_geocode_country(dict(row)) or "-"
        print(f"{team:<24} {int(row['blocked_matches']):>7}  {geo_country:<12} {name}")
    if len(rows) > 40:
        print(f"... and {len(rows) - 40} more")


def export_missing_coords(path: Path, geocode_probe: bool = False) -> None:
    """Write full list of teams missing coords (weather-blocked homes)."""
    with engine.connect() as conn:
        rows = conn.execute(text(MISSING_COORDS_SQL)).mappings().fetchall()

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "canonical_team_id", "stadium_id", "team", "stadium_name",
        "city", "country", "domestic_country", "competition_country",
        "blocked_matches", "wikidata_qid",
        "probe_latitude", "probe_longitude",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            rd = dict(row)
            geo_country = _resolve_geocode_country(rd)
            out = {
                "canonical_team_id": rd.get("canonical_team_id"),
                "stadium_id": rd.get("stadium_id"),
                "team": rd.get("team") or rd.get("team_slug"),
                "stadium_name": rd.get("stadium_name"),
                "city": rd.get("city"),
                "country": geo_country,
                "domestic_country": rd.get("domestic_country"),
                "competition_country": rd.get("competition_country"),
                "blocked_matches": rd.get("blocked_matches"),
                "wikidata_qid": rd.get("wikidata_qid"),
            }
            if geocode_probe:
                g = geocode_stadium_fallback(
                    rd.get("stadium_name") or "",
                    team=out["team"] or "",
                    city=rd.get("city") or "",
                    country=geo_country,
                )
                out["probe_latitude"] = g.get("latitude")
                out["probe_longitude"] = g.get("longitude")
            else:
                out["probe_latitude"] = None
                out["probe_longitude"] = None
            writer.writerow(out)
    print(f"Exported {len(rows)} teams -> {path}")


def preview_resolution(limit: int = 5, geocode_only: bool = False) -> None:
    with engine.connect() as conn:
        rows = conn.execute(
            text(MISSING_COORDS_SQL + " LIMIT :limit"),
            {"limit": limit},
        ).mappings().fetchall()

    print(f"\n=== Resolution preview ({len(rows)} stadiums) ===\n")
    for row in rows:
        country = _resolve_geocode_country(dict(row))
        data = resolve_stadium_coords_with_fallback(
            row.get("stadium_name") or "",
            team=row.get("team") or row.get("team_slug") or "",
            existing_qid=row.get("wikidata_qid"),
            city=row.get("city") or "",
            country=country,
            use_wikidata=not geocode_only,
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
    parser.add_argument("--export", type=Path, nargs="?", const=DEFAULT_EXPORT,
                        help="Export full CSV (optional probe geocoding).")
    parser.add_argument("--probe", action="store_true",
                        help="With --export, test Nominatim per row (slow).")
    parser.add_argument("--preview", action="store_true", help="Test resolution without DB writes.")
    parser.add_argument("--fix", action="store_true", help="Fetch coords and update dim_stadium.")
    parser.add_argument("--geocode-only", action="store_true",
                        help="Use Nominatim only (recommended; avoids Wikidata 429).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    if not args.analyze and not args.preview and not args.fix and args.export is None:
        args.analyze = True

    if args.analyze:
        analyze_missing_coords()

    if args.export is not None:
        export_missing_coords(args.export, geocode_probe=args.probe)

    if args.preview:
        preview_resolution(limit=args.limit or 5, geocode_only=args.geocode_only)

    if args.fix:
        updated = enrich_stadiums_missing_coords(
            engine,
            dry_run=args.dry_run,
            limit=args.limit,
            weather_gaps_only=True,
            use_wikidata=not args.geocode_only,
        )
        print(f"\nStadiums updated: {updated}" + (" (dry-run)" if args.dry_run else ""))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
