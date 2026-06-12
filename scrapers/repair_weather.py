"""
scrapers/repair_weather.py
===========================
Analyze and repair missing weather data in dim_match.

Most gaps are caused by home stadiums without lat/lon (needs Wikidata first).
A smaller subset only needs weather fetch (coords already present).

Usage:
    python -m scrapers.repair_weather --analyze
    python -m scrapers.repair_weather --fix
    python -m scrapers.repair_weather --fix --skip-coords
    python -m scrapers.repair_weather --export data/.cache/weather_gaps.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from scrapers.weather_scraper import Provider, enrich_weather
from scrapers.wikidata_stadium_enricher import enrich_stadiums_missing_coords

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT_PATH = PROJECT_ROOT / "data" / ".cache" / "weather_gaps_report.json"

SUMMARY_SQL = text("""
    SELECT
        COUNT(*) FILTER (WHERE m.match_date IS NOT NULL) AS total_matches,
        COUNT(*) FILTER (WHERE m.temperature_c IS NOT NULL) AS with_weather,
        COUNT(*) FILTER (
            WHERE m.match_date IS NOT NULL AND m.temperature_c IS NULL
        ) AS missing_weather,
        COUNT(*) FILTER (
            WHERE m.match_date IS NOT NULL
              AND m.temperature_c IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM dim_stadium s
                  WHERE s.stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
                    AND s.latitude IS NOT NULL
                    AND s.longitude IS NOT NULL
              )
        ) AS fillable_now,
        COUNT(*) FILTER (
            WHERE m.match_date IS NOT NULL
              AND m.temperature_c IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM dim_stadium s
                  WHERE s.stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
                    AND s.latitude IS NOT NULL
                    AND s.longitude IS NOT NULL
              )
        ) AS blocked_no_coords
    FROM dim_match m
""")

BY_COMPETITION_SQL = text("""
    SELECT COALESCE(c.canonical_name, m.competition, 'unknown') AS competition,
           COUNT(*) AS missing_matches,
           MIN(m.match_date) AS first_date,
           MAX(m.match_date) AS last_date
    FROM dim_match m
    LEFT JOIN dim_competition c ON m.competition_id = c.canonical_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
    GROUP BY 1
    ORDER BY missing_matches DESC, competition
""")

BY_TEAM_SQL = text("""
    SELECT ht.canonical_name AS home_team,
           COUNT(*) AS missing_matches,
           MIN(m.match_date) AS first_date,
           MAX(m.match_date) AS last_date,
           BOOL_OR(
               EXISTS (
                   SELECT 1
                   FROM dim_stadium s
                   WHERE s.stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
                     AND s.latitude IS NOT NULL
                     AND s.longitude IS NOT NULL
               )
           ) AS has_coords
    FROM dim_match m
    JOIN dim_team ht ON m.home_team_id = ht.canonical_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
    GROUP BY ht.canonical_name
    ORDER BY missing_matches DESC, home_team
""")

BY_MONTH_SQL = text("""
    SELECT DATE_TRUNC('month', m.match_date)::date AS month,
           COUNT(*) AS missing_matches
    FROM dim_match m
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
    GROUP BY 1
    ORDER BY 1
""")

BLOCKING_STADIUMS_SQL = text("""
    SELECT s.stadium_id,
           s.stadium_name,
           s.team_slug,
           COALESCE(t.canonical_name, s.team_slug) AS team,
           COUNT(m.match_id) AS blocked_matches,
           MIN(m.match_date) AS first_match,
           MAX(m.match_date) AS last_match
    FROM dim_match m
    JOIN dim_stadium s ON s.stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
    LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
      AND (s.latitude IS NULL OR s.longitude IS NULL)
    GROUP BY s.stadium_id, s.stadium_name, s.team_slug, t.canonical_name
    ORDER BY blocked_matches DESC, team
""")

FILLABLE_RECENT_DAYS = 21

FILLABLE_RECENT_SQL = text("""
    SELECT COUNT(*) AS recent_fillable
    FROM dim_match m
    JOIN LATERAL (
        SELECT 1
        FROM dim_stadium s
        WHERE s.stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
          AND s.latitude IS NOT NULL
          AND s.longitude IS NOT NULL
        LIMIT 1
    ) s ON TRUE
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
      AND m.match_date >= CURRENT_DATE - :recent_days
""")

FILLABLE_MATCHES_SQL = text("""
    SELECT m.match_id,
           m.match_date,
           ht.canonical_name AS home_team,
           s.latitude,
           s.longitude
    FROM dim_match m
    JOIN dim_team ht ON m.home_team_id = ht.canonical_id
    JOIN LATERAL (
        SELECT latitude, longitude
        FROM dim_stadium
        WHERE stadium_id = COALESCE(m.match_stadium_id, m.stadium_id)
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        LIMIT 1
    ) s ON TRUE
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
    ORDER BY m.match_date, m.match_id
""")


@dataclass
class WeatherGapReport:
    summary: dict[str, int]
    by_competition: list[dict[str, Any]] = field(default_factory=list)
    by_team: list[dict[str, Any]] = field(default_factory=list)
    by_month: list[dict[str, Any]] = field(default_factory=list)
    blocking_stadiums: list[dict[str, Any]] = field(default_factory=list)
    fillable_matches: list[dict[str, Any]] = field(default_factory=list)


def _serialize(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "__float__"):
        try:
            return float(value)
        except (TypeError, ValueError):
            return value
    return value


def _row_dict(row) -> dict[str, Any]:
    return {key: _serialize(val) for key, val in row._mapping.items()}


def analyze_weather_gaps(limit_fillable: Optional[int] = None) -> WeatherGapReport:
    with engine.connect() as conn:
        summary = _row_dict(conn.execute(SUMMARY_SQL).one())
        recent = int(conn.execute(
            FILLABLE_RECENT_SQL,
            {"recent_days": FILLABLE_RECENT_DAYS},
        ).scalar() or 0)
        summary["fillable_recent_no_api"] = recent
        summary["fillable_historical"] = max(
            int(summary.get("fillable_now", 0)) - recent, 0,
        )
        by_competition = [_row_dict(r) for r in conn.execute(BY_COMPETITION_SQL)]
        by_team = [_row_dict(r) for r in conn.execute(BY_TEAM_SQL)]
        by_month = [_row_dict(r) for r in conn.execute(BY_MONTH_SQL)]
        blocking_stadiums = [_row_dict(r) for r in conn.execute(BLOCKING_STADIUMS_SQL)]

        fillable_sql = FILLABLE_MATCHES_SQL.text
        params: dict[str, Any] = {}
        if limit_fillable:
            fillable_sql += " LIMIT :limit"
            params["limit"] = limit_fillable
        fillable_matches = [
            _row_dict(r) for r in conn.execute(text(fillable_sql), params)
        ]

    return WeatherGapReport(
        summary={k: int(v) for k, v in summary.items()},
        by_competition=by_competition,
        by_team=by_team,
        by_month=by_month,
        blocking_stadiums=blocking_stadiums,
        fillable_matches=fillable_matches,
    )


def print_report(report: WeatherGapReport) -> None:
    s = report.summary
    coverage = 100.0 * s["with_weather"] / s["total_matches"] if s["total_matches"] else 0.0
    print("\n=== Weather coverage ===")
    print(f"  Matches with date:     {s['total_matches']}")
    print(f"  With weather:          {s['with_weather']} ({coverage:.1f}%)")
    print(f"  Missing weather:       {s['missing_weather']}")
    print(f"  Fillable now:          {s['fillable_now']}  (coords OK, need fetch)")
    if s.get("fillable_recent_no_api"):
        print(
            f"    -> recent ({FILLABLE_RECENT_DAYS}d): {s['fillable_recent_no_api']}  "
            "(NASA may not publish hourly data yet)"
        )
    print(f"  Blocked (no coords):   {s['blocked_no_coords']}")

    if report.by_competition:
        print("\n=== Missing by competition ===")
        for row in report.by_competition[:12]:
            print(
                f"  {row['competition']:<22} {int(row['missing_matches']):>5}  "
                f"{row['first_date']} .. {row['last_date']}"
            )

    if report.blocking_stadiums:
        print("\n=== Stadiums blocking enrichment (no lat/lon) ===")
        for row in report.blocking_stadiums[:12]:
            name = row.get("stadium_name") or row.get("team") or "?"
            print(
                f"  {row['team']:<22} {int(row['blocked_matches']):>5} matches  "
                f"stadium={name!r}  {row['first_match']} .. {row['last_match']}"
            )
        remaining = len(report.blocking_stadiums) - 12
        if remaining > 0:
            print(f"  ... and {remaining} more stadiums")

    teams_no_coords = [r for r in report.by_team if not r.get("has_coords")]
    if teams_no_coords:
        print("\n=== Top teams missing weather (no stadium coords) ===")
        for row in teams_no_coords[:10]:
            print(
                f"  {row['home_team']:<22} {int(row['missing_matches']):>5}  "
                f"{row['first_date']} .. {row['last_date']}"
            )

    if report.fillable_matches:
        print("\n=== Ready to fetch (sample) ===")
        for row in report.fillable_matches[:8]:
            print(
                f"  match_id={row['match_id']}  {row['match_date']}  "
                f"{row['home_team']}  ({row['latitude']}, {row['longitude']})"
            )
        extra = s["fillable_now"] - len(report.fillable_matches)
        if extra > 0:
            print(f"  ... and {extra} more fillable matches")


def save_report(report: WeatherGapReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(asdict(report), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Report saved to %s", path)


def export_fillable_csv(report: WeatherGapReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with engine.connect() as conn:
        rows = [_row_dict(r) for r in conn.execute(FILLABLE_MATCHES_SQL)]

    if not rows:
        log.info("No fillable matches to export.")
        return

    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log.info("Exported %d fillable matches to %s", len(rows), path)


def repair_stadium_coords(
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> int:
    return enrich_stadiums_missing_coords(
        engine,
        dry_run=dry_run,
        limit=limit,
        weather_gaps_only=True,
    )


def repair_weather_gaps(
    dry_run: bool = False,
    skip_coords: bool = False,
    skip_weather: bool = False,
    stadium_limit: Optional[int] = None,
    weather_limit: Optional[int] = None,
    provider: Provider = "nasa",
    request_delay: Optional[float] = None,
) -> dict[str, int]:
    before = analyze_weather_gaps()
    print_report(before)

    result = {"stadiums_updated": 0, "matches_updated": 0}

    if not skip_coords and before.summary["blocked_no_coords"] > 0:
        log.info("Step 1/2: Wikidata coords for blocking stadiums")
        result["stadiums_updated"] = repair_stadium_coords(
            dry_run=dry_run,
            limit=stadium_limit,
        )
    elif skip_coords:
        log.info("Skipping stadium coord repair (--skip-coords)")
    else:
        log.info("No stadium coord repair needed")

    if not skip_weather:
        after_coords = analyze_weather_gaps()
        fillable = after_coords.summary["fillable_now"]
        if fillable == 0:
            log.info("Step 2/2: no matches ready for weather fetch")
        else:
            log.info("Step 2/2: weather fetch for %d match(es)", fillable)
            result["matches_updated"] = enrich_weather(
                dry_run=dry_run,
                limit=weather_limit,
                provider=provider,
                request_delay=request_delay,
            )
    else:
        log.info("Skipping weather fetch (--skip-weather)")

    after = analyze_weather_gaps()
    print("\n=== After repair ===")
    print_report(after)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Analyze and repair missing weather data in dim_match.",
    )
    parser.add_argument("--analyze", action="store_true", help="Print gap report only.")
    parser.add_argument("--fix", action="store_true", help="Repair coords then fetch weather.")
    parser.add_argument("--skip-coords", action="store_true", help="With --fix: skip Wikidata step.")
    parser.add_argument("--skip-weather", action="store_true", help="With --fix: skip weather fetch.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stadium-limit", type=int, default=None)
    parser.add_argument("--weather-limit", type=int, default=None)
    parser.add_argument("--provider", choices=("nasa", "openmeteo"), default="nasa")
    parser.add_argument("--delay", type=float, default=None)
    parser.add_argument(
        "--save-report",
        type=Path,
        default=None,
        help=f"Save JSON report (default with --analyze: {DEFAULT_REPORT_PATH}).",
    )
    parser.add_argument(
        "--export",
        type=Path,
        default=None,
        help="Export fillable matches (coords OK) to CSV.",
    )
    args = parser.parse_args()

    if not args.analyze and not args.fix and not args.export:
        args.analyze = True

    report = analyze_weather_gaps()

    if args.analyze and not args.fix:
        print_report(report)

    save_path = args.save_report
    if save_path is None and args.analyze and not args.fix:
        save_path = DEFAULT_REPORT_PATH
    if save_path:
        save_report(report, save_path)

    if args.export:
        export_fillable_csv(report, args.export)

    if args.fix:
        result = repair_weather_gaps(
            dry_run=args.dry_run,
            skip_coords=args.skip_coords,
            skip_weather=args.skip_weather,
            stadium_limit=args.stadium_limit,
            weather_limit=args.weather_limit,
            provider=args.provider,
            request_delay=args.delay,
        )
        print(
            f"\nRepair done. stadiums={result['stadiums_updated']}  "
            f"matches={result['matches_updated']}"
            + (" (dry-run)" if args.dry_run else "")
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
