"""
Vincula cada partido con el estadio real (match_stadium_id), independiente del equipo local.

Fuentes (prioridad):
  1. JSON SofaScore  data/raw/attendance/{id_sofascore}.json  (nombre + coords)
  2. CSV WhoScored   data/clean/**/whoscored/match_enrichment.csv
  3. dim_match.venue_name ya cargado

No usa el estadio del equipo local salvo con --allow-home-fallback (legacy stadium_id).

Uso:
    python -m scripts.backfill_stadium_match --dry-run
    python -m scripts.backfill_stadium_match --force
    python -m scripts.backfill_stadium_match --data-root "C:/Users/Ivan/Desktop/football_scraping_backup"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from loaders.common import engine
from loaders.match_stadium_resolver import (
    DEFAULT_DATA_ROOT,
    MatchStadiumResolver,
    VenueInfo,
    load_sofascore_venues,
    load_whoscored_venues,
)

log = logging.getLogger(__name__)

MIGRATION = Path(__file__).resolve().parents[1] / "db" / "migrations" / "add_match_stadium_id.sql"


def _ensure_schema(conn) -> None:
    conn.execute(text(
        "ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS match_stadium_id INTEGER "
        "REFERENCES dim_stadium (stadium_id) ON DELETE SET NULL"
    ))
    conn.execute(text(
        "ALTER TABLE dim_match ADD COLUMN IF NOT EXISTS match_venue_source VARCHAR(32)"
    ))
    conn.execute(text(
        "CREATE INDEX IF NOT EXISTS idx_match_match_stadium ON dim_match (match_stadium_id)"
    ))


def backfill(
    *,
    dry_run: bool = False,
    force: bool = False,
    data_root: Path = DEFAULT_DATA_ROOT,
    allow_home_fallback: bool = False,
) -> dict[str, int]:
    ss_venues = load_sofascore_venues(data_root)
    ws_venues = load_whoscored_venues(data_root)

    stats = {
        "sofascore_json": 0,
        "whoscored_csv": 0,
        "venue_name": 0,
        "home_fallback": 0,
        "none": 0,
        "changed_vs_stadium_id": 0,
    }
    updates: list[dict] = []

    with engine.begin() as conn:
        _ensure_schema(conn)

        before = conn.execute(text(
            "SELECT COUNT(*) FROM dim_match WHERE match_stadium_id IS NOT NULL"
        )).scalar()

        resolver = MatchStadiumResolver(conn)
        where = "TRUE" if force else "m.match_stadium_id IS NULL"
        matches = conn.execute(text(f"""
            SELECT m.match_id, m.id_sofascore, m.id_whoscored, m.venue_name,
                   m.home_team_id, m.season, m.stadium_id
            FROM dim_match m
            WHERE {where}
            ORDER BY m.match_id
        """)).mappings().all()

        total = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
        log.info("Partidos a evaluar: %d / %d", len(matches), total)

        def _find_home_stadium(team_id: int, season: str) -> int | None:
            row = conn.execute(text("""
                SELECT stadium_id FROM dim_stadium
                WHERE canonical_team_id = :tid
                  AND valid_from_season <= :season AND valid_to_season >= :season
                ORDER BY valid_to_season DESC
                LIMIT 1
            """), {"tid": team_id, "season": season}).scalar()
            if row:
                return int(row)
            return conn.execute(text("""
                SELECT stadium_id FROM dim_stadium
                WHERE canonical_team_id = :tid
                ORDER BY valid_to_season DESC LIMIT 1
            """), {"tid": team_id}).scalar()

        for m in matches:
            venue: VenueInfo | None = None
            source: str | None = None

            ss_key = str(int(m["id_sofascore"])) if m["id_sofascore"] else None
            if ss_key and ss_key in ss_venues:
                venue = ss_venues[ss_key]
                source = "sofascore_json"

            if venue is None and m["id_whoscored"]:
                try:
                    ws_id = int(m["id_whoscored"])
                except (TypeError, ValueError):
                    ws_id = None
                if ws_id and ws_id in ws_venues:
                    venue = VenueInfo(name=ws_venues[ws_id])
                    source = "whoscored_csv"

            if venue is None and m["venue_name"] and str(m["venue_name"]).strip():
                venue = VenueInfo(name=str(m["venue_name"]).strip())
                source = "venue_name"

            match_stadium_id = None
            if venue:
                match_stadium_id = resolver.resolve(venue, dry_run=dry_run)
                if match_stadium_id == -1:
                    match_stadium_id = None

            if match_stadium_id is None and allow_home_fallback:
                if m["home_team_id"] and m["season"]:
                    match_stadium_id = _find_home_stadium(m["home_team_id"], m["season"])
                    if match_stadium_id:
                        source = "home_fallback"

            if match_stadium_id and source:
                stats[source] += 1
                if m["stadium_id"] and int(m["stadium_id"]) != int(match_stadium_id):
                    stats["changed_vs_stadium_id"] += 1
                updates.append({
                    "match_id": m["match_id"],
                    "match_stadium_id": match_stadium_id,
                    "match_venue_source": source,
                    "venue_name": venue.name if venue else m["venue_name"],
                })
            else:
                stats["none"] += 1

        if not dry_run:
            for u in updates:
                conn.execute(text("""
                    UPDATE dim_match
                    SET match_stadium_id = :match_stadium_id,
                        match_venue_source = :match_venue_source,
                        venue_name = COALESCE(NULLIF(TRIM(venue_name), ''), :venue_name)
                    WHERE match_id = :match_id
                """), u)

        after = conn.execute(text(
            "SELECT COUNT(*) FROM dim_match WHERE match_stadium_id IS NOT NULL"
        )).scalar()

    print(f"\n{'=' * 60}")
    print(f"  {'DRY-RUN — ' if dry_run else ''}match_stadium_id en dim_match")
    print(f"{'=' * 60}")
    print(f"  Data root:                {data_root}")
    print(f"  SofaScore JSON venues:    {len(ss_venues):,}")
    print(f"  WhoScored CSV venues:     {len(ws_venues):,}")
    print(f"  Partidos evaluados:       {len(matches):,}")
    print(f"  Ya tenían match_stadium:  {before:,}")
    print(f"  ── Resueltos ──")
    print(f"    sofascore_json:         {stats['sofascore_json']:,}")
    print(f"    whoscored_csv:          {stats['whoscored_csv']:,}")
    print(f"    venue_name:             {stats['venue_name']:,}")
    if allow_home_fallback:
        print(f"    home_fallback:          {stats['home_fallback']:,}")
    print(f"    sin resolver:           {stats['none']:,}")
    print(f"  Distintos de stadium_id: {stats['changed_vs_stadium_id']:,}")
    print(f"  Con match_stadium ahora:  {after:,} / {total:,}")
    if total:
        print(f"  Cobertura:                {100 * after / total:.1f}%")
    print(f"{'=' * 60}\n")
    return stats


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(
        description="Backfill dim_match.match_stadium_id desde JSON/CSV (sin depender del local).",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true",
                        help="Re-evalúa todos los partidos.")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT,
                        help=f"Raíz del backup con data/ (default: {DEFAULT_DATA_ROOT})")
    parser.add_argument("--allow-home-fallback", action="store_true",
                        help="Último recurso: estadio del equipo local.")
    args = parser.parse_args()
    backfill(
        dry_run=args.dry_run,
        force=args.force,
        data_root=args.data_root,
        allow_home_fallback=args.allow_home_fallback,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
