"""
scripts/diagnose_continental.py
================================
Diagnostic script for continental competitions (Champions League, Europa League,
Europa Conference League) in the database.

Checks:
  1. dim_competition entries and source IDs
  2. dim_match coverage per competition/season
  3. Potential team duplicates across domestic + continental
  4. WhoScored stage configuration
  5. Missing source cross-references

Usage:
    python -m scripts.diagnose_continental
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine

log = logging.getLogger(__name__)

CONTINENTAL = ["Champions League", "Europa League", "Europa Conference League"]


def _section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def check_dim_competition(conn) -> None:
    _section("1. dim_competition — continental entries")
    rows = conn.execute(text("""
        SELECT canonical_id, canonical_name, country,
               id_sofascore, id_understat, id_transfermarkt,
               id_statsbomb, id_whoscored
        FROM dim_competition
        WHERE canonical_name IN :names
        ORDER BY canonical_name
    """), {"names": tuple(CONTINENTAL)}).mappings().fetchall()

    if not rows:
        print("  [WARN] No continental competitions found in dim_competition!")
        print("         Run: python -m scripts.load_dimensions --all")
        return

    for r in rows:
        print(f"\n  {r['canonical_name']} (id={r['canonical_id']})")
        for src in ("sofascore", "understat", "transfermarkt", "statsbomb", "whoscored"):
            val = r.get(f"id_{src}")
            status = f"{val}" if val is not None else "[MISSING]"
            print(f"    {src:15s}: {status}")


def check_match_coverage(conn) -> None:
    _section("2. dim_match — coverage per competition/season")
    rows = conn.execute(text("""
        SELECT m.competition, m.season,
               COUNT(*) AS matches,
               COUNT(m.match_date) AS with_date,
               COUNT(m.id_sofascore) AS ss,
               COUNT(m.id_whoscored) AS ws,
               COUNT(m.id_understat) AS us,
               COUNT(m.id_statsbomb) AS sb
        FROM dim_match m
        WHERE m.competition IN :names
        GROUP BY m.competition, m.season
        ORDER BY m.competition, m.season
    """), {"names": tuple(CONTINENTAL)}).mappings().fetchall()

    if not rows:
        print("  No matches found for continental competitions.")
        return

    for r in rows:
        print(f"\n  {r['competition']} {r['season']}: {r['matches']} matches "
              f"(dates: {r['with_date']})")
        print(f"    Sources: SS={r['ss']} WS={r['ws']} US={r['us']} SB={r['sb']}")


def check_team_duplicates(conn) -> None:
    _section("3. Potential team duplicates (domestic vs continental)")
    rows = conn.execute(text("""
        SELECT canonical_name, COUNT(*) AS cnt
        FROM dim_team
        GROUP BY canonical_name
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 20
    """)).mappings().fetchall()

    if not rows:
        print("  No duplicate team names found. Good.")
        return

    print(f"  Found {len(rows)} team names with multiple entries:")
    for r in rows:
        print(f"    {r['canonical_name']}: {r['cnt']} entries")
        details = conn.execute(text("""
            SELECT canonical_id, id_sofascore, id_whoscored, id_transfermarkt
            FROM dim_team WHERE canonical_name = :name
        """), {"name": r["canonical_name"]}).mappings().fetchall()
        for d in details:
            print(f"      id={d['canonical_id']} SS={d['id_sofascore']} "
                  f"WS={d['id_whoscored']} TM={d['id_transfermarkt']}")


def check_whoscored_stages(conn) -> None:
    _section("4. WhoScored stage configuration")
    try:
        from scrapers.whoscored_scraper import WHOSCORED_STAGES
    except ImportError:
        print("  [WARN] Could not import WHOSCORED_STAGES")
        return

    for comp in CONTINENTAL:
        stages = {k: v for k, v in WHOSCORED_STAGES.items() if k[0] == comp}
        if not stages:
            print(f"  {comp}: no WhoScored stages configured")
        else:
            for (c, s), info in sorted(stages.items()):
                n_stages = len(info.get("stages", []))
                print(f"  {c} {s}: {n_stages} stages")


def check_missing_crossrefs(conn) -> None:
    _section("5. Matches missing source cross-references")
    rows = conn.execute(text("""
        SELECT m.competition, m.season,
               SUM(CASE WHEN m.id_sofascore IS NULL THEN 1 ELSE 0 END) AS no_ss,
               SUM(CASE WHEN m.id_whoscored IS NULL THEN 1 ELSE 0 END) AS no_ws,
               COUNT(*) AS total
        FROM dim_match m
        WHERE m.competition IN :names
        GROUP BY m.competition, m.season
        ORDER BY m.competition, m.season
    """), {"names": tuple(CONTINENTAL)}).mappings().fetchall()

    if not rows:
        print("  No continental matches to check.")
        return

    for r in rows:
        print(f"  {r['competition']} {r['season']}: "
              f"{r['total']} total | "
              f"no SofaScore: {r['no_ss']} | no WhoScored: {r['no_ws']}")


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    print("Continental competitions diagnostic")
    print(f"Checking: {', '.join(CONTINENTAL)}")

    with engine.connect() as conn:
        check_dim_competition(conn)
        check_match_coverage(conn)
        check_team_duplicates(conn)
        check_whoscored_stages(conn)
        check_missing_crossrefs(conn)

    print(f"\n{'=' * 60}")
    print("  Diagnostic complete.")
    print(f"{'=' * 60}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
