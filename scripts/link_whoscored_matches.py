"""
scripts/link_whoscored_matches.py
==================================
Enlaza dim_match.id_whoscored desde match_enrichment.csv (home/away WS reales)
en todas las raíces clean/ disponibles.

Uso:
    python -m scripts.link_whoscored_matches
    python -m scripts.link_whoscored_matches --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loaders.common import engine, safe_read_csv
from utils.data_paths import season_db_format, slugify_competition

log = logging.getLogger(__name__)

CLEAN_ROOTS = [
    ROOT / "data" / "clean",
    Path(r"C:\Users\Ivan\Desktop\football_scraping_data\clean"),
    Path(r"C:\Users\Ivan\Desktop\football_scraping_backup\data\clean"),
]


def _parse_score(val) -> tuple[int | None, int | None]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    m = re.match(r"(\d+)\s*:\s*(\d+)", str(val).strip())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _comp_id_map(conn) -> dict[str, int]:
    rows = conn.execute(text(
        "SELECT canonical_id, canonical_name FROM dim_competition"
    )).fetchall()
    return {slugify_competition(name): cid for cid, name in rows}


def _discover_enrichment() -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for clean_root in CLEAN_ROOTS:
        if not clean_root.is_dir():
            continue
        for fp in clean_root.glob("**/whoscored/match_enrichment.csv"):
            key = str(fp.resolve())
            if key not in seen:
                seen.add(key)
                out.append(fp)
    return sorted(out)


def link_matches(dry_run: bool = False) -> dict:
    files = _discover_enrichment()
    log.info("match_enrichment.csv encontrados: %d", len(files))

    linked = skipped_team = skipped_assigned = skipped_no_match = 0

    with engine.begin() as conn:
        comp_map = _comp_id_map(conn)
        team_cache = {
            r[0]: r[1]
            for r in conn.execute(text(
                "SELECT id_whoscored, canonical_id FROM dim_team WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }
        assigned_ws = {
            r[0]
            for r in conn.execute(text(
                "SELECT id_whoscored FROM dim_match WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }

        for path in files:
            # .../clean/<comp_slug>/<season>/whoscored/match_enrichment.csv
            comp_slug = path.parts[-4]
            season_path = path.parts[-3]
            comp_id = comp_map.get(comp_slug)
            season_db = season_db_format(season_path.replace("_", "/"))
            if not comp_id or not season_db:
                continue

            dates: dict[int, str | None] = {}
            matches_path = path.parent / "matches.csv"
            if matches_path.is_file():
                mdf = safe_read_csv(matches_path)
                if mdf is not None:
                    for _, mr in mdf.iterrows():
                        mid = mr.get("whoscored_match_id")
                        if pd.notna(mid):
                            d = mr.get("match_date")
                            dates[int(mid)] = str(d)[:10] if pd.notna(d) else None

            df = safe_read_csv(path)
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                if pd.isna(row.get("whoscored_match_id")):
                    continue
                ws_mid = int(row["whoscored_match_id"])
                if ws_mid in assigned_ws:
                    continue

                h_ws = int(row["home_team_ws_id"]) if pd.notna(row.get("home_team_ws_id")) else None
                a_ws = int(row["away_team_ws_id"]) if pd.notna(row.get("away_team_ws_id")) else None
                if not h_ws or not a_ws:
                    skipped_team += 1
                    continue
                h_id = team_cache.get(h_ws)
                a_id = team_cache.get(a_ws)
                if not h_id or not a_id:
                    skipped_team += 1
                    continue

                existing = conn.execute(text("""
                    SELECT match_id FROM dim_match
                    WHERE competition_id = :cid
                      AND season = :season
                      AND id_whoscored IS NULL
                      AND (
                          (home_team_id = :hid AND away_team_id = :aid)
                          OR (home_team_id = :aid AND away_team_id = :hid)
                      )
                      AND (:dt IS NULL OR match_date IS NULL OR match_date = CAST(:dt AS DATE))
                    ORDER BY
                      CASE WHEN home_team_id = :hid AND away_team_id = :aid THEN 0 ELSE 1 END,
                      CASE WHEN match_date = CAST(:dt AS DATE) THEN 0 ELSE 1 END
                    LIMIT 1
                """), {
                    "cid": comp_id,
                    "season": season_db,
                    "hid": h_id,
                    "aid": a_id,
                    "dt": dates.get(ws_mid),
                }).fetchone()

                if not existing:
                    skipped_no_match += 1
                    continue

                h_sc, a_sc = _parse_score(row.get("ft_score"))
                att = row.get("attendance")
                att_i = int(float(att)) if pd.notna(att) and int(float(att)) > 0 else None

                if dry_run:
                    linked += 1
                    assigned_ws.add(ws_mid)
                    continue

                conn.execute(text("""
                    UPDATE dim_match SET
                        id_whoscored = :ws_id,
                        venue_name   = COALESCE(venue_name, :venue),
                        manager_home = COALESCE(manager_home, :mgr_h),
                        manager_away = COALESCE(manager_away, :mgr_a),
                        attendance   = COALESCE(attendance, :att),
                        home_score   = COALESCE(home_score, :hsc),
                        away_score   = COALESCE(away_score, :asc)
                    WHERE match_id = :mid
                      AND id_whoscored IS NULL
                """), {
                    "ws_id": ws_mid,
                    "venue": row.get("venue_name") if pd.notna(row.get("venue_name")) else None,
                    "mgr_h": row.get("manager_home") if pd.notna(row.get("manager_home")) else None,
                    "mgr_a": row.get("manager_away") if pd.notna(row.get("manager_away")) else None,
                    "att": att_i,
                    "hsc": h_sc,
                    "asc": a_sc,
                    "mid": existing[0],
                })
                linked += 1
                assigned_ws.add(ws_mid)

    stats = {
        "linked": linked,
        "skipped_team": skipped_team,
        "skipped_no_match": skipped_no_match,
        "skipped_assigned": skipped_assigned,
    }
    log.info("Enlazados: %d | sin equipos WS: %d | sin partido BD: %d",
             linked, skipped_team, skipped_no_match)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    link_matches(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
