"""
scripts/link_whoscored_aggressive.py
====================================
Repara enlaces WhoScored erróneos (sobre todo CL/UEL) y reasigna ids.

Problema detectado: cientos de partidos europeos tienen id_whoscored de
partidos domésticos (mismo id mal asignado). Este script:
  1. Limpia id_whoscored cuando el par de equipos no coincide con enrichment.
  2. Reasigna ids libres a partidos europeos por temporada + equipos + fecha.

Uso:
    python -m scripts.link_whoscored_aggressive
    python -m scripts.link_whoscored_aggressive --dry-run
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from collections import defaultdict
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

CONTINENTAL = {"champions_league", "europa_league", "europa_conference_league"}


def _parse_score(val) -> tuple[int | None, int | None]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    m = re.match(r"(\d+)\s*:\s*(\d+)", str(val).strip())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _pair_key(h: int, a: int) -> tuple[int, int]:
    return (min(h, a), max(h, a))


def _discover_enrichment() -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for root in CLEAN_ROOTS:
        if not root.is_dir():
            continue
        for fp in root.glob("**/whoscored/match_enrichment.csv"):
            key = str(fp.resolve())
            if key not in seen:
                seen.add(key)
                out.append(fp)
    return sorted(out)


def _load_enrichment(team_cache: dict[int, int]) -> dict[int, dict]:
    rows: dict[int, dict] = {}
    for path in _discover_enrichment():
        comp_slug = path.parts[-4]
        season_db = season_db_format(path.parts[-3].replace("_", "/"))
        df = safe_read_csv(path)
        if df is None or df.empty:
            continue

        dates: dict[int, str | None] = {}
        mp = path.parent / "matches.csv"
        if mp.is_file():
            mdf = safe_read_csv(mp)
            if mdf is not None:
                for _, mr in mdf.iterrows():
                    if pd.notna(mr.get("whoscored_match_id")) and pd.notna(mr.get("match_date")):
                        dates[int(mr["whoscored_match_id"])] = str(mr["match_date"])[:10]

        for _, row in df.iterrows():
            if pd.isna(row.get("whoscored_match_id")):
                continue
            if pd.isna(row.get("home_team_ws_id")) or pd.isna(row.get("away_team_ws_id")):
                continue
            ws = int(row["whoscored_match_id"])
            h_can = team_cache.get(int(row["home_team_ws_id"]))
            a_can = team_cache.get(int(row["away_team_ws_id"]))
            if not h_can or not a_can:
                continue
            h_sc, a_sc = _parse_score(row.get("ft_score"))
            att = row.get("attendance")
            att_i = int(float(att)) if pd.notna(att) and int(float(att)) > 0 else None
            rows[ws] = {
                "ws_id": ws,
                "comp_slug": comp_slug,
                "season": season_db,
                "home_can": h_can,
                "away_can": a_can,
                "pair": _pair_key(h_can, a_can),
                "match_date": dates.get(ws),
                "home_score": h_sc,
                "away_score": a_sc,
                "venue": row.get("venue_name") if pd.notna(row.get("venue_name")) else None,
                "manager_home": row.get("manager_home") if pd.notna(row.get("manager_home")) else None,
                "manager_away": row.get("manager_away") if pd.notna(row.get("manager_away")) else None,
                "attendance": att_i,
                "ref_ws": int(row["referee_id_whoscored"]) if pd.notna(row.get("referee_id_whoscored")) else None,
            }
    return rows


def _score(c: dict, row: dict) -> int:
    s = 0
    if c["comp_slug"] == row.get("comp_slug"):
        s += 15
    if c["season"] == row["season"]:
        s += 25
    if c["home_can"] == row["home_team_id"] and c["away_can"] == row["away_team_id"]:
        s += 10
    elif c["home_can"] == row["away_team_id"] and c["away_can"] == row["home_team_id"]:
        s += 6
    if c.get("match_date") and row.get("match_date") and str(c["match_date"]) == str(row["match_date"]):
        s += 35
    hs, asc = c.get("home_score"), c.get("away_score")
    if hs is not None and row.get("home_score") == hs and row.get("away_score") == asc:
        s += 20
    return s


def run(dry_run: bool = False) -> dict:
    stats = {"cleared_mismatch": 0, "linked": 0}

    with engine.begin() as conn:
        team_cache = {
            r[0]: r[1]
            for r in conn.execute(text(
                "SELECT id_whoscored, canonical_id FROM dim_team WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }
        ref_cache = {
            r[0]: r[1]
            for r in conn.execute(text(
                "SELECT id_whoscored, referee_id FROM dim_referee WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }
        comp_slug_by_id = {
            r[0]: slugify_competition(r[1])
            for r in conn.execute(text(
                "SELECT canonical_id, canonical_name FROM dim_competition"
            )).fetchall()
        }

        centres = _load_enrichment(team_cache)
        log.info("Enrichment WS únicos: %d", len(centres))

        matches = conn.execute(text("""
            SELECT m.match_id, m.id_whoscored, m.match_date, m.season,
                   m.home_team_id, m.away_team_id, m.home_score, m.away_score,
                   m.competition_id
            FROM dim_match m
        """)).mappings().all()

        by_id: dict[int, dict] = {}
        ws_to_mid: dict[int, int] = {}
        for m in matches:
            d = dict(m)
            d["comp_slug"] = comp_slug_by_id.get(m["competition_id"], "")
            by_id[m["match_id"]] = d
            if m["id_whoscored"]:
                ws_to_mid[int(m["id_whoscored"])] = m["match_id"]

        open_slots: dict[tuple, list[int]] = defaultdict(list)
        for m in matches:
            if m["id_whoscored"]:
                continue
            key = (m["season"], _pair_key(m["home_team_id"], m["away_team_id"]))
            open_slots[key].append(m["match_id"])

        def _clear(mid: int) -> None:
            if not dry_run:
                conn.execute(text("""
                    UPDATE dim_match SET id_whoscored = NULL
                    WHERE match_id = :mid
                """), {"mid": mid})

        def _apply(mid: int, c: dict) -> None:
            ref_id = ref_cache.get(c["ref_ws"]) if c.get("ref_ws") else None
            conn.execute(text("""
                UPDATE dim_match SET
                    id_whoscored = :ws,
                    venue_name   = COALESCE(venue_name, :venue),
                    manager_home = COALESCE(manager_home, :mh),
                    manager_away = COALESCE(manager_away, :ma),
                    attendance   = COALESCE(attendance, :att),
                    home_score   = COALESCE(home_score, :hs),
                    away_score   = COALESCE(away_score, :as),
                    referee_id   = COALESCE(referee_id, :ref),
                    match_date   = COALESCE(match_date, CAST(:dt AS DATE))
                WHERE match_id = :mid AND id_whoscored IS NULL
            """), {
                "ws": c["ws_id"], "venue": c.get("venue"),
                "mh": c.get("manager_home"), "ma": c.get("manager_away"),
                "att": c.get("attendance"), "hs": c.get("home_score"),
                "as": c.get("away_score"), "ref": ref_id,
                "dt": c.get("match_date"), "mid": mid,
            })

        # Fase 1a: competición enrichment != dim_match (solo continental)
        for ws_id, mid in list(ws_to_mid.items()):
            c = centres.get(ws_id)
            if not c:
                continue
            row = by_id[mid]
            if row["comp_slug"] not in CONTINENTAL and c["comp_slug"] not in CONTINENTAL:
                continue
            if c["comp_slug"] == row["comp_slug"]:
                continue
            _clear(mid)
            stats["cleared_mismatch"] += 1
            del ws_to_mid[ws_id]
            by_id[mid]["id_whoscored"] = None
            open_slots[(row["season"], _pair_key(row["home_team_id"], row["away_team_id"]))].append(mid)

        # Fase 1b: par de equipos no coincide (solo continental)
        for ws_id, mid in list(ws_to_mid.items()):
            c = centres.get(ws_id)
            if not c:
                continue
            row = by_id[mid]
            if row["comp_slug"] not in CONTINENTAL:
                continue
            c = centres.get(ws_id)
            if not c:
                continue
            row = by_id[mid]
            if _pair_key(row["home_team_id"], row["away_team_id"]) == c["pair"]:
                continue
            _clear(mid)
            stats["cleared_mismatch"] += 1
            del ws_to_mid[ws_id]
            by_id[mid]["id_whoscored"] = None
            open_slots[(row["season"], _pair_key(row["home_team_id"], row["away_team_id"]))].append(mid)

        # Fase 2: reasignar ids libres (priorizar misma competición slug en enrichment)
        for ws_id, c in sorted(centres.items(), key=lambda x: (0 if x[1]["comp_slug"] in CONTINENTAL else 1)):
            if ws_id in ws_to_mid:
                continue

            pool = list(open_slots.get((c["season"], c["pair"]), []))
            if not pool:
                continue

            best = max(pool, key=lambda mid: _score(c, by_id[mid]))
            if _score(c, by_id[best]) < 20:
                continue

            if not dry_run:
                _apply(best, c)
            stats["linked"] += 1
            ws_to_mid[ws_id] = best
            by_id[best]["id_whoscored"] = ws_id
            open_slots[(by_id[best]["season"], c["pair"])].remove(best)

    log.info("cleared_mismatch=%d linked=%d", stats["cleared_mismatch"], stats["linked"])
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
