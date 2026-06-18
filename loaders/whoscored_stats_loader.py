"""
loaders/whoscored_stats_loader.py
==================================
Carga los CSVs generados por whoscored_stats_extractor.py a PostgreSQL:

  - player_match_stats.csv → fact_player_match_stats
  - formations.csv         → fact_formations
  - referees.csv           → dim_referee (upsert id_whoscored)
  - match_enrichment.csv   → UPDATE dim_match (venue, managers, scores, referee_id)

Requisitos:
  - Las tablas deben existir (ejecutar db/add_whoscored_stats.sql primero)
  - dim_match, dim_player, dim_team deben tener los IDs de WhoScored

Uso:
    python -m loaders.whoscored_stats_loader
    python -m loaders.whoscored_stats_loader --competition la_liga --season 2025_2026
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine, safe_read_csv
from utils.data_paths import CLEAN_ROOT, iter_clean_csvs

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Cache de FK lookups ──────────────────────────────────────────────

def _build_player_cache(conn) -> dict[int, int]:
    """ws_player_id → canonical_id."""
    rows = conn.execute(text(
        "SELECT id_whoscored, canonical_id FROM dim_player WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


def _build_team_cache(conn) -> dict[int, int]:
    """ws_team_id → canonical_id."""
    rows = conn.execute(text(
        "SELECT id_whoscored, canonical_id FROM dim_team WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


def _build_match_cache(conn) -> dict[int, int]:
    """ws_match_id → match_id."""
    rows = conn.execute(text(
        "SELECT id_whoscored, match_id FROM dim_match WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


def _build_referee_cache(conn) -> dict[int, int]:
    """ws_referee_id → referee_id."""
    rows = conn.execute(text(
        "SELECT id_whoscored, referee_id FROM dim_referee WHERE id_whoscored IS NOT NULL"
    )).fetchall()
    return {r[0]: r[1] for r in rows}


# ── Loaders ──────────────────────────────────────────────────────────

def load_referees(csv_paths: list[Path]) -> int:
    """Upsert de árbitros desde referees.csv."""
    loaded = 0
    with engine.begin() as conn:
        for path in csv_paths:
            df = safe_read_csv(path)
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                ws_id = row.get("id_whoscored")
                name = row.get("canonical_name")
                if pd.isna(ws_id) or not name:
                    continue
                ws_id = int(ws_id)
                # Upsert: insert if not exists
                existing = conn.execute(text(
                    "SELECT referee_id FROM dim_referee WHERE id_whoscored = :ws"
                ), {"ws": ws_id}).fetchone()
                if not existing:
                    conn.execute(text(
                        "INSERT INTO dim_referee (canonical_name, id_whoscored, data_source) "
                        "VALUES (:name, :ws, 'whoscored')"
                    ), {"name": name, "ws": ws_id})
                    loaded += 1
    log.info("  Referees: %d nuevos insertados", loaded)
    return loaded


def load_player_match_stats(csv_paths: list[Path]) -> int:
    """Carga player_match_stats.csv → fact_player_match_stats."""
    loaded = 0
    skipped_fk = 0

    with engine.begin() as conn:
        match_cache = _build_match_cache(conn)
        player_cache = _build_player_cache(conn)
        team_cache = _build_team_cache(conn)

        for path in csv_paths:
            df = safe_read_csv(path)
            if df is None or df.empty:
                continue

            batch = []
            for _, row in df.iterrows():
                ws_match = int(row["whoscored_match_id"]) if pd.notna(row.get("whoscored_match_id")) else None
                ws_player = int(row["whoscored_player_id"]) if pd.notna(row.get("whoscored_player_id")) else None
                ws_team = int(row["whoscored_team_id"]) if pd.notna(row.get("whoscored_team_id")) else None

                match_id = match_cache.get(ws_match)
                player_id = player_cache.get(ws_player)
                team_id = team_cache.get(ws_team)

                if not all([match_id, player_id, team_id]):
                    skipped_fk += 1
                    continue

                def _val(col, as_type=None):
                    v = row.get(col)
                    if v is None or (isinstance(v, float) and pd.isna(v)):
                        return None
                    if as_type:
                        return as_type(v)
                    return v

                batch.append({
                    "match_id": match_id,
                    "player_id": player_id,
                    "team_id": team_id,
                    "is_starter": _val("is_starter"),
                    "position": _val("position"),
                    "shirt_no": _val("shirt_no", int),
                    "age": _val("age", int),
                    "height_cm": _val("height_cm", int),
                    "weight_kg": _val("weight_kg", int),
                    "is_man_of_the_match": _val("is_man_of_the_match"),
                    "subbed_in_minute": _val("subbed_in_minute", int),
                    "subbed_out_minute": _val("subbed_out_minute", int),
                    "rating": _val("rating", float),
                    "passes_total": _val("passes_total", int),
                    "passes_accurate": _val("passes_accurate", int),
                    "passes_key": _val("passes_key", int),
                    "pass_success_pct": _val("pass_success_pct", float),
                    "shots_total": _val("shots_total", int),
                    "shots_on_target": _val("shots_on_target", int),
                    "shots_off_target": _val("shots_off_target", int),
                    "shots_blocked": _val("shots_blocked", int),
                    "dribbles_attempted": _val("dribbles_attempted", int),
                    "dribbles_won": _val("dribbles_won", int),
                    "dribbles_lost": _val("dribbles_lost", int),
                    "tackles_total": _val("tackles_total", int),
                    "tackles_successful": _val("tackles_successful", int),
                    "interceptions": _val("interceptions", int),
                    "clearances": _val("clearances", int),
                    "aerials_total": _val("aerials_total", int),
                    "aerials_won": _val("aerials_won", int),
                    "fouls_committed": _val("fouls_committed", int),
                    "was_dribbled_past": _val("was_dribbled_past", int),
                    "dispossessed": _val("dispossessed", int),
                    "touches": _val("touches", int),
                    "offsides_caught": _val("offsides_caught", int),
                    "corners_total": _val("corners_total", int),
                    "corners_accurate": _val("corners_accurate", int),
                    "throw_ins_total": _val("throw_ins_total", int),
                    "throw_ins_accurate": _val("throw_ins_accurate", int),
                    "saves_total": _val("saves_total", int),
                    "saves_parried_safe": _val("saves_parried_safe", int),
                    "saves_parried_danger": _val("saves_parried_danger", int),
                    "claims_high": _val("claims_high", int),
                    "collected": _val("collected", int),
                    "possession_pct": _val("possession_pct", float),
                })

            # Bulk insert con ON CONFLICT
            if batch:
                cols = list(batch[0].keys())
                placeholders = ", ".join(f":{c}" for c in cols)
                col_list = ", ".join(cols)
                sql = f"""
                    INSERT INTO fact_player_match_stats ({col_list})
                    VALUES ({placeholders})
                    ON CONFLICT (match_id, player_id, data_source) DO NOTHING
                """
                conn.execute(text(sql), batch)
                loaded += len(batch)

    log.info("  Player match stats: %d insertados, %d sin FK", loaded, skipped_fk)
    return loaded


def load_formations(csv_paths: list[Path]) -> int:
    """Carga formations.csv → fact_formations."""
    loaded = 0
    skipped = 0

    with engine.begin() as conn:
        match_cache = _build_match_cache(conn)
        team_cache = _build_team_cache(conn)
        player_cache = _build_player_cache(conn)

        for path in csv_paths:
            df = safe_read_csv(path)
            if df is None or df.empty:
                continue

            batch = []
            for _, row in df.iterrows():
                ws_match = int(row["whoscored_match_id"]) if pd.notna(row.get("whoscored_match_id")) else None
                ws_team = int(row["whoscored_team_id"]) if pd.notna(row.get("whoscored_team_id")) else None

                match_id = match_cache.get(ws_match)
                team_id = team_cache.get(ws_team)

                if not match_id or not team_id:
                    skipped += 1
                    continue

                captain_ws = int(row["captain_player_id_ws"]) if pd.notna(row.get("captain_player_id_ws")) else None
                captain_id = player_cache.get(captain_ws) if captain_ws else None

                start_min = int(row["start_minute"]) if pd.notna(row.get("start_minute")) else 0
                end_min = int(row["end_minute"]) if pd.notna(row.get("end_minute")) else None

                batch.append({
                    "match_id": match_id,
                    "team_id": team_id,
                    "side": row.get("side"),
                    "formation_name": row.get("formation_name"),
                    "captain_player_id": captain_id,
                    "start_minute": start_min,
                    "end_minute": end_min,
                })

            if batch:
                sql = """
                    INSERT INTO fact_formations
                        (match_id, team_id, side, formation_name, captain_player_id,
                         start_minute, end_minute)
                    VALUES
                        (:match_id, :team_id, :side, :formation_name, :captain_player_id,
                         :start_minute, :end_minute)
                    ON CONFLICT (match_id, team_id, start_minute, data_source) DO NOTHING
                """
                conn.execute(text(sql), batch)
                loaded += len(batch)

    log.info("  Formations: %d insertadas, %d sin FK", loaded, skipped)
    return loaded


def load_match_enrichment(csv_paths: list[Path]) -> int:
    """Actualiza dim_match con venue, managers, scores, referee desde match_enrichment.csv."""
    updated = 0

    with engine.begin() as conn:
        match_cache = _build_match_cache(conn)
        referee_cache = _build_referee_cache(conn)
        team_cache = _build_team_cache(conn)  # ws_team_id -> canonical_id
        # match_id -> home_team_id real de la fila, para orientar managers
        home_team_by_match = {
            r[0]: r[1] for r in conn.execute(text(
                "SELECT match_id, home_team_id FROM dim_match "
                "WHERE id_whoscored IS NOT NULL"
            )).fetchall()
        }

        for path in csv_paths:
            df = safe_read_csv(path)
            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                ws_match = int(row["whoscored_match_id"]) if pd.notna(row.get("whoscored_match_id")) else None
                match_id = match_cache.get(ws_match)
                if not match_id:
                    continue

                # Orientar managers a la fila: si el home real (WhoScored) no
                # coincide con home_team_id de la fila, vienen invertidos.
                mgr_h = row.get("manager_home")
                mgr_a = row.get("manager_away")
                try:
                    home_ws = (int(row["home_team_ws_id"])
                               if pd.notna(row.get("home_team_ws_id")) else None)
                except (ValueError, TypeError):
                    home_ws = None
                home_canon = team_cache.get(home_ws) if home_ws else None
                row_home = home_team_by_match.get(match_id)
                if home_canon and row_home and home_canon != row_home:
                    mgr_h, mgr_a = mgr_a, mgr_h  # fila invertida -> swap

                updates = {}
                if pd.notna(row.get("venue_name")) and row["venue_name"]:
                    updates["venue_name"] = str(row["venue_name"])
                if pd.notna(mgr_h) and mgr_h:
                    updates["manager_home"] = str(mgr_h)
                if pd.notna(mgr_a) and mgr_a:
                    updates["manager_away"] = str(mgr_a)
                if pd.notna(row.get("ht_score")) and row["ht_score"]:
                    updates["ht_score"] = str(row["ht_score"]).strip()
                if pd.notna(row.get("ft_score")) and row["ft_score"]:
                    updates["ft_score"] = str(row["ft_score"]).strip()
                if pd.notna(row.get("attendance")) and row["attendance"]:
                    try:
                        updates["attendance"] = int(float(row["attendance"]))
                    except (ValueError, TypeError):
                        pass

                # Referee FK
                ref_ws = int(row["referee_id_whoscored"]) if pd.notna(row.get("referee_id_whoscored")) else None
                if ref_ws and ref_ws in referee_cache:
                    updates["referee_id"] = referee_cache[ref_ws]

                if not updates:
                    continue

                set_clause = ", ".join(f"{k} = :{k}" for k in updates)
                updates["mid"] = match_id
                conn.execute(
                    text(f"UPDATE dim_match SET {set_clause} WHERE match_id = :mid"),
                    updates,
                )
                updated += 1

    log.info("  Match enrichment: %d partidos actualizados", updated)
    return updated


# ── Orquestador ─────────────────────────────────────────────────────

def load_all(
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> dict:
    """Carga todos los CSVs de stats de WhoScored."""
    # Buscar CSVs
    ref_csvs = iter_clean_csvs(competition, season, "whoscored", "referees")
    stats_csvs = iter_clean_csvs(competition, season, "whoscored", "player_match_stats")
    form_csvs = iter_clean_csvs(competition, season, "whoscored", "formations")
    enrich_csvs = iter_clean_csvs(competition, season, "whoscored", "match_enrichment")

    log.info("CSVs encontrados: %d referees, %d stats, %d formations, %d enrichment",
             len(ref_csvs), len(stats_csvs), len(form_csvs), len(enrich_csvs))

    # Orden: referees primero (para que match_enrichment pueda linkar referee_id)
    results = {}
    if ref_csvs:
        results["referees"] = load_referees(ref_csvs)
    if stats_csvs:
        results["player_match_stats"] = load_player_match_stats(stats_csvs)
    if form_csvs:
        results["formations"] = load_formations(form_csvs)
    if enrich_csvs:
        results["match_enrichment"] = load_match_enrichment(enrich_csvs)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Carga stats de WhoScored a PostgreSQL"
    )
    parser.add_argument("--competition", "-c", default=None)
    parser.add_argument("--season", "-s", default=None)
    args = parser.parse_args()

    print("=" * 55)
    print("  WhoScored Stats Loader")
    print("=" * 55)
    results = load_all(args.competition, args.season)
    print("\n[OK] Carga completada:", results)


if __name__ == "__main__":
    main()
