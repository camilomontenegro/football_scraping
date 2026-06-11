"""
Backfill FKs WhoScored para Primeira Liga:
  - dim_player: jugadores en players.csv sin id_whoscored
  - dim_match: partidos en match_enrichment sin id_whoscored
  - Limpia fact_events duplicados por whoscored_event_id (mismo partido)
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine
from loaders.fact_loader_generico import load_events

log = logging.getLogger(__name__)

COMP = "Primeira Liga"
COMP_ID = 10
SEASONS = [f"{y}_{y+1}" for y in range(2020, 2026)]
CLEAN = PROJECT_ROOT / "data" / "clean" / "primeira_liga"


def _parse_score(val) -> tuple[int | None, int | None]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    s = str(val).strip()
    m = re.match(r"(\d+)\s*:\s*(\d+)", s)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _team_canonical(conn, ws_team_id: int) -> int | None:
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE id_whoscored = :tid"),
        {"tid": ws_team_id},
    ).fetchone()
    return row[0] if row else None


def backfill_players(conn) -> int:
    inserted = 0
    seen: set[int] = set()
    for season in SEASONS:
        path = CLEAN / season / "whoscored" / "players.csv"
        if not path.is_file():
            continue
        df = pd.read_csv(path)
        for _, row in df.iterrows():
            ws_id = row.get("whoscored_player_id")
            name = row.get("player_name") or row.get("canonical_name")
            if pd.isna(ws_id) or not name:
                continue
            ws_id = int(ws_id)
            if ws_id in seen:
                continue
            seen.add(ws_id)
            exists = conn.execute(
                text("SELECT 1 FROM dim_player WHERE id_whoscored = :id"),
                {"id": ws_id},
            ).fetchone()
            if exists:
                continue
            conn.execute(
                text("""
                    INSERT INTO dim_player (canonical_name, id_whoscored)
                    VALUES (:name, :ws_id)
                """),
                {"name": str(name).strip(), "ws_id": ws_id},
            )
            inserted += 1
    log.info("Jugadores insertados: %d", inserted)
    return inserted


def backfill_matches(conn) -> int:
    linked = inserted = skipped = 0
    for season in SEASONS:
        enrich_path = CLEAN / season / "whoscored" / "match_enrichment.csv"
        matches_path = CLEAN / season / "whoscored" / "matches.csv"
        if not enrich_path.is_file():
            continue
        enrich = pd.read_csv(enrich_path)
        dates: dict[int, str | None] = {}
        if matches_path.is_file():
            mdf = pd.read_csv(matches_path)
            for _, r in mdf.iterrows():
                mid = r.get("whoscored_match_id")
                if pd.notna(mid):
                    dates[int(mid)] = str(r.get("match_date"))[:10] if pd.notna(r.get("match_date")) else None

        season_label = season.replace("_", "/")

        for _, row in enrich.iterrows():
            ws_mid = int(row["whoscored_match_id"])
            if conn.execute(
                text("SELECT 1 FROM dim_match WHERE id_whoscored = :id"),
                {"id": ws_mid},
            ).fetchone():
                continue

            h_ws = int(row["home_team_ws_id"])
            a_ws = int(row["away_team_ws_id"])
            h_id = _team_canonical(conn, h_ws)
            a_id = _team_canonical(conn, a_ws)
            if not h_id or not a_id:
                skipped += 1
                continue

            match_date = dates.get(ws_mid)
            h_sc, a_sc = _parse_score(row.get("ft_score"))
            att = row.get("attendance")
            att_i = int(att) if pd.notna(att) else None

            existing = conn.execute(
                text("""
                    SELECT match_id FROM dim_match
                    WHERE competition_id = :cid
                      AND season = :season
                      AND home_team_id = :hid
                      AND away_team_id = :aid
                      AND id_whoscored IS NULL
                    LIMIT 1
                """),
                {"cid": COMP_ID, "season": season_label, "hid": h_id, "aid": a_id},
            ).fetchone()

            if existing:
                conn.execute(
                    text("""
                        UPDATE dim_match
                        SET id_whoscored = :ws_id,
                            match_date = COALESCE(match_date, CAST(:dt AS DATE)),
                            attendance = COALESCE(attendance, :att),
                            venue_name = COALESCE(venue_name, :venue),
                            manager_home = COALESCE(manager_home, :mgr_h),
                            manager_away = COALESCE(manager_away, :mgr_a),
                            home_score = COALESCE(home_score, :hsc),
                            away_score = COALESCE(away_score, :asc)
                        WHERE match_id = :mid
                          AND id_whoscored IS NULL
                    """),
                    {
                        "ws_id": ws_mid,
                        "dt": match_date,
                        "att": att_i,
                        "venue": row.get("venue_name"),
                        "mgr_h": row.get("manager_home"),
                        "mgr_a": row.get("manager_away"),
                        "hsc": h_sc,
                        "asc": a_sc,
                        "mid": existing[0],
                    },
                )
                linked += 1
                continue

            conn.execute(
                text("""
                    INSERT INTO dim_match (
                        match_date, competition, season,
                        home_team_id, away_team_id, competition_id,
                        home_score, away_score, attendance,
                        data_source, id_whoscored,
                        venue_name, manager_home, manager_away
                    ) VALUES (
                        CAST(:dt AS DATE), :comp, :season,
                        :hid, :aid, :cid,
                        :hsc, :asc, :att,
                        'whoscored', :ws_id,
                        :venue, :mgr_h, :mgr_a
                    )
                """),
                {
                    "dt": match_date,
                    "comp": COMP,
                    "season": season_label,
                    "hid": h_id,
                    "aid": a_id,
                    "cid": COMP_ID,
                    "hsc": h_sc,
                    "asc": a_sc,
                    "att": att_i,
                    "ws_id": ws_mid,
                    "venue": row.get("venue_name"),
                    "mgr_h": row.get("manager_home"),
                    "mgr_a": row.get("manager_away"),
                },
            )
            inserted += 1

    log.info("Partidos enlazados: %d | insertados: %d | omitidos: %d", linked, inserted, skipped)
    return linked + inserted


def dedupe_ws_event_ids(conn) -> int:
    """Elimina filas duplicadas por (whoscored_event_id, match_id) conservando la del jugador WS correcto."""
    conflicts = conn.execute(
        text("""
            SELECT fe.whoscored_event_id, fe.match_id, m.id_whoscored AS ws_match_id
            FROM fact_events fe
            JOIN dim_match m ON m.match_id = fe.match_id
            WHERE m.competition_id = :cid
              AND fe.whoscored_event_id IS NOT NULL
            GROUP BY fe.whoscored_event_id, fe.match_id, m.id_whoscored
            HAVING COUNT(DISTINCT fe.player_id) > 1
        """),
        {"cid": COMP_ID},
    ).fetchall()

    if not conflicts:
        return 0

    truth: dict[tuple[int, int], int] = {}
    for season in SEASONS:
        path = CLEAN / season / "whoscored" / "events.csv"
        if not path.is_file():
            continue
        df = pd.read_csv(
            path,
            usecols=["whoscored_match_id", "whoscored_player_id", "whoscored_event_id"],
            low_memory=False,
        )
        sub = df.dropna(subset=["whoscored_event_id", "whoscored_player_id"])
        for _, r in sub.iterrows():
            truth[(int(r["whoscored_match_id"]), int(r["whoscored_event_id"]))] = int(
                r["whoscored_player_id"]
            )

    deleted = 0
    for ws_eid, match_id, ws_match_id in conflicts:
        ws_pid = truth.get((int(ws_match_id), int(ws_eid)))
        rows = conn.execute(
            text("""
                SELECT fe.event_id, fe.player_id, dp.id_whoscored
                FROM fact_events fe
                JOIN dim_player dp ON dp.canonical_id = fe.player_id
                WHERE fe.match_id = :mid AND fe.whoscored_event_id = :eid
            """),
            {"mid": match_id, "eid": ws_eid},
        ).fetchall()
        if ws_pid is not None:
            keep = next((r[0] for r in rows if r[2] == ws_pid), rows[0][0])
        else:
            keep = rows[0][0]
        for event_id, _, _ in rows:
            if event_id != keep:
                conn.execute(
                    text("DELETE FROM fact_events WHERE event_id = :eid"),
                    {"eid": event_id},
                )
                deleted += 1

    log.info("Eventos duplicados eliminados (whoscored_event_id): %d", deleted)
    return deleted


def reload_events(conn) -> int:
    total = 0
    for season in SEASONS:
        ws_path = CLEAN / season / "whoscored"
        if not (ws_path / "events.csv").is_file():
            continue
        n = load_events(conn, ws_path=ws_path)
        log.info("Temporada %s: %d eventos procesados", season, n)
        total += n
    return total


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-reload", action="store_true")
    args = parser.parse_args()

    with engine.begin() as conn:
        backfill_players(conn)
        backfill_matches(conn)
        dedupe_ws_event_ids(conn)

    if not args.skip_reload:
        with engine.begin() as conn:
            reload_events(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
