"""
dashboard/explore.py
====================
Read-only DB queries for the Exploration tab.

All queries accept an optional `team` argument. When `team` is None, the result
spans every team in the season. When a team name is provided, it is resolved
to a team_id via dim_team and used to filter the query.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from dashboard.db import get_engine, query_df


def get_competitions() -> list[str]:
    # dim_match has no competition column today, so the project scope is
    # effectively a single competition. Expand this query once a competition
    # column or mapping is available.
    return ["La Liga"]


def get_seasons_for_competition(competition: str) -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT label FROM dim_season ORDER BY year_start DESC"
        )).fetchall()
    return [r[0] for r in rows]


def get_teams_for_season(season_label: str) -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT t.name_canonical
            FROM dim_match m
            JOIN dim_season s ON m.season_id = s.season_id
            JOIN dim_team t   ON t.team_id IN (m.home_team_id, m.away_team_id)
            WHERE s.label = :season
            ORDER BY t.name_canonical
        """), {"season": season_label}).fetchall()
    return [r[0] for r in rows]


def _team_id(conn, team: str | None) -> int | None:
    if team is None:
        return None
    row = conn.execute(text(
        "SELECT team_id FROM dim_team WHERE name_canonical = :n"
    ), {"n": team}).fetchone()
    return int(row[0]) if row else None


def get_season_summary(season_label: str, team: str | None) -> dict:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)

        if tid is None:
            matches = conn.execute(text("""
                SELECT COUNT(*)
                FROM dim_match m
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season
            """), {"season": season_label}).scalar() or 0

            goals = conn.execute(text("""
                SELECT COALESCE(SUM(home_score), 0) + COALESCE(SUM(away_score), 0)
                FROM dim_match m
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season
            """), {"season": season_label}).scalar() or 0

            xg = conn.execute(text("""
                SELECT COALESCE(SUM(fs.xg), 0)
                FROM fact_shots fs
                JOIN dim_match m  ON fs.match_id = m.match_id
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season
            """), {"season": season_label}).scalar() or 0

            injuries = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_injuries fi
                JOIN dim_season s ON fi.season_id = s.season_id
                WHERE s.label = :season
            """), {"season": season_label}).scalar() or 0
        else:
            matches = conn.execute(text("""
                SELECT COUNT(*)
                FROM dim_match m
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season
                  AND (m.home_team_id = :tid OR m.away_team_id = :tid)
            """), {"season": season_label, "tid": tid}).scalar() or 0

            goals = conn.execute(text("""
                SELECT COALESCE(SUM(
                    CASE
                        WHEN m.home_team_id = :tid THEN m.home_score
                        WHEN m.away_team_id = :tid THEN m.away_score
                        ELSE 0
                    END
                ), 0)
                FROM dim_match m
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season
                  AND (m.home_team_id = :tid OR m.away_team_id = :tid)
            """), {"season": season_label, "tid": tid}).scalar() or 0

            xg = conn.execute(text("""
                SELECT COALESCE(SUM(fs.xg), 0)
                FROM fact_shots fs
                JOIN dim_match m  ON fs.match_id = m.match_id
                JOIN dim_season s ON m.season_id = s.season_id
                WHERE s.label = :season AND fs.team_id = :tid
            """), {"season": season_label, "tid": tid}).scalar() or 0

            injuries = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_injuries fi
                JOIN dim_season s ON fi.season_id = s.season_id
                WHERE s.label = :season AND fi.team_id = :tid
            """), {"season": season_label, "tid": tid}).scalar() or 0

    return {
        "matches":  int(matches),
        "goals":    int(goals),
        "xg":       round(float(xg), 1),
        "injuries": int(injuries),
    }


def get_results(season_label: str, team: str | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": season_label}
    sql = """
        SELECT m.match_date, s.label AS season,
               ht.name_canonical AS home_team,
               at.name_canonical AS away_team,
               m.home_score, m.away_score, m.data_source,
               m.home_team_id, m.away_team_id
        FROM dim_match m
        JOIN dim_season s ON m.season_id = s.season_id
        LEFT JOIN dim_team ht ON m.home_team_id = ht.team_id
        LEFT JOIN dim_team at ON m.away_team_id = at.team_id
        WHERE s.label = :season
    """
    if tid is not None:
        sql += " AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    sql += " ORDER BY m.match_date DESC"
    df = query_df(sql, params)

    if tid is not None and not df.empty:
        def _outcome(row):
            hs, as_ = row["home_score"], row["away_score"]
            if pd.isna(hs) or pd.isna(as_):
                return None
            is_home = row["home_team_id"] == tid
            scored, conceded = (hs, as_) if is_home else (as_, hs)
            if scored > conceded: return "W"
            if scored < conceded: return "L"
            return "D"
        df.insert(0, "result", df.apply(_outcome, axis=1))

    return df.drop(columns=["home_team_id", "away_team_id"])


def get_player_stats(season_label: str, team: str | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": season_label}
    sql = """
        SELECT p.name_canonical AS player,
               p.player_position AS position,
               COUNT(*) AS shots,
               SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
               ROUND(SUM(fs.xg)::numeric, 2) AS xg,
               ROUND(
                   (SUM(fs.xg) / NULLIF(COUNT(*), 0))::numeric, 3
               ) AS xg_per_shot
        FROM fact_shots fs
        JOIN dim_match m   ON fs.match_id = m.match_id
        JOIN dim_season s  ON m.season_id = s.season_id
        JOIN dim_player p  ON fs.player_id = p.player_id
        WHERE s.label = :season
    """
    if tid is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = tid
    sql += """
        GROUP BY p.player_id, p.name_canonical, p.player_position
        ORDER BY xg DESC NULLS LAST
        LIMIT 50
    """
    return query_df(sql, params)


def get_shots_by_source(season_label: str, team: str | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": season_label}
    sql = """
        SELECT fs.data_source,
               COUNT(*) AS shots,
               SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
               ROUND(SUM(fs.xg)::numeric, 2) AS total_xg
        FROM fact_shots fs
        JOIN dim_match m  ON fs.match_id = m.match_id
        JOIN dim_season s ON m.season_id = s.season_id
        WHERE s.label = :season
    """
    if tid is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = tid
    sql += """
        GROUP BY fs.data_source
        ORDER BY shots DESC
    """
    return query_df(sql, params)


def get_injuries(season_label: str, team: str | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": season_label}
    sql = """
        SELECT p.name_canonical AS player,
               p.player_position AS position,
               it.name AS injury_type,
               it.category,
               fi.date_from, fi.date_until,
               fi.days_absent, fi.matches_missed
        FROM fact_injuries fi
        JOIN dim_player p       ON fi.player_id = p.player_id
        LEFT JOIN dim_injury_type it ON fi.injury_type_id = it.injury_type_id
        JOIN dim_season s       ON fi.season_id = s.season_id
        WHERE s.label = :season
    """
    if tid is not None:
        sql += " AND fi.team_id = :tid"
        params["tid"] = tid
    sql += " ORDER BY fi.days_absent DESC NULLS LAST"
    return query_df(sql, params)


def get_events_summary(season_label: str, team: str | None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": season_label}
    sql = """
        SELECT fe.data_source, fe.event_type, COUNT(*) AS count
        FROM fact_events fe
        JOIN dim_match m  ON fe.match_id = m.match_id
        JOIN dim_season s ON m.season_id = s.season_id
        WHERE s.label = :season
          AND fe.event_type IS NOT NULL
    """
    if tid is not None:
        sql += " AND fe.team_id = :tid"
        params["tid"] = tid
    sql += """
        GROUP BY fe.data_source, fe.event_type
        ORDER BY count DESC
        LIMIT 100
    """
    return query_df(sql, params)
