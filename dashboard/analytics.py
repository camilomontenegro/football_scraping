"""
dashboard/analytics.py
======================
Read-only DB queries for the Shot Intelligence tab.

All queries filter data_source = 'understat'. The three sources in fact_shots
use incompatible coordinate systems (StatsBomb 0-120x0-80 yards, Understat
0-105x0-68 meters, SofaScore 0-100x0-100 percent). Understat is the only
loaded source and its 105x68m system matches mplsoccer's custom pitch exactly.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

from dashboard.db import get_engine, query_df

_PERIODS = ["00-15", "16-30", "31-45", "46-60", "61-75", "76-90", "90+"]


def _resolve_team_id(team: str | None) -> int | None:
    if team is None:
        return None
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE canonical_name = :n"),
            {"n": team},
        ).fetchone()
    return int(row[0]) if row else None


def get_heatmap_data(season_label: str, team_id: int | None) -> pd.DataFrame:
    """Zone-level shot data for the pitch heatmap.

    Returns columns: x_band, y_band, shots, goals, avg_xg, conversion_rate.
    Zones with fewer than 10 shots are excluded.
    """
    params: dict = {"season": season_label}
    sql = """
        SELECT
            FLOOR(fs.x / 10) * 10            AS x_band,
            FLOOR(fs.y / 10) * 10            AS y_band,
            COUNT(*)                          AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(AVG(fs.xg)::numeric, 3)    AS avg_xg,
            ROUND(
                SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0),
                3
            )                                AS conversion_rate
        FROM fact_shots fs
        JOIN dim_match m   ON fs.match_id  = m.match_id
        WHERE m.season = :season
          AND fs.data_source = 'understat'
          AND fs.x IS NOT NULL
          AND fs.y IS NOT NULL
    """
    if team_id is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = team_id
    sql += """
        GROUP BY x_band, y_band
        HAVING COUNT(*) >= 10
        ORDER BY avg_xg DESC
    """
    return query_df(sql, params)


def get_player_finishing(season_label: str, team_id: int | None) -> pd.DataFrame:
    """Top 20 players by goals minus xG (finishing over/underperformance).

    Minimum 20 shots to qualify.
    """
    params: dict = {"season": season_label}
    sql = """
        SELECT
            p.canonical_name AS player,
            COUNT(*) AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(SUM(fs.xg)::numeric, 2) AS total_xg,
            ROUND(
                SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)::numeric
                - SUM(fs.xg)::numeric,
                2
            ) AS goals_minus_xg
        FROM fact_shots fs
        JOIN dim_player p  ON fs.player_id = p.canonical_id
        JOIN dim_match m   ON fs.match_id  = m.match_id
        WHERE m.season = :season
          AND fs.data_source = 'understat'
    """
    if team_id is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = team_id
    sql += """
        GROUP BY p.canonical_name
        HAVING COUNT(*) >= 20
        ORDER BY goals_minus_xg DESC
        LIMIT 20
    """
    return query_df(sql, params)


def get_shot_type_breakdown(season_label: str, team_id: int | None) -> pd.DataFrame:
    """Conversion rate and avg xG by shot_type. Minimum 10 shots per type."""
    params: dict = {"season": season_label}
    sql = """
        SELECT
            fs.shot_type,
            COUNT(*) AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(
                SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0),
                3
            ) AS conversion_rate,
            ROUND(AVG(fs.xg)::numeric, 3) AS avg_xg
        FROM fact_shots fs
        JOIN dim_match m  ON fs.match_id = m.match_id
        WHERE m.season = :season
          AND fs.data_source = 'understat'
          AND fs.shot_type IS NOT NULL
    """
    if team_id is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = team_id
    sql += """
        GROUP BY fs.shot_type
        HAVING COUNT(*) >= 10
        ORDER BY conversion_rate DESC
    """
    return query_df(sql, params)


def get_situation_breakdown(season_label: str, team_id: int | None) -> pd.DataFrame:
    """Conversion rate and avg xG by situation. Minimum 10 shots per situation."""
    params: dict = {"season": season_label}
    sql = """
        SELECT
            fs.situation,
            COUNT(*) AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(
                SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0),
                3
            ) AS conversion_rate,
            ROUND(AVG(fs.xg)::numeric, 3) AS avg_xg
        FROM fact_shots fs
        JOIN dim_match m  ON fs.match_id = m.match_id
        WHERE m.season = :season
          AND fs.data_source = 'understat'
          AND fs.situation IS NOT NULL
    """
    if team_id is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = team_id
    sql += """
        GROUP BY fs.situation
        HAVING COUNT(*) >= 10
        ORDER BY conversion_rate DESC
    """
    return query_df(sql, params)


def get_period_breakdown(season_label: str, team_id: int | None) -> pd.DataFrame:
    """Avg xG and goal count by 15-minute match period.

    All seven standard periods are always returned; periods with no shots get zeros.
    """
    params: dict = {"season": season_label}
    sql = """
        SELECT
            CASE
                WHEN fs.minute BETWEEN 0  AND 15 THEN '00-15'
                WHEN fs.minute BETWEEN 16 AND 30 THEN '16-30'
                WHEN fs.minute BETWEEN 31 AND 45 THEN '31-45'
                WHEN fs.minute BETWEEN 46 AND 60 THEN '46-60'
                WHEN fs.minute BETWEEN 61 AND 75 THEN '61-75'
                WHEN fs.minute BETWEEN 76 AND 90 THEN '76-90'
                ELSE '90+'
            END AS match_period,
            COUNT(*) AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(AVG(fs.xg)::numeric, 3) AS avg_xg
        FROM fact_shots fs
        JOIN dim_match m  ON fs.match_id = m.match_id
        WHERE m.season = :season
          AND fs.data_source = 'understat'
          AND fs.minute IS NOT NULL
    """
    if team_id is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = team_id
    sql += """
        GROUP BY match_period
        ORDER BY match_period
    """
    df = query_df(sql, params)

    # Ensure all seven periods are present even if a period had no shots.
    if df.empty:
        return pd.DataFrame(
            {"match_period": _PERIODS, "shots": 0, "goals": 0, "avg_xg": 0.0}
        )

    existing = set(df["match_period"])
    missing = [p for p in _PERIODS if p not in existing]
    if missing:
        filler = pd.DataFrame(
            {"match_period": missing, "shots": 0, "goals": 0, "avg_xg": 0.0}
        )
        df = pd.concat([df, filler], ignore_index=True)

    df["match_period"] = pd.Categorical(df["match_period"], categories=_PERIODS, ordered=True)
    return df.sort_values("match_period").reset_index(drop=True)
