"""
dashboard/analytics.py
======================
Read-only DB queries for the Shot Intelligence tab.

Coordinate normalisation (all sources converted to 0-105 m × 0-68 m):
  - Understat raw (0-1):   x * 105,  y * 68   (detected when x <= 1.1)
  - Understat meters:      as-is
  - SofaScore (0-100 %):  x * 1.05, y * 0.68
  - StatsBomb (120×80 yd): x * 0.875, y * 0.85
  - WhoScored (0-100):     x * 1.05,  y * 0.68
Normalisation is applied inline in SQL so it works regardless of whether
the database was populated with raw or pre-normalised values.

Data-source agnostic: queries work with shots from any scraper source,
including continental competitions (Champions League, Europa League, etc.)
that may only have SofaScore or StatsBomb data.
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


def _fact_events_has_end_z() -> bool:
    """True if the optional end_z column exists (db/add_end_z_fact_events.sql)."""
    df = query_df("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'fact_events' AND column_name = 'end_z'
    """)
    return not df.empty


def get_match_shots(match_id: int, team_id: int) -> pd.DataFrame:
    """Shot events of one team in one match (WhoScored).

    Returns: player, event_type, minute, x, y, end_x, end_y, end_z —
    coordinates normalised 0-1. event_type ∈ Goal / MissedShots /
    SavedShot / ShotOnPost.
    end_x/end_y (goal-line crossing point) and end_z (height, optional
    column) are NULL until the pipeline populates them from WhoScored's
    goalMouthY/goalMouthZ — the UI draws trajectories and the goal-mouth
    view only for rows that have them.
    Penalty-shootout attempts (minute > 120) are excluded — they all sit
    on the penalty spot and would distort the map and the shot counts.
    """
    end_z_col = "e.end_z" if _fact_events_has_end_z() else "NULL AS end_z"
    df = query_df(f"""
        SELECT p.canonical_name AS player, e.event_type, e.minute,
               e.x, e.y, e.end_x, e.end_y, {end_z_col}
        FROM fact_events e
        JOIN dim_player p ON p.canonical_id = e.player_id
        WHERE e.match_id = :mid
          AND e.team_id = :tid
          AND e.data_source = 'whoscored'
          AND e.event_type IN ('Goal', 'MissedShots', 'SavedShot', 'ShotOnPost')
          AND e.minute <= 120
          AND e.x IS NOT NULL AND e.y IS NOT NULL
        ORDER BY e.minute, e.second
    """, {"mid": match_id, "tid": team_id})
    if df.empty:
        return df
    for col in ("x", "y", "end_x", "end_y", "end_z"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def get_heatmap_data(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Zone-level shot data for the pitch heatmap.

    Returns columns: x_band, y_band, shots, goals, avg_xg, conversion_rate.
    Zones with fewer than 10 shots are excluded.
    """
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition
    team_filter = ""
    if team_id is not None:
        team_filter = "AND fs.team_id = :tid"
        params["tid"] = team_id
    sql = f"""
        WITH _norm AS (
            SELECT
                CASE fs.data_source
                    WHEN 'understat' THEN
                        CASE WHEN fs.x <= 1.1 THEN fs.x * 105 ELSE fs.x END
                    WHEN 'sofascore' THEN (100 - fs.x) * 1.05
                    WHEN 'statsbomb' THEN fs.x * 0.875
                    WHEN 'whoscored' THEN fs.x * 1.05
                    ELSE fs.x
                END AS x_m,
                CASE fs.data_source
                    WHEN 'understat' THEN
                        CASE WHEN fs.y <= 1.1 THEN fs.y * 68 ELSE fs.y END
                    WHEN 'sofascore' THEN fs.y * 0.68
                    WHEN 'statsbomb' THEN fs.y * 0.85
                    WHEN 'whoscored' THEN fs.y * 0.68
                    ELSE fs.y
                END AS y_m,
                fs.result,
                fs.xg
            FROM fact_shots fs
            JOIN dim_match m ON fs.match_id = m.match_id
            {comp_join}
            WHERE m.season = :season
              AND fs.x IS NOT NULL
              AND fs.y IS NOT NULL
              {comp_filter}
              {team_filter}
        )
        SELECT
            FLOOR(x_m / 10) * 10             AS x_band,
            FLOOR(y_m / 10) * 10             AS y_band,
            COUNT(*)                          AS shots,
            SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(AVG(xg)::numeric, 3)       AS avg_xg,
            ROUND(
                SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END)::numeric
                / NULLIF(COUNT(*), 0),
                3
            )                                AS conversion_rate
        FROM _norm
        GROUP BY x_band, y_band
        HAVING COUNT(*) >= 10
        ORDER BY avg_xg DESC
    """
    return query_df(sql, params)


def get_player_finishing(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Top 20 players by goals minus xG (finishing over/underperformance).

    Minimum 20 shots to qualify.
    """
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition
    team_filter = ""
    if team_id is not None:
        team_filter = "AND fs.team_id = :tid"
        params["tid"] = team_id
    sql = f"""
        SELECT
            p.canonical_name AS player,
            COUNT(*) AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(COALESCE(SUM(fs.xg), 0)::numeric, 2) AS total_xg,
            ROUND(
                SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)::numeric
                - COALESCE(SUM(fs.xg), 0)::numeric,
                2
            ) AS goals_minus_xg
        FROM fact_shots fs
        JOIN dim_player p ON fs.player_id = p.canonical_id
        JOIN dim_match m  ON fs.match_id  = m.match_id
        {comp_join}
        WHERE m.season = :season
          {comp_filter}
          {team_filter}
        GROUP BY p.canonical_name
        HAVING COUNT(*) >= 20
        ORDER BY goals_minus_xg DESC
        LIMIT 20
    """
    return query_df(sql, params)


def get_shot_type_breakdown(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Conversion rate and avg xG by shot_type. Minimum 10 shots per type."""
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition
    sql = f"""
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
        {comp_join}
        WHERE m.season = :season
          AND fs.shot_type IS NOT NULL
          {comp_filter}
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


def get_situation_breakdown(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Conversion rate and avg xG by situation. Minimum 10 shots per situation."""
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition
    sql = f"""
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
        {comp_join}
        WHERE m.season = :season
          AND fs.situation IS NOT NULL
          {comp_filter}
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


def get_period_breakdown(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Avg xG and goal count by 15-minute match period.

    All seven standard periods are always returned; periods with no shots get zeros.
    """
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition
    sql = f"""
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
        {comp_join}
        WHERE m.season = :season
          AND fs.minute IS NOT NULL
          {comp_filter}
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


def get_setpiece_goals(
    season_label: str,
    team_id: int | None,
    player_id: int | None = None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Set-piece goal stats for the Set-piece Specialists section.

    player_id=None  → ranked table: player, team, penalty_goals, freekick_goals,
                       openplay_goals, total_goals (only players with pen/fk goals).
    player_id=<id>  → bucket breakdown: situation_bucket, goals (for one player).

    Data source: fact_shots (all sources) filtered to result='Goal'.
    Situation normalisation via LOWER() handles mixed casing across scrapers.
    """
    params: dict = {"season": season_label}
    comp_join = ""
    comp_filter = ""
    if competition is not None:
        comp_join = "JOIN dim_competition dc ON dc.canonical_id = m.competition_id"
        comp_filter = "AND dc.canonical_name = :competition"
        params["competition"] = competition

    if player_id is None:
        team_filter = ""
        if team_id is not None:
            team_filter = "AND fs.team_id = :tid"
            params["tid"] = team_id
        sql = f"""
            SELECT
                p.canonical_name AS player,
                t.canonical_name AS team,
                SUM(CASE WHEN LOWER(fs.situation) = 'penalty'
                         THEN 1 ELSE 0 END)                                 AS penalty_goals,
                SUM(CASE WHEN LOWER(fs.situation) IN ('direct freekick','free-kick')
                         THEN 1 ELSE 0 END)                                 AS freekick_goals,
                SUM(CASE WHEN LOWER(fs.situation) IN ('open play','assisted','fast-break','regular')
                         THEN 1 ELSE 0 END)                                 AS openplay_goals,
                SUM(CASE WHEN LOWER(fs.situation) NOT IN (
                              'penalty','direct freekick','free-kick',
                              'open play','assisted','fast-break','regular')
                         THEN 1 ELSE 0 END)                                 AS setpiece_other_goals,
                COUNT(*)                                                     AS total_goals
            FROM fact_shots fs
            JOIN dim_player p ON fs.player_id  = p.canonical_id
            JOIN dim_team   t ON fs.team_id    = t.canonical_id
            JOIN dim_match  m ON fs.match_id   = m.match_id
            {comp_join}
            WHERE fs.result      = 'Goal'
              AND m.season       = :season
              {comp_filter}
              {team_filter}
            GROUP BY p.canonical_id, p.canonical_name, t.canonical_id, t.canonical_name
            HAVING
                SUM(CASE WHEN LOWER(fs.situation) = 'penalty' THEN 1 ELSE 0 END) > 0
                OR SUM(CASE WHEN LOWER(fs.situation) IN ('direct freekick','free-kick')
                            THEN 1 ELSE 0 END) > 0
            ORDER BY penalty_goals DESC, freekick_goals DESC
        """
    else:
        params["pid"] = player_id
        sql = f"""
            SELECT
                CASE
                    WHEN LOWER(fs.situation) = 'penalty'
                         THEN 'Penalty'
                    WHEN LOWER(fs.situation) IN ('direct freekick','free-kick')
                         THEN 'Free Kick'
                    WHEN LOWER(fs.situation) IN ('open play','assisted','fast-break','regular')
                         THEN 'Open Play'
                    ELSE 'Set Piece / Other'
                END AS situation_bucket,
                COUNT(*) AS goals
            FROM fact_shots fs
            JOIN dim_match m ON fs.match_id = m.match_id
            {comp_join}
            WHERE fs.result      = 'Goal'
              AND m.season       = :season
              AND fs.player_id   = :pid
              {comp_filter}
            GROUP BY situation_bucket
            ORDER BY goals DESC
        """

    return query_df(sql, params)
