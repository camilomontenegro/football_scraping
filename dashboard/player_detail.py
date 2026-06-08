"""
dashboard/player_detail.py
==========================
Read-only DB queries for the Player Detail tab.

All functions return DataFrames. Query logic is isolated here so app.py
stays focused on layout only.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.db import get_engine, query_df


@st.cache_data(ttl=300)
def get_all_players() -> pd.DataFrame:
    return query_df("""
        SELECT canonical_id, canonical_name, position, nationality, birth_date,
               photo_url,
               id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
        FROM dim_player
        ORDER BY canonical_name
    """)


def get_player_mdm(canonical_id: int) -> pd.DataFrame:
    return query_df("""
        SELECT
            pr.source_system,
            pr.source_name,
            pr.source_id,
            pr.similarity_score,
            pr.resolved
        FROM player_review pr
        WHERE pr.suggested_canonical_id = :cid
           OR pr.canonical_id_assigned   = :cid
        ORDER BY pr.source_system, pr.similarity_score DESC NULLS LAST
    """, {"cid": canonical_id})


def get_player_shots(
    canonical_id: int,
    season: str | None = None,
    source: str | None = None,
    match_id: int | None = None,
) -> pd.DataFrame:
    params: dict = {"cid": canonical_id}
    season_filter = ""
    if season and season != "All":
        season_filter = "AND m.season = :season"
        params["season"] = season
    source_filter = ""
    if source and source != "All":
        source_filter = "AND fs.data_source = :source"
        params["source"] = source
    match_filter = ""
    if match_id is not None:
        match_filter = "AND fs.match_id = :match_id"
        params["match_id"] = match_id
    return query_df(f"""
        WITH _plot_shots AS (
            SELECT
                CASE
                    WHEN fs.x IS NULL THEN NULL
                    WHEN fs.data_source = 'sofascore' AND fs.x <= 1.1 THEN (1 - LEAST(GREATEST(fs.x, 0), 1)) * 105
                    WHEN fs.data_source = 'sofascore' AND fs.x <= 100 THEN (100 - fs.x) * 1.05
                    WHEN fs.x <= 1.1 THEN LEAST(GREATEST(fs.x, 0), 1) * 105
                    WHEN fs.data_source = 'statsbomb' THEN fs.x * 0.875
                    WHEN fs.x <= 100 THEN fs.x * 1.05
                    ELSE fs.x
                END AS x,
                CASE
                    WHEN fs.y IS NULL THEN NULL
                    WHEN fs.y <= 1.1 THEN LEAST(GREATEST(fs.y, 0), 1) * 68
                    WHEN fs.data_source = 'statsbomb' THEN fs.y * 0.85
                    WHEN fs.y <= 100 THEN fs.y * 0.68
                    ELSE fs.y
                END AS y,
                fs.result, fs.xg,
                fs.minute, m.season,
                dc.canonical_name AS competition,
                fs.data_source
            FROM fact_shots fs
            JOIN dim_match m ON m.match_id = fs.match_id
            LEFT JOIN dim_competition dc ON dc.canonical_id = m.competition_id
            WHERE fs.player_id = :cid
              {season_filter}
              {source_filter}
              {match_filter}
        )
        SELECT
            CASE WHEN x IS NULL THEN NULL ELSE LEAST(GREATEST(x, 0), 105) END AS x,
            CASE WHEN y IS NULL THEN NULL ELSE LEAST(GREATEST(y, 0), 68) END AS y,
            result, xg,
            minute, season,
            competition,
            data_source
        FROM _plot_shots
    """, params)


def get_player_shot_matches(
    canonical_id: int,
    season: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Return match list with shot counts for a player (for per-match filtering)."""
    params: dict = {"cid": canonical_id}
    season_filter = ""
    if season and season != "All":
        season_filter = "AND m.season = :season"
        params["season"] = season
    source_filter = ""
    if source and source != "All":
        source_filter = "AND fs.data_source = :source"
        params["source"] = source
    return query_df(f"""
        SELECT
            m.match_id,
            m.match_date,
            m.season,
            COALESCE(dc.canonical_name, m.competition) AS competition,
            ht.canonical_name AS home_team,
            at.canonical_name AS away_team,
            m.home_score,
            m.away_score,
            COUNT(*) AS shots
        FROM fact_shots fs
        JOIN dim_match m ON m.match_id = fs.match_id
        LEFT JOIN dim_competition dc ON dc.canonical_id = m.competition_id
        LEFT JOIN dim_team ht ON ht.canonical_id = m.home_team_id
        LEFT JOIN dim_team at ON at.canonical_id = m.away_team_id
        WHERE fs.player_id = :cid
          {season_filter}
          {source_filter}
        GROUP BY
            m.match_id, m.match_date, m.season, dc.canonical_name, m.competition,
            ht.canonical_name, at.canonical_name, m.home_score, m.away_score
        ORDER BY m.match_date DESC NULLS LAST, m.match_id DESC
    """, params)


def get_player_seasonal_stats(canonical_id: int) -> pd.DataFrame:
    return query_df("""
        SELECT
            m.season,
            dc.canonical_name AS competition,
            COUNT(*)                                         AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
            ROUND(SUM(fs.xg)::numeric, 2)                   AS xg
        FROM fact_shots fs
        JOIN dim_match m ON m.match_id = fs.match_id
        LEFT JOIN dim_competition dc ON dc.canonical_id = m.competition_id
        WHERE fs.player_id = :cid
        GROUP BY m.season, dc.canonical_name
        ORDER BY m.season DESC, dc.canonical_name
    """, {"cid": canonical_id})


def get_player_injuries(canonical_id: int) -> pd.DataFrame:
    return query_df("""
        SELECT season, injury_type, date_from, date_until,
               days_absent, matches_missed
        FROM fact_injuries
        WHERE player_id = :cid
        ORDER BY date_from DESC NULLS LAST
    """, {"cid": canonical_id})


def get_player_shot_seasons(canonical_id: int) -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT m.season
            FROM fact_shots fs
            JOIN dim_match m ON m.match_id = fs.match_id
            WHERE fs.player_id = :cid AND m.season IS NOT NULL
            ORDER BY m.season DESC
        """), {"cid": canonical_id}).fetchall()
    return [r[0] for r in rows]


def get_player_shot_sources(canonical_id: int) -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT data_source
            FROM fact_shots
            WHERE player_id = :cid AND data_source IS NOT NULL
            ORDER BY data_source
        """), {"cid": canonical_id}).fetchall()
    return [r[0] for r in rows]
