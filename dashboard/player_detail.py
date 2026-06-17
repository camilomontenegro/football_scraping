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


@st.cache_data(ttl=300)
def get_player_event_locations(
    canonical_id: int, season: str | None = None,
) -> pd.DataFrame:
    """WhoScored located events for a player (action heatmap).

    fact_events coordinates are normalised 0-1, scaled here to the 105x68 pitch.
    """
    params: dict = {"cid": canonical_id}
    season_filter = ""
    if season and season != "All":
        season_filter = "AND m.season = :season"
        params["season"] = season
    return query_df(f"""
        SELECT fe.x * 105.0 AS x, fe.y * 68.0 AS y, fe.event_type
        FROM fact_events fe
        JOIN dim_match m ON m.match_id = fe.match_id
        WHERE fe.player_id = :cid
          AND fe.data_source = 'whoscored'
          AND fe.x IS NOT NULL AND fe.y IS NOT NULL
          {season_filter}
    """, params)


# ════════════════════════════════════════════════════════════════════
# SUMMARY STATS  (for the LaLiga-style stats grid)
# ════════════════════════════════════════════════════════════════════

def get_player_summary_stats(
    canonical_id: int, season: str | None = None,
) -> dict:
    """Aggregate stats for one player: goals, shots, xG, cards, matches, penalties."""
    params: dict = {"cid": canonical_id}
    season_filter_shots = ""
    season_filter_events = ""
    if season and season != "All":
        season_filter_shots = "AND m.season = :season"
        season_filter_events = "AND m.season = :season"
        params["season"] = season

    # ── Shots / goals / xG / penalties ──────────────────────────
    shots_df = query_df(f"""
        SELECT
            COUNT(*)                                              AS shots,
            SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END)  AS goals,
            ROUND(COALESCE(SUM(fs.xg), 0)::numeric, 2)           AS xg,
            COUNT(DISTINCT fs.match_id)                           AS matches,
            SUM(CASE WHEN LOWER(fs.situation) = 'penalty' THEN 1 ELSE 0 END) AS penalties,
            SUM(CASE WHEN LOWER(fs.situation) = 'penalty'
                      AND fs.result = 'Goal' THEN 1 ELSE 0 END)  AS penalty_goals
        FROM fact_shots fs
        JOIN dim_match m ON m.match_id = fs.match_id
        WHERE fs.player_id = :cid {season_filter_shots}
    """, params)

    # ── Cards from events ───────────────────────────────────────
    cards_df = query_df(f"""
        SELECT
            SUM(CASE WHEN LOWER(fe.event_type) LIKE '%%yellow%%' THEN 1 ELSE 0 END) AS yellows,
            SUM(CASE WHEN LOWER(fe.event_type) LIKE '%%red%%'
                       OR LOWER(fe.event_type) LIKE '%%dismissal%%' THEN 1 ELSE 0 END) AS reds
        FROM fact_events fe
        JOIN dim_match m ON m.match_id = fe.match_id
        WHERE fe.player_id = :cid {season_filter_events}
    """, params)

    # ── Team (most frequent) ───────────────────────────────────
    team_df = query_df(f"""
        SELECT dt.canonical_name AS team
        FROM fact_shots fs
        JOIN dim_team dt ON dt.canonical_id = fs.team_id
        JOIN dim_match m ON m.match_id = fs.match_id
        WHERE fs.player_id = :cid {season_filter_shots}
        GROUP BY dt.canonical_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """, params)

    s = shots_df.iloc[0] if not shots_df.empty else {}
    c = cards_df.iloc[0] if not cards_df.empty else {}
    return {
        "goals":         int(s.get("goals", 0) or 0),
        "shots":         int(s.get("shots", 0) or 0),
        "xg":            float(s.get("xg", 0) or 0),
        "matches":       int(s.get("matches", 0) or 0),
        "penalties":     int(s.get("penalties", 0) or 0),
        "penalty_goals": int(s.get("penalty_goals", 0) or 0),
        "yellows":       int(c.get("yellows", 0) or 0),
        "reds":          int(c.get("reds", 0) or 0),
        "team":          team_df.iloc[0]["team"] if not team_df.empty else None,
    }


# ════════════════════════════════════════════════════════════════════
# RADAR DATA  (player per-match metrics + league avg + head-to-head)
# ════════════════════════════════════════════════════════════════════

_RADAR_METRICS = ["Goals/match", "Shots/match", "xG/match",
                  "Conversion %", "Penalties/match"]


def _player_radar_row(
    canonical_id: int,
    season: str | None = None,
    competition_id: int | None = None,
) -> list[float] | None:
    """Per-match radar values for one player. Returns list of 5 floats or None."""
    params: dict = {"cid": canonical_id}
    filters = ""
    if season and season != "All":
        filters += " AND m.season = :season"
        params["season"] = season
    if competition_id is not None:
        filters += " AND m.competition_id = :compid"
        params["compid"] = competition_id

    df = query_df(f"""
        SELECT
            COUNT(DISTINCT fs.match_id)                            AS matches,
            COUNT(*)::float / NULLIF(COUNT(DISTINCT fs.match_id), 0) AS shots_pm,
            SUM(CASE WHEN fs.result='Goal' THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS goals_pm,
            COALESCE(SUM(fs.xg), 0)::float
                / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS xg_pm,
            CASE WHEN COUNT(*) > 0
                THEN SUM(CASE WHEN fs.result='Goal' THEN 1 ELSE 0 END)::float / COUNT(*)
                ELSE 0 END                                         AS conversion,
            SUM(CASE WHEN LOWER(fs.situation)='penalty' THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS penalties_pm
        FROM fact_shots fs
        JOIN dim_match m ON m.match_id = fs.match_id
        WHERE fs.player_id = :cid {filters}
    """, params)
    if df.empty or pd.isna(df.iloc[0]["matches"]) or int(df.iloc[0]["matches"]) == 0:
        return None
    r = df.iloc[0]

    def _safe(v):
        return 0.0 if pd.isna(v) else float(v)

    return [
        _safe(r["goals_pm"]),
        _safe(r["shots_pm"]),
        _safe(r["xg_pm"]),
        _safe(r["conversion"]) * 100,
        _safe(r["penalties_pm"]),
    ]


def get_player_primary_competition(
    canonical_id: int, season: str | None = None,
) -> tuple[int, str] | None:
    """Return (competition_id, competition_name) where the player has most shots."""
    params: dict = {"cid": canonical_id}
    season_filter = ""
    if season and season != "All":
        season_filter = "AND m.season = :season"
        params["season"] = season
    df = query_df(f"""
        SELECT m.competition_id,
               COALESCE(dc.canonical_name, m.competition) AS comp_name
        FROM fact_shots fs
        JOIN dim_match m ON m.match_id = fs.match_id
        LEFT JOIN dim_competition dc ON dc.canonical_id = m.competition_id
        WHERE fs.player_id = :cid {season_filter}
          AND m.competition_id IS NOT NULL
        GROUP BY m.competition_id, dc.canonical_name, m.competition
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """, params)
    if df.empty:
        return None
    return int(df.iloc[0]["competition_id"]), str(df.iloc[0]["comp_name"] or "League")


def get_league_avg_radar(
    competition_id: int,
    season: str | None = None,
    exclude_player: int | None = None,
) -> list[float] | None:
    """Per-player per-match averages across the whole competition.

    Computes per-match stats for each player individually, then averages
    across players.  The old formula (total / players / matches) assumed
    every player appeared in every match, giving numbers ~10× too low.
    """
    params: dict = {"compid": competition_id}
    filters = ""
    if season and season != "All":
        filters += " AND m.season = :season"
        params["season"] = season
    exclude = ""
    if exclude_player is not None:
        exclude = " AND fs.player_id != :excl"
        params["excl"] = exclude_player

    df = query_df(f"""
        WITH player_stats AS (
            SELECT
                fs.player_id,
                COUNT(DISTINCT fs.match_id)                            AS matches,
                COUNT(*)::float
                    / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS shots_pm,
                SUM(CASE WHEN fs.result='Goal' THEN 1 ELSE 0 END)::float
                    / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS goals_pm,
                COALESCE(SUM(fs.xg), 0)::float
                    / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS xg_pm,
                CASE WHEN COUNT(*) > 0
                    THEN SUM(CASE WHEN fs.result='Goal' THEN 1 ELSE 0 END)::float / COUNT(*)
                    ELSE 0 END                                         AS conversion,
                SUM(CASE WHEN LOWER(fs.situation)='penalty' THEN 1 ELSE 0 END)::float
                    / NULLIF(COUNT(DISTINCT fs.match_id), 0)           AS penalties_pm
            FROM fact_shots fs
            JOIN dim_match m ON m.match_id = fs.match_id
            WHERE m.competition_id = :compid {filters} {exclude}
            GROUP BY fs.player_id
            HAVING COUNT(DISTINCT fs.match_id) >= 1
        )
        SELECT
            AVG(shots_pm)      AS shots_pm,
            AVG(goals_pm)      AS goals_pm,
            AVG(xg_pm)         AS xg_pm,
            AVG(conversion)    AS conversion,
            AVG(penalties_pm)  AS penalties_pm
        FROM player_stats
    """, params)
    if df.empty or pd.isna(df.iloc[0]["shots_pm"]):
        return None
    r = df.iloc[0]

    def _safe(v):
        return 0.0 if pd.isna(v) else float(v)

    return [
        _safe(r["goals_pm"]),
        _safe(r["shots_pm"]),
        _safe(r["xg_pm"]),
        _safe(r["conversion"]) * 100,
        _safe(r["penalties_pm"]),
    ]
