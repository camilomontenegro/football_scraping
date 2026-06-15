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


# ════════════════════════════════════════════════════════════════════
# MARKET VALUE  (market value tab)
# ════════════════════════════════════════════════════════════════════

def get_market_value_history(canonical_id: int) -> pd.DataFrame:
    
    """Full market value history for a player, ordered by date."""

    return query_df("""
        SELECT value_date, market_value, club_name, id_tm_club
        FROM fact_market_value
        WHERE player_id = :cid
        ORDER BY value_date
    """, {"cid": canonical_id})


def get_transfer_history(canonical_id: int) -> pd.DataFrame:
    
    """Transfer history for a player — used as milestone markers on the chart."""

    return query_df("""
        SELECT transfer_date, season, from_team_name, to_team_name,
               fee_euros, transfer_type, is_loan
        FROM fact_transfers
        WHERE player_id = :cid
          AND transfer_date IS NOT NULL
        ORDER BY transfer_date
    """, {"cid": canonical_id})


def get_market_value_benchmark(position: str) -> pd.DataFrame:
    """
    Computes market value percentiles (P25, P50, P75) by age
    for all players at the given position in the database.

    Used to draw the benchmark band on the market value chart —
    the green shaded area shows where 50% of players at that position
    and age fall, and the dashed line shows the median (typical) value.

    Age is computed at the time of each valuation by subtracting
    birth_date from value_date, so the same player contributes one
    data point per valuation at the age they had at that moment.

    Example output (one row per age):
        age | p25     | median   | p75
        25  | 425000  | 1000000  | 4000000
        26  | 475000  | 1200000  | 4212500
        ...

    Parameters:
        position (str): player position as stored in dim_player
                        e.g. 'Defensa central', 'Delantero centro'

    Returns:
        DataFrame with columns: age, p25, median, p75
    """
    return query_df("""
        SELECT
            EXTRACT(YEAR FROM AGE(fmv.value_date, dp.birth_date))::int AS age,
            percentile_cont(0.25) WITHIN GROUP (ORDER BY fmv.market_value) AS p25,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY fmv.market_value) AS median,
            percentile_cont(0.75) WITHIN GROUP (ORDER BY fmv.market_value) AS p75
        FROM fact_market_value fmv
        JOIN dim_player dp ON fmv.player_id = dp.canonical_id
        WHERE dp.position = :position
          AND dp.birth_date IS NOT NULL
          AND fmv.market_value > 0
        GROUP BY age
        ORDER BY age
    """, {"position": position})


def get_market_value_kpis(canonical_id: int) -> dict:
    """
    Computes KPI panel values:
        - Current value (latest valuation)
        - Peak value and date
        - % change from peak
        - Change over last year
        - Number of transfers
    """
    mv_df       = get_market_value_history(canonical_id)
    transfer_df = get_transfer_history(canonical_id)

    if mv_df.empty:
        return {
            "current_value":      None,
            "peak_value":         None,
            "peak_date":          None,
            "pct_from_peak":      None,
            "change_last_year":   None,
            "num_transfers":      0,
        }

    mv_df["value_date"] = pd.to_datetime(mv_df["value_date"])
    mv_df = mv_df.sort_values("value_date")

    # current value — latest valuation
    latest_row    = mv_df.iloc[-1]
    current_value = int(latest_row["market_value"])

    # peak value and date
    peak_idx   = mv_df["market_value"].idxmax()
    peak_row   = mv_df.loc[peak_idx]
    peak_value = int(peak_row["market_value"])
    peak_date  = peak_row["value_date"]

    # % change from peak
    pct_from_peak = ((current_value - peak_value) / peak_value * 100) if peak_value else None

    # change over last year
    one_year_ago     = latest_row["value_date"] - pd.DateOffset(years=1)
    mv_one_year_ago  = mv_df[mv_df["value_date"] <= one_year_ago]
    change_last_year = (
        current_value - int(mv_one_year_ago.iloc[-1]["market_value"])
        if not mv_one_year_ago.empty else None
    )

    # count transfers with economic information — excludes unknown
    num_transfers = len(transfer_df[
        transfer_df["transfer_type"].isin(["transfer", "loan", "end_of_loan", "free"])
    ]) if not transfer_df.empty else 0

    return {
        "current_value":    current_value,
        "peak_value":       peak_value,
        "peak_date":        peak_date,
        "pct_from_peak":    round(pct_from_peak, 1) if pct_from_peak is not None else None,
        "change_last_year": change_last_year,
        "num_transfers":    num_transfers,
    }

# ════════════════════════════════════════════════════════════════════
# PLAYER_DETAIL  - TEAM RECORD
# ════════════════════════════════════════════════════════════════════
def get_player_team_history(
    canonical_id: int,
    season: str | None = None,
    all_time: bool = False
) -> pd.DataFrame:
    """
    Returns the team history for a player using LEAD() to compute
    the period spent at each club.

    Each row represents a spell at a club:
        - date_from: arrival date (transfer_date to that club)
        - date_to:   departure date (next transfer_date) — NULL means still there
        - team:      club name

    LEAD() is computed over the full transfer history first, then filtered
    by season date range if provided — this ensures players who stayed at a
    club across multiple seasons appear correctly when filtering by season.

    Season date range: e.g. '2022/2023' → start=2022-07-01, end=2023-06-30
    This follows the football season convention: starts July 1st, ends June 30th.

    Excludes 'Retirado' entries — handled separately via is_player_retired().

    Parameters:
        canonical_id (int): player canonical_id from dim_player
        season       (str): season in format '2024/2025', or None for all
        all_time    (bool): if True, ignore season filter entirely

    Returns:
        DataFrame with columns: season, date_from, date_to, team
    """
    params: dict = {"cid": canonical_id, "season_start": None, "season_end": None}

    if season and season != "All" and not all_time:
        # convert season string to date range
        # e.g. '2022/2023' → start=2022-07-01, end=2023-06-30
        start_year = int(season.split("/")[0])
        end_year   = int(season.split("/")[1])
        params["season_start"] = f"{start_year}-07-01"
        params["season_end"]   = f"{end_year}-06-30"

    return query_df("""
    WITH career AS (
        SELECT
            ft.season,
            ft.transfer_date                                AS date_from,
            LEAD(ft.transfer_date) OVER (
                PARTITION BY ft.player_id
                ORDER BY ft.transfer_date
            )                                               AS date_to,
            ft.to_team_name                                 AS team
        FROM fact_transfers ft
        WHERE ft.player_id = :cid
          AND ft.to_team_name IS NOT NULL
    )
    SELECT * FROM career
    WHERE (
        :season_start IS NULL
        OR (date_from <= :season_end AND (date_to >= :season_start OR date_to IS NULL))
    )
    ORDER BY date_from DESC NULLS LAST
""", params)

def is_player_retired(canonical_id: int) -> bool:
    """
    Returns True if the player has a 'Retirado' entry in fact_transfers,
    indicating they have retired from professional football.
    """
    df = query_df("""
        SELECT 1 FROM fact_transfers
        WHERE player_id = :cid
          AND to_team_name = 'Retirado'
        LIMIT 1
    """, {"cid": canonical_id})
    return not df.empty


# ════════════════════════════════════════════════════════════════════
# TRANSFER HISTORY
# ════════════════════════════════════════════════════════════════════
def get_transfer_history_kpis(canonical_id: int) -> dict:
    """
    Computes KPI values for the Transfer History tab:
        - total_fees: sum of fee_euros for all transfers with known fee
        - max_fee: highest single transfer fee
        - max_fee_team: destination team of the most expensive transfer
        - max_fee_date: date of the most expensive transfer

    Only counts transfer_type = 'transfer' for fees — excludes loans and free transfers.

    Returns:
        dict with keys: total_fees, max_fee, max_fee_team, max_fee_date
    """
    df = query_df("""
        SELECT
            SUM(fee_euros)                          AS total_fees,
            MAX(fee_euros)                          AS max_fee
        FROM fact_transfers
        WHERE player_id = :cid
          AND transfer_type = 'transfer'
          AND fee_euros IS NOT NULL
          AND fee_euros > 0
    """, {"cid": canonical_id})

    # get the most expensive transfer details
    max_df = query_df("""
        SELECT to_team_name, transfer_date, fee_euros
        FROM fact_transfers
        WHERE player_id = :cid
          AND transfer_type = 'transfer'
          AND fee_euros IS NOT NULL
          AND fee_euros > 0
        ORDER BY fee_euros DESC
        LIMIT 1
    """, {"cid": canonical_id})

    row = df.iloc[0] if not df.empty else {}
    max_row = max_df.iloc[0] if not max_df.empty else {}

    return {
        "total_fees":   int(row.get("total_fees") or 0),
        "max_fee":      int(max_row.get("fee_euros") or 0),
        "max_fee_team": max_row.get("to_team_name"),
        "max_fee_date": max_row.get("transfer_date"),
    }