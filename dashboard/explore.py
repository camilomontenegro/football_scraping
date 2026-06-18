"""
dashboard/explore.py
====================
Read-only DB queries for the Exploration tab.

Column names match the live schema (create_tables.sql):
  dim_team:   canonical_id (PK), canonical_name
  dim_player: canonical_id (PK), canonical_name, position
  dim_match:  season VARCHAR (no dim_season table)
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
from sqlalchemy import text

from dashboard.db import get_engine, query_df

try:
    from wizard.competitions import WORKING_COMPETITION_NAMES
except Exception:  # pragma: no cover - fallback si el modulo no esta disponible
    WORKING_COMPETITION_NAMES = set()


def _short_season(label: str) -> str:
    """Convert '2020/2021' -> '20/21' to match fact_injuries season format."""
    parts = label.split("/")
    if len(parts) == 2 and len(parts[0]) == 4 and len(parts[1]) == 4:
        return f"{parts[0][2:]}/{parts[1][2:]}"
    return label


def _comp_clause(competition: str | None, match_alias: str = "m") -> tuple[str, str]:
    """Return (JOIN snippet, WHERE snippet) for filtering dim_match by competition.

    Uses the competition_id FK so the filter is immune to raw-string variation
    in the competition VARCHAR column.
    """
    if competition is None:
        return "", ""
    join = (
        f"JOIN dim_competition dc"
        f" ON dc.canonical_id = {match_alias}.competition_id"
    )
    where = "AND dc.canonical_name = :competition"
    return join, where


def _sql_is_yellow_card(fe_alias: str = "fe") -> str:
    """SQL predicate for a yellow-card event (WhoScored + SofaScore + legacy strings)."""
    fe = fe_alias
    return f"""(
        ({fe}.event_type ILIKE '%%yellow%%' AND {fe}.event_type NOT ILIKE '%%red%%')
        OR (LOWER({fe}.event_type) = 'card' AND LOWER({fe}.outcome) = 'yellow')
        OR (
            LOWER({fe}.event_type) = 'card'
            AND ({fe}.qualifiers->>'Yellow')::boolean IS TRUE
            AND COALESCE(({fe}.qualifiers->>'SecondYellow')::boolean, FALSE) IS NOT TRUE
        )
    )"""


def _sql_is_second_yellow_card(fe_alias: str = "fe") -> str:
    """SQL predicate for a second-yellow dismissal."""
    fe = fe_alias
    return f"""(
        ({fe}.event_type ILIKE '%%yellow%%red%%')
        OR (LOWER({fe}.event_type) = 'card' AND LOWER({fe}.outcome) = 'yellowred')
        OR (
            LOWER({fe}.event_type) = 'card'
            AND ({fe}.qualifiers->>'SecondYellow')::boolean IS TRUE
        )
    )"""


def _sql_is_direct_red_card(fe_alias: str = "fe") -> str:
    """SQL predicate for a direct red card, excluding second yellows."""
    fe = fe_alias
    return f"""(
        ({fe}.event_type ILIKE '%%red%%' AND {fe}.event_type NOT ILIKE '%%yellow%%')
        OR (LOWER({fe}.event_type) = 'card' AND LOWER({fe}.outcome) = 'red')
        OR (
            LOWER({fe}.event_type) = 'card'
            AND (
                ({fe}.qualifiers->>'RedCard')::boolean IS TRUE
                OR ({fe}.qualifiers->>'Red')::boolean IS TRUE
            )
            AND COALESCE(({fe}.qualifiers->>'SecondYellow')::boolean, FALSE) IS NOT TRUE
        )
    )"""


def _sql_is_red_card(fe_alias: str = "fe") -> str:
    """SQL predicate for a red-card event (direct red or second yellow)."""
    fe = fe_alias
    return f"""(
        {_sql_is_direct_red_card(fe_alias)}
        OR {_sql_is_second_yellow_card(fe_alias)}
    )"""


@st.cache_data(ttl=300)
def get_competitions() -> list[str]:
    """Lista las competiciones del dashboard.

    Se filtra por `WORKING_COMPETITION_NAMES` (definido en
    `wizard/competitions.py`) para que en la UI solo aparezcan las
    ligas/torneos con los que trabajamos hoy. Si por cualquier motivo
    el conjunto esta vacio (modulo no importable o sin entradas),
    cae al comportamiento antiguo de mostrar todas.
    """
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT canonical_name FROM dim_competition ORDER BY canonical_name"
        )).fetchall()
    names = [r[0] for r in rows]
    if WORKING_COMPETITION_NAMES:
        names = [n for n in names if n in WORKING_COMPETITION_NAMES]
    return names or ["La Liga"]


@st.cache_data(ttl=300)
def get_seasons_for_competition(competition: str) -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT m.season
            FROM dim_match m
            JOIN dim_competition c ON c.canonical_id = m.competition_id
            WHERE m.season IS NOT NULL AND c.canonical_name = :competition
            ORDER BY m.season DESC
        """), {"competition": competition}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def get_teams_for_season(season_label: str, competition: str | None = None) -> list[str]:
    eng = get_engine()
    params: dict = {"season": season_label}
    comp_join, comp_filter = _comp_clause(competition)
    if competition is not None:
        params["competition"] = competition
    with eng.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT DISTINCT t.canonical_name
            FROM dim_match m
            {comp_join}
            JOIN dim_team t ON t.canonical_id IN (m.home_team_id, m.away_team_id)
            WHERE m.season = :season {comp_filter}
            ORDER BY t.canonical_name
        """), params).fetchall()
    return [r[0] for r in rows]


def _team_id(conn, team: str | None) -> int | None:
    if team is None:
        return None
    row = conn.execute(text(
        "SELECT canonical_id FROM dim_team WHERE canonical_name = :n"
    ), {"n": team}).fetchone()
    return int(row[0]) if row else None


def get_season_summary(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> dict:
    comp_join, comp_filter = _comp_clause(competition)
    comp_params = {"competition": competition} if competition else {}

    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)

        if tid is None:
            matches = conn.execute(text(f"""
                SELECT COUNT(*) FROM dim_match m {comp_join}
                WHERE m.season = :season {comp_filter}
            """), {"season": season_label, **comp_params}).scalar() or 0

            goals = conn.execute(text(f"""
                SELECT COALESCE(SUM(m.home_score), 0) + COALESCE(SUM(m.away_score), 0)
                FROM dim_match m {comp_join}
                WHERE m.season = :season {comp_filter}
            """), {"season": season_label, **comp_params}).scalar() or 0

            xg = conn.execute(text(f"""
                SELECT COALESCE(SUM(fs.xg), 0)
                FROM fact_shots fs
                JOIN dim_match m ON fs.match_id = m.match_id
                {comp_join}
                WHERE m.season = :season {comp_filter}
            """), {"season": season_label, **comp_params}).scalar() or 0

            injuries = conn.execute(text(
                "SELECT COUNT(*) FROM fact_injuries WHERE season = :season"
            ), {"season": _short_season(season_label)}).scalar() or 0
        else:
            matches = conn.execute(text(f"""
                SELECT COUNT(*) FROM dim_match m {comp_join}
                WHERE m.season = :season {comp_filter}
                  AND (m.home_team_id = :tid OR m.away_team_id = :tid)
            """), {"season": season_label, "tid": tid, **comp_params}).scalar() or 0

            goals = conn.execute(text(f"""
                SELECT COALESCE(SUM(
                    CASE
                        WHEN m.home_team_id = :tid THEN m.home_score
                        WHEN m.away_team_id = :tid THEN m.away_score
                        ELSE 0
                    END
                ), 0)
                FROM dim_match m {comp_join}
                WHERE m.season = :season {comp_filter}
                  AND (m.home_team_id = :tid OR m.away_team_id = :tid)
            """), {"season": season_label, "tid": tid, **comp_params}).scalar() or 0

            xg = conn.execute(text(f"""
                SELECT COALESCE(SUM(fs.xg), 0)
                FROM fact_shots fs
                JOIN dim_match m ON fs.match_id = m.match_id
                {comp_join}
                WHERE m.season = :season {comp_filter}
                  AND fs.team_id = :tid
            """), {"season": season_label, "tid": tid, **comp_params}).scalar() or 0

            injuries = conn.execute(text("""
                SELECT COUNT(*) FROM fact_injuries fi
                WHERE fi.season = :short_season
                  AND fi.player_id IN (
                      SELECT fe.player_id FROM fact_events fe
                      JOIN dim_match m ON m.match_id = fe.match_id
                      WHERE fe.team_id = :tid
                        AND m.season  = :season
                  )
            """), {"short_season": _short_season(season_label), "season": season_label, "tid": tid}).scalar() or 0

    return {
        "matches":  int(matches),
        "goals":    int(goals),
        "xg":       round(float(xg), 1),
        "injuries": int(injuries),
    }


def get_results(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    stadium_join = _match_stadium_join()
    stadium_cols = ""
    if stadium_join:
        venue_src = (
            "m.match_venue_source"
            if _match_stadium_column_exists()
            else "NULL::varchar AS match_venue_source"
        )
        stadium_cols = (
            f", {_match_stadium_name_expr()} AS stadium,"
            f" m.venue_name, {venue_src}"
        )
    sql = f"""
        SELECT m.match_date, m.season,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score, m.data_source,
               m.home_team_id, m.away_team_id,
               m.attendance,
               m.temperature_c, m.humidity_pct,
               m.precipitation_mm, m.wind_speed_kmh, m.weather_code
               {stadium_cols}
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
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


def get_player_stats(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    sql = f"""
        SELECT p.canonical_name AS player,
               p.position,
               COUNT(*) AS shots,
               SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
               ROUND(SUM(fs.xg)::numeric, 2) AS xg,
               ROUND((SUM(fs.xg) / NULLIF(COUNT(*), 0))::numeric, 3) AS xg_per_shot
        FROM fact_shots fs
        JOIN dim_match m  ON fs.match_id  = m.match_id
        {comp_join}
        JOIN dim_player p ON fs.player_id = p.canonical_id
        WHERE m.season = :season {comp_filter}
    """
    if tid is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = tid
    sql += """
        GROUP BY p.canonical_id, p.canonical_name, p.position
        ORDER BY xg DESC NULLS LAST
        LIMIT 50
    """
    return query_df(sql, params)


def get_shots_by_source(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    sql = f"""
        SELECT fs.data_source,
               COUNT(*) AS shots,
               SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
               ROUND(SUM(fs.xg)::numeric, 2) AS total_xg
        FROM fact_shots fs
        JOIN dim_match m ON fs.match_id = m.match_id
        {comp_join}
        WHERE m.season = :season {comp_filter}
    """
    if tid is not None:
        sql += " AND fs.team_id = :tid"
        params["tid"] = tid
    sql += " GROUP BY fs.data_source ORDER BY shots DESC"
    return query_df(sql, params)


def get_injuries(season_label: str, team: str | None) -> pd.DataFrame:
    # fact_injuries has no competition link -- season short-format filter only
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params = {"season": _short_season(season_label)}
    sql = """
        SELECT p.canonical_name AS player,
               p.position,
               fi.injury_type,
               fi.date_from, fi.date_until,
               fi.days_absent, fi.matches_missed
        FROM fact_injuries fi
        JOIN dim_player p ON fi.player_id = p.canonical_id
        WHERE fi.season = :season
    """
    if tid is not None:
        sql += " AND fi.team_id = :tid"
        params["tid"] = tid
    sql += " ORDER BY fi.days_absent DESC NULLS LAST"
    return query_df(sql, params)


def get_events_summary(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    sql = f"""
        SELECT fe.data_source, fe.event_type, COUNT(*) AS count
        FROM fact_events fe
        JOIN dim_match m ON fe.match_id = m.match_id
        {comp_join}
        WHERE m.season = :season {comp_filter}
          AND fe.event_type IS NOT NULL
    """
    if tid is not None:
        sql += " AND fe.team_id = :tid"
        params["tid"] = tid
    sql += " GROUP BY fe.data_source, fe.event_type ORDER BY count DESC LIMIT 100"
    return query_df(sql, params)


def get_team_standings(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params: dict = {"season": season_label}
    outer_filter = ""
    if tid is not None:
        outer_filter = "AND c.team_id = :tid"
        params["tid"] = tid

    # Resolve competition_id once in a CTE to avoid repeating the JOIN in every CTE branch.
    comp_cte = ""
    comp_match_filter = ""
    if competition is not None:
        comp_cte = """
        comp_id AS (
            SELECT canonical_id FROM dim_competition WHERE canonical_name = :competition
        ),"""
        comp_match_filter = "AND m.competition_id = (SELECT canonical_id FROM comp_id)"
        params["competition"] = competition

    sql = f"""
        WITH {comp_cte}
        home_stats AS (
            SELECT m.home_team_id AS team_id,
                   COUNT(*) AS played,
                   SUM(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) AS won,
                   SUM(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) AS drawn,
                   SUM(CASE WHEN m.home_score < m.away_score THEN 1 ELSE 0 END) AS lost,
                   SUM(COALESCE(m.home_score, 0)) AS gf,
                   SUM(COALESCE(m.away_score, 0)) AS ga
            FROM dim_match m
            WHERE m.season = :season {comp_match_filter}
              AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
            GROUP BY m.home_team_id
        ),
        away_stats AS (
            SELECT m.away_team_id AS team_id,
                   COUNT(*) AS played,
                   SUM(CASE WHEN m.away_score > m.home_score THEN 1 ELSE 0 END) AS won,
                   SUM(CASE WHEN m.home_score = m.away_score THEN 1 ELSE 0 END) AS drawn,
                   SUM(CASE WHEN m.away_score < m.home_score THEN 1 ELSE 0 END) AS lost,
                   SUM(COALESCE(m.away_score, 0)) AS gf,
                   SUM(COALESCE(m.home_score, 0)) AS ga
            FROM dim_match m
            WHERE m.season = :season {comp_match_filter}
              AND m.home_score IS NOT NULL AND m.away_score IS NOT NULL
            GROUP BY m.away_team_id
        ),
        combined AS (
            SELECT team_id,
                   SUM(played) AS p, SUM(won) AS w, SUM(drawn) AS d, SUM(lost) AS l,
                   SUM(gf) AS gf, SUM(ga) AS ga
            FROM (SELECT * FROM home_stats UNION ALL SELECT * FROM away_stats) x
            GROUP BY team_id
        ),
        xg_for AS (
            SELECT fs.team_id,
                   ROUND(SUM(fs.xg)::numeric, 2) AS xg_for,
                   COUNT(fs.shot_id) AS shots_for
            FROM fact_shots fs
            JOIN dim_match m ON fs.match_id = m.match_id
            WHERE m.season = :season {comp_match_filter}
            GROUP BY fs.team_id
        ),
        xg_against AS (
            SELECT mt.team_id,
                   ROUND(SUM(fs.xg)::numeric, 2) AS xg_against,
                   COUNT(fs.shot_id) AS shots_against
            FROM (
                SELECT m.home_team_id AS team_id, m.match_id
                FROM dim_match m
                WHERE m.season = :season {comp_match_filter}
                UNION ALL
                SELECT m.away_team_id AS team_id, m.match_id
                FROM dim_match m
                WHERE m.season = :season {comp_match_filter}
            ) mt
            JOIN fact_shots fs ON fs.match_id = mt.match_id AND fs.team_id != mt.team_id
            GROUP BY mt.team_id
        )
        SELECT t.canonical_name AS team,
               c.p, c.w, c.d, c.l, c.gf, c.ga, (c.gf - c.ga) AS gd,
               COALESCE(xf.xg_for, 0) AS xg_for,
               COALESCE(xa.xg_against, 0) AS xg_against,
               COALESCE(xf.shots_for, 0) AS shots_for,
               COALESCE(xa.shots_against, 0) AS shots_against
        FROM combined c
        JOIN dim_team t ON t.canonical_id = c.team_id
        LEFT JOIN xg_for xf ON xf.team_id = c.team_id
        LEFT JOIN xg_against xa ON xa.team_id = c.team_id
        WHERE 1=1 {outer_filter}
        ORDER BY gd DESC, c.gf DESC
    """
    return query_df(sql, params)


def get_goalkeeper_stats(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params: dict = {"season": season_label}
    outer_filter = ""
    if tid is not None:
        outer_filter = "AND gtm.team_id = :tid"
        params["tid"] = tid

    comp_cte = ""
    comp_match_filter = ""
    if competition is not None:
        comp_cte = """
        comp_id AS (
            SELECT canonical_id FROM dim_competition WHERE canonical_name = :competition
        ),"""
        comp_match_filter = "AND m.competition_id = (SELECT canonical_id FROM comp_id)"
        params["competition"] = competition

    sql = f"""
        WITH {comp_cte}
        -- Step 1: Identify GKs and their primary team.
        -- Use fact_shots (opponent shots → GK's team) + fact_events as
        -- fallback to resolve which team the GK plays for.
        gk_players AS (
            -- A GK is identified two ways and we UNION both:
            --   (a) dim_player.position (Transfermarkt) = goalkeeper, BUT this
            --       canonical id is often NOT the one the WhoScored events use,
            --       so on its own it resolves no team and the GK vanishes.
            --   (b) anyone who lined up as 'GK' in fact_player_match_stats
            --       (WhoScored) — same canonical id as fact_events, so the
            --       downstream team-resolution join actually matches.
            SELECT canonical_id AS player_id, canonical_name AS goalkeeper
            FROM dim_player
            WHERE LOWER(position) IN ('portero', 'goalkeeper', 'gk', 'keeper')
               OR canonical_id IN (
                   SELECT player_id
                   FROM fact_player_match_stats
                   WHERE UPPER(position) = 'GK'
               )
        ),
        -- Team assignment: prefer the team that appears most often as
        -- the GK's team across fact_events OR as the opposing team in
        -- fact_shots (the team that was NOT shooting).
        gk_team_raw AS (
            -- from events (subs, cards, etc.)
            SELECT gk.player_id, gk.goalkeeper, fe.team_id, COUNT(*) AS cnt
            FROM gk_players gk
            JOIN fact_events fe ON fe.player_id = gk.player_id
            JOIN dim_match m ON fe.match_id = m.match_id
            WHERE m.season = :season {comp_match_filter}
            GROUP BY gk.player_id, gk.goalkeeper, fe.team_id
          UNION ALL
            -- from fact_shots: GK's team is the one being shot AT,
            -- i.e. the team that is NOT fs.team_id
            SELECT gk.player_id, gk.goalkeeper,
                   CASE WHEN m.home_team_id = fs.team_id
                        THEN m.away_team_id
                        ELSE m.home_team_id END AS team_id,
                   COUNT(*) AS cnt
            FROM gk_players gk
            JOIN fact_shots fs ON fs.player_id = gk.player_id
            JOIN dim_match m ON fs.match_id = m.match_id
            WHERE m.season = :season {comp_match_filter}
            GROUP BY gk.player_id, gk.goalkeeper,
                     CASE WHEN m.home_team_id = fs.team_id
                          THEN m.away_team_id
                          ELSE m.home_team_id END
        ),
        gk_team_agg AS (
            SELECT player_id, goalkeeper, team_id, SUM(cnt) AS total_cnt
            FROM gk_team_raw
            GROUP BY player_id, goalkeeper, team_id
        ),
        gk_team_map AS (
            SELECT DISTINCT ON (player_id) player_id, goalkeeper, team_id
            FROM gk_team_agg
            ORDER BY player_id, total_cnt DESC
        ),
        -- Step 2: matches_played = all team matches in the season
        -- (not just matches where the GK had an event).
        -- This is an approximation — assumes the GK played every match.
        -- Without a lineup table, it's the best we can do.
        team_matches AS (
            SELECT gtm.player_id, m.match_id
            FROM gk_team_map gtm
            JOIN dim_match m ON (m.home_team_id = gtm.team_id
                                 OR m.away_team_id = gtm.team_id)
            WHERE m.season = :season {comp_match_filter}
        ),
        matches_played AS (
            SELECT player_id, COUNT(DISTINCT match_id) AS matches
            FROM team_matches
            GROUP BY player_id
        ),
        -- Step 3: shots faced = on-target shots by the opponent
        shots_faced AS (
            SELECT tm.player_id,
                   COUNT(fs.shot_id) AS shots_faced,
                   SUM(CASE WHEN LOWER(fs.result) = 'goal' THEN 1 ELSE 0 END) AS goals_allowed,
                   ROUND(SUM(fs.xg)::numeric, 2) AS xg_conceded
            FROM team_matches tm
            JOIN gk_team_map gtm ON gtm.player_id = tm.player_id
            JOIN fact_shots fs ON fs.match_id = tm.match_id
                               AND fs.team_id != gtm.team_id
                               AND LOWER(fs.result) IN ('goal', 'saved', 'save', 'savedshot')
            GROUP BY tm.player_id
        ),
        clean_sheets AS (
            SELECT tm.player_id, COUNT(*) AS clean_sheets
            FROM team_matches tm
            JOIN gk_team_map gtm ON gtm.player_id = tm.player_id
            JOIN dim_match m ON m.match_id = tm.match_id
            WHERE (m.home_team_id = gtm.team_id AND COALESCE(m.away_score, 1) = 0)
               OR (m.away_team_id = gtm.team_id AND COALESCE(m.home_score, 1) = 0)
            GROUP BY tm.player_id
        )
        SELECT gtm.goalkeeper,
               t.canonical_name AS team,
               COALESCE(mp.matches, 0) AS matches_played,
               COALESCE(sf.goals_allowed, 0) AS goals_allowed,
               COALESCE(sf.shots_faced, 0) AS shots_faced,
               COALESCE(sf.shots_faced - sf.goals_allowed, 0) AS saves,
               ROUND(
                   COALESCE(sf.shots_faced - sf.goals_allowed, 0)::numeric
                   / NULLIF(sf.shots_faced, 0) * 100,
                   1
               ) AS save_pct,
               COALESCE(sf.xg_conceded, 0) AS xg_conceded,
               ROUND(
                   (COALESCE(sf.xg_conceded, 0)
                    - COALESCE(sf.goals_allowed, 0))::numeric,
                   2
               ) AS goals_saved_above_expected,
               COALESCE(cs.clean_sheets, 0) AS clean_sheets
        FROM gk_team_map gtm
        JOIN dim_team t ON t.canonical_id = gtm.team_id
        LEFT JOIN matches_played mp ON mp.player_id = gtm.player_id
        LEFT JOIN shots_faced sf ON sf.player_id = gtm.player_id
        LEFT JOIN clean_sheets cs ON cs.player_id = gtm.player_id
        WHERE 1=1 {outer_filter}
        ORDER BY goals_saved_above_expected DESC NULLS LAST
    """
    return query_df(sql, params)


def get_player_discipline(
    season_label: str | None,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params: dict = {}
    season_filter = ""
    shot_team_filter = ""
    event_team_filter = ""
    comp_join, comp_filter = _comp_clause(competition)
    if season_label is not None:
        season_filter = "AND m.season = :season"
        params["season"] = season_label
    if tid is not None:
        shot_team_filter = "AND fs.team_id = :tid"
        event_team_filter = "AND fe.team_id = :tid"
        params["tid"] = tid
    if competition is not None:
        params["competition"] = competition
    sql = f"""
        WITH shot_stats AS (
            SELECT fs.player_id,
                   fs.team_id,
                   m.season,
                   COUNT(fs.shot_id) AS shots,
                   SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals,
                   ROUND(SUM(fs.xg)::numeric, 2) AS xg
            FROM fact_shots fs
            JOIN dim_match m ON fs.match_id = m.match_id
            {comp_join}
            WHERE 1=1 {season_filter} {shot_team_filter} {comp_filter}
            GROUP BY fs.player_id, fs.team_id, m.season
        ),
        -- A player can get at most 1 yellow + 1 red per match.
        -- Collapse per (match, player) so multi-source data doesn't
        -- inflate counts even when minute values differ across sources.
        match_player_cards AS (
            SELECT fe.match_id, fe.player_id, fe.team_id,
                   MAX(CASE WHEN {_sql_is_yellow_card('fe')} THEN 1 ELSE 0 END) AS got_yellow,
                   MAX(CASE WHEN {_sql_is_red_card('fe')} THEN 1 ELSE 0 END) AS got_red
            FROM fact_events fe
            WHERE fe.event_type IS NOT NULL
            GROUP BY fe.match_id, fe.player_id, fe.team_id
        ),
        card_stats AS (
            SELECT mpc.player_id,
                   m.season,
                   SUM(mpc.got_yellow) AS yellow_cards,
                   SUM(mpc.got_red) AS red_cards
            FROM match_player_cards mpc
            JOIN dim_match m ON mpc.match_id = m.match_id
            {comp_join}
            WHERE 1=1
              {season_filter} {event_team_filter.replace('fe.', 'mpc.')} {comp_filter}
            GROUP BY mpc.player_id, m.season
        )
        SELECT p.canonical_name AS player,
               t.canonical_name AS team,
               ss.season,
               p.position,
               COALESCE(ss.goals, 0) AS goals,
               COALESCE(ss.xg, 0) AS xg,
               COALESCE(ss.shots, 0) AS shots,
               ROUND(
                   COALESCE(ss.xg, 0)::numeric / NULLIF(ss.shots, 0),
                   3
               ) AS xg_per_shot,
               ROUND((COALESCE(ss.goals, 0) - COALESCE(ss.xg, 0))::numeric, 2) AS goals_minus_xg,
               COALESCE(cs.yellow_cards, 0) AS yellow_cards,
               COALESCE(cs.red_cards, 0) AS red_cards,
               CAST(NULL AS INTEGER) AS minutes_played
        FROM shot_stats ss
        JOIN dim_player p ON p.canonical_id = ss.player_id
        JOIN dim_team t ON t.canonical_id = ss.team_id
        LEFT JOIN card_stats cs ON cs.player_id = ss.player_id AND cs.season = ss.season
        ORDER BY goals DESC NULLS LAST
    """
    return query_df(sql, params)


def get_injuries_standalone(season_label: str | None, team: str | None) -> pd.DataFrame:
    # fact_injuries has no competition link
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params: dict = {}
    season_filter = ""
    team_filter = ""
    if season_label is not None:
        season_filter = "AND fi.season = :season"
        params["season"] = _short_season(season_label)
    if tid is not None:
        season_join_filter = "AND m.season = :full_season" if season_label is not None else ""
        team_filter = f"""
            AND fi.player_id IN (
                SELECT fe.player_id FROM fact_events fe
                JOIN dim_match m ON m.match_id = fe.match_id
                WHERE fe.team_id = :tid
                  {season_join_filter}
            )
        """
        params["tid"] = tid
        if season_label is not None:
            params["full_season"] = season_label
    sql = f"""
        SELECT p.canonical_name AS player,
               fi.season,
               fi.injury_type,
               fi.date_from,
               fi.date_until,
               fi.days_absent,
               fi.matches_missed
        FROM fact_injuries fi
        JOIN dim_player p ON fi.player_id = p.canonical_id
        WHERE 1=1 {season_filter} {team_filter}
        ORDER BY fi.days_absent DESC NULLS FIRST, fi.date_from DESC
    """
    return query_df(sql, params)


def get_injury_type_breakdown(season_label: str | None, team: str | None) -> pd.DataFrame:
    # fact_injuries has no competition link
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    params: dict = {}
    season_filter = ""
    team_filter = ""
    if season_label is not None:
        season_filter = "AND fi.season = :season"
        params["season"] = _short_season(season_label)
    if tid is not None:
        season_join_filter = "AND m.season = :full_season" if season_label is not None else ""
        team_filter = f"""
            AND fi.player_id IN (
                SELECT fe.player_id FROM fact_events fe
                JOIN dim_match m ON m.match_id = fe.match_id
                WHERE fe.team_id = :tid
                  {season_join_filter}
            )
        """
        params["tid"] = tid
        if season_label is not None:
            params["full_season"] = season_label
    sql = f"""
        SELECT fi.injury_type,
               COUNT(*) AS count
        FROM fact_injuries fi
        WHERE fi.injury_type IS NOT NULL
          {season_filter}
          {team_filter}
        GROUP BY fi.injury_type
        ORDER BY count DESC
    """
    return query_df(sql, params)


def get_players_for_season(
    season_label: str,
    team_id: int | None,
    competition: str | None = None,
) -> list[tuple[str, int]]:
    """Return (canonical_name, canonical_id) for players with >= 1 goal in the season."""
    eng = get_engine()
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    team_filter = ""
    if team_id is not None:
        team_filter = "AND fs.team_id = :tid"
        params["tid"] = team_id
    sql = f"""
        SELECT DISTINCT p.canonical_name, p.canonical_id
        FROM fact_shots fs
        JOIN dim_player p ON fs.player_id = p.canonical_id
        JOIN dim_match  m ON fs.match_id  = m.match_id
        {comp_join}
        WHERE fs.result = 'Goal'
          AND m.season  = :season
          {comp_filter}
          {team_filter}
        ORDER BY p.canonical_name
    """
    with eng.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_weather_by_match(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Return match-level weather data for matches that have temperature info."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    stadium_join = _match_stadium_join()
    stadium_cols = ""
    if stadium_join:
        stadium_cols = (
            f"{_match_stadium_name_expr()} AS stadium,"
            f" ds.city AS stadium_city, m.venue_name"
        )
    else:
        stadium_cols = "m.venue_name"
    sql = f"""
        SELECT m.match_date,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score,
               m.temperature_c, m.humidity_pct,
               m.precipitation_mm, m.wind_speed_kmh,
               m.weather_code,
               {stadium_cols}
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
          AND m.temperature_c IS NOT NULL
          AND m.temperature_c BETWEEN -60 AND 60
    """
    if tid is not None:
        sql += " AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    sql += " ORDER BY m.match_date"
    return query_df(sql, params)


def get_weather_summary(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> dict:
    """Aggregate weather metrics: avg temp, matches with rain, avg humidity."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    team_filter = ""
    if tid is not None:
        team_filter = "AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    sql = f"""
        SELECT
            COUNT(*) AS matches_with_weather,
            ROUND(AVG(m.temperature_c)::numeric, 1) AS avg_temp,
            ROUND(MIN(m.temperature_c)::numeric, 1) AS min_temp,
            ROUND(MAX(m.temperature_c)::numeric, 1) AS max_temp,
            ROUND(AVG(m.humidity_pct)::numeric, 0) AS avg_humidity,
            ROUND(AVG(m.wind_speed_kmh)::numeric, 1) AS avg_wind,
            SUM(CASE WHEN m.precipitation_mm > 0 THEN 1 ELSE 0 END) AS rainy_matches
        FROM dim_match m
        {comp_join}
        WHERE m.season = :season {comp_filter}
          AND m.temperature_c IS NOT NULL
          AND m.temperature_c BETWEEN -60 AND 60
          {team_filter}
    """
    row = query_df(sql, params)
    if row.empty:
        return {"matches_with_weather": 0, "avg_temp": 0, "min_temp": 0,
                "max_temp": 0, "avg_humidity": 0, "avg_wind": 0, "rainy_matches": 0}
    r = row.iloc[0]
    return {k: (float(r[k]) if r[k] is not None else 0) for k in r.index}


def get_attendance_by_match(
    season_label: str,
    team: str | None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Return match-level attendance data with stadium capacity and fill %."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition

    stadium_join = _match_stadium_join()
    capacity_col = "NULL::integer"
    stadium_name_col = "m.venue_name AS stadium"
    if stadium_join:
        capacity_col = "ds.capacity"
        stadium_name_col = f"{_match_stadium_name_expr()} AS stadium"

    sql = f"""
        SELECT m.match_date,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score,
               m.attendance,
               m.venue_name,
               {stadium_name_col},
               {capacity_col} AS capacity
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
          AND m.attendance IS NOT NULL AND m.attendance > 0
    """
    if tid is not None:
        sql += " AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    sql += " ORDER BY m.attendance DESC"
    df = query_df(sql, params)
    if not df.empty and "capacity" in df.columns:
        att = pd.to_numeric(df["attendance"], errors="coerce")
        cap = pd.to_numeric(df["capacity"], errors="coerce")
        df["fill_pct"] = (att / cap * 100).round(1)
    return df


def get_attendance_by_team(
    season_label: str,
    competition: str | None = None,
) -> pd.DataFrame:
    """Average home attendance per team with optional capacity utilization."""
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition

    stadium_join = _match_stadium_join()
    capacity_col = "NULL::integer AS capacity"
    if stadium_join:
        capacity_col = "MAX(ds.capacity) AS capacity"

    sql = f"""
        SELECT ht.canonical_name AS team,
               COUNT(*) AS home_matches,
               ROUND(AVG(m.attendance)) AS avg_attendance,
               MAX(m.attendance) AS max_attendance,
               MIN(m.attendance) AS min_attendance,
               SUM(m.attendance) AS total_attendance,
               {capacity_col}
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
          AND m.attendance IS NOT NULL AND m.attendance > 0
        GROUP BY ht.canonical_name
        ORDER BY avg_attendance DESC
    """
    df = query_df(sql, params)
    if not df.empty and "capacity" in df.columns:
        cap = pd.to_numeric(df["capacity"], errors="coerce")
        avg = pd.to_numeric(df["avg_attendance"], errors="coerce")
        df["fill_pct"] = (avg / cap * 100).round(1)
    return df


def get_attendance_trend(
    season_label: str,
    team: str | None = None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Attendance per match date (for trend line chart)."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    team_filter = ""
    if tid is not None:
        team_filter = "AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    stadium_join = _match_stadium_join()
    stadium_name_col = "m.venue_name AS stadium"
    if stadium_join:
        stadium_name_col = f"{_match_stadium_name_expr()} AS stadium"
    sql = f"""
        SELECT m.match_date,
               m.attendance,
               m.venue_name,
               {stadium_name_col},
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
          AND m.attendance IS NOT NULL AND m.attendance > 0
          {team_filter}
        ORDER BY m.match_date
    """
    return query_df(sql, params)


def get_referee_stats(
    season_label: str | None,
    competition: str | None = None,
    team: str | None = None,
) -> pd.DataFrame:
    """Referee match counts and card stats for a season.

    When *team* is provided the card counts are restricted to cards
    received **by that team only**, so you can see which referees book
    a specific team the most.  Match counts still reflect all matches
    the referee officiated (with the team filter applied to the match
    selection).
    """
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)

    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {}
    season_filter = ""
    team_match_filter = ""
    card_team_filter = ""

    if season_label is not None:
        season_filter = "AND m.season = :season"
        params["season"] = season_label
    if competition:
        params["competition"] = competition
    if tid is not None:
        team_match_filter = "AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        card_team_filter = "AND fe.team_id = :tid"
        params["tid"] = tid

    sql = f"""
        WITH ref_matches AS (
            SELECT r.canonical_name AS referee,
                   m.match_id,
                   m.home_score,
                   m.away_score
            FROM dim_match m
            JOIN dim_referee r ON r.referee_id = m.referee_id
            {comp_join}
            WHERE m.referee_id IS NOT NULL
              {season_filter} {comp_filter}
              {team_match_filter}
        ),
        -- Collapse possible multi-source duplicates per (match, player).
        -- A second yellow is a yellow-card event plus a dismissal, not an
        -- independent direct red card.
        match_player_cards AS (
            SELECT fe.match_id, fe.player_id, fe.team_id,
                   MAX(CASE WHEN {_sql_is_yellow_card('fe')} THEN 1 ELSE 0 END) AS got_yellow,
                   MAX(CASE WHEN {_sql_is_second_yellow_card('fe')} THEN 1 ELSE 0 END) AS got_second_yellow,
                   MAX(CASE WHEN {_sql_is_direct_red_card('fe')} THEN 1 ELSE 0 END) AS got_red
            FROM fact_events fe
            WHERE fe.event_type IS NOT NULL
            GROUP BY fe.match_id, fe.player_id, fe.team_id
        ),
        ref_cards AS (
            SELECT rm.referee,
                   SUM(mpc.got_yellow + mpc.got_second_yellow) AS yellow_cards,
                   SUM(mpc.got_red) AS red_cards,
                   SUM(mpc.got_second_yellow) AS second_yellow_reds
            FROM ref_matches rm
            JOIN match_player_cards mpc ON mpc.match_id = rm.match_id
            WHERE 1=1
              {card_team_filter.replace('fe.', 'mpc.')}
            GROUP BY rm.referee
        )
        SELECT rm.referee,
               COUNT(DISTINCT rm.match_id) AS matches_officiated,
               COALESCE(MAX(rc.yellow_cards), 0) AS yellow_cards,
               COALESCE(MAX(rc.red_cards), 0) AS red_cards,
               COALESCE(MAX(rc.second_yellow_reds), 0) AS second_yellow_reds,
               COALESCE(MAX(rc.yellow_cards), 0) + COALESCE(MAX(rc.red_cards), 0) AS total_cards,
               ROUND(
                   (COALESCE(MAX(rc.yellow_cards), 0) + COALESCE(MAX(rc.red_cards), 0))::numeric
                   / NULLIF(COUNT(DISTINCT rm.match_id), 0),
                   2
               ) AS cards_per_match
        FROM ref_matches rm
        LEFT JOIN ref_cards rc ON rc.referee = rm.referee
        GROUP BY rm.referee
        ORDER BY matches_officiated DESC
    """
    try:
        return query_df(sql, params)
    except Exception:
        return pd.DataFrame()


def get_manager_stats(
    season_label: str | None,
    competition: str | None = None,
    team: str | None = None,
    min_matches: int = 3,
) -> pd.DataFrame:
    """Manager win/draw/loss record from dim_match text columns.

    When *team* is provided, only rows where the manager coached that
    specific team are included, so you see a per-team breakdown.

    *min_matches* drops managers with too few games (interim / one-off
    spells) so the points_pct ranking isn't dominated by 1-2 match samples.
    """
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)

    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"min_matches": int(min_matches)}
    season_filter = ""
    if season_label is not None:
        season_filter = "AND m.season = :season"
        params["season"] = season_label
    if competition:
        params["competition"] = competition

    # Team filter: restrict to matches where the manager coached *this* team
    home_team_filter = ""
    away_team_filter = ""
    if tid is not None:
        home_team_filter = "AND m.home_team_id = :tid"
        away_team_filter = "AND m.away_team_id = :tid"
        params["tid"] = tid

    sql = f"""
        WITH manager_matches AS (
            SELECT m.manager_home AS manager,
                   ht.canonical_name AS team,
                   m.home_score AS scored,
                   m.away_score AS conceded,
                   m.match_id,
                   m.match_date
            FROM dim_match m
            {comp_join}
            LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
            WHERE m.manager_home IS NOT NULL
              AND m.home_score IS NOT NULL
              {season_filter} {comp_filter}
              {home_team_filter}

            UNION ALL

            SELECT m.manager_away AS manager,
                   at.canonical_name AS team,
                   m.away_score AS scored,
                   m.home_score AS conceded,
                   m.match_id,
                   m.match_date
            FROM dim_match m
            {comp_join}
            LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
            WHERE m.manager_away IS NOT NULL
              AND m.away_score IS NOT NULL
              {season_filter} {comp_filter}
              {away_team_filter}
        )
        SELECT manager,
               (ARRAY_AGG(team ORDER BY match_date DESC NULLS LAST))[1] AS team,
               COUNT(*) AS matches,
               SUM(CASE WHEN scored > conceded THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN scored = conceded THEN 1 ELSE 0 END) AS draws,
               SUM(CASE WHEN scored < conceded THEN 1 ELSE 0 END) AS losses,
               SUM(scored) AS goals_for,
               SUM(conceded) AS goals_against,
               ROUND(SUM(scored)::numeric / NULLIF(COUNT(*), 0), 2) AS avg_gf,
               ROUND(SUM(conceded)::numeric / NULLIF(COUNT(*), 0), 2) AS avg_ga,
               ROUND(
                   (SUM(CASE WHEN scored > conceded THEN 3
                             WHEN scored = conceded THEN 1
                             ELSE 0 END)::numeric
                    / NULLIF(COUNT(*) * 3, 0)) * 100,
                   1
               ) AS points_pct
        FROM manager_matches
        GROUP BY manager
        HAVING COUNT(*) >= :min_matches
        ORDER BY points_pct DESC, matches DESC
    """
    return query_df(sql, params)


@st.cache_data(ttl=300)
def get_matches_for_context(
    season_label: str,
    team: str | None = None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Match list for the per-match context view (most recent first)."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    sql = f"""
        SELECT m.match_id, m.match_date,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        WHERE m.season = :season {comp_filter}
    """
    if tid is not None:
        sql += " AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    sql += " ORDER BY m.match_date DESC NULLS LAST, m.match_id DESC"
    return query_df(sql, params)


@st.cache_data(ttl=300)
def get_match_context(match_id: int) -> dict:
    """Full context for a single match: teams, score, weather, attendance,
    stadium + capacity, managers and (best-effort) referee.

    Returns a flat dict; ``fill_pct`` is added when attendance and capacity
    are both available. ``referee`` is resolved in a guarded query because the
    dim_referee migration may not be present in every database.
    """
    stadium_join = _match_stadium_join()
    stadium_name = _match_stadium_name_expr() if stadium_join else "m.venue_name"
    cap_col = "ds.capacity" if stadium_join else "NULL::integer"
    sql = f"""
        SELECT m.match_id, m.match_date, m.season,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score, m.attendance,
               m.temperature_c, m.humidity_pct,
               m.precipitation_mm, m.wind_speed_kmh, m.weather_code,
               m.manager_home, m.manager_away, m.data_source,
               m.venue_name,
               {stadium_name} AS stadium,
               {cap_col} AS capacity
        FROM dim_match m
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.match_id = :mid
    """
    df = query_df(sql, {"mid": match_id})
    if df.empty:
        return {}
    ctx = df.iloc[0].to_dict()
    att, cap = ctx.get("attendance"), ctx.get("capacity")
    ctx["fill_pct"] = None
    if att is not None and cap not in (None, 0):
        try:
            ctx["fill_pct"] = round(float(att) / float(cap) * 100, 1)
        except (TypeError, ValueError, ZeroDivisionError):
            ctx["fill_pct"] = None
    try:
        rdf = query_df("""
            SELECT r.canonical_name AS referee
            FROM dim_match m
            JOIN dim_referee r ON r.referee_id = m.referee_id
            WHERE m.match_id = :mid
        """, {"mid": match_id})
        ctx["referee"] = rdf.iloc[0]["referee"] if not rdf.empty else None
    except Exception:
        ctx["referee"] = None
    return ctx


@st.cache_data(ttl=300)
def get_match_cards(match_id: int) -> pd.DataFrame:
    """Yellow/red cards per team in one match (best-effort heuristic).

    Mirrors the detection used in get_referee_stats; ``%%`` is required because
    query_df runs through psycopg2 where a literal percent must be escaped.
    """
    sql = f"""
        SELECT t.canonical_name AS team,
               SUM(CASE WHEN {_sql_is_yellow_card()} THEN 1 ELSE 0 END) AS yellow_cards,
               SUM(CASE WHEN {_sql_is_red_card()} THEN 1 ELSE 0 END) AS red_cards
        FROM fact_events fe
        JOIN dim_team t ON t.canonical_id = fe.team_id
        WHERE fe.match_id = :mid AND fe.event_type IS NOT NULL
        GROUP BY t.canonical_name
        ORDER BY t.canonical_name
    """
    try:
        return query_df(sql, {"mid": match_id})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_whoscored_event_coverage() -> pd.DataFrame:
    """Per-season count of WhoScored events / matches (event diagnostics)."""
    return query_df("""
        SELECT m.season,
               COUNT(DISTINCT fe.match_id) AS matches,
               COUNT(*) AS events
        FROM fact_events fe
        JOIN dim_match m ON m.match_id = fe.match_id
        WHERE fe.data_source = 'whoscored'
        GROUP BY m.season
        ORDER BY m.season DESC
    """)


@st.cache_data(ttl=300)
def get_whoscored_event_types(limit: int = 60) -> pd.DataFrame:
    """Distinct WhoScored event_type values with counts (event diagnostics)."""
    return query_df("""
        SELECT fe.event_type,
               COUNT(*) AS events,
               COUNT(*) FILTER (WHERE fe.x IS NOT NULL AND fe.y IS NOT NULL) AS with_xy
        FROM fact_events fe
        WHERE fe.data_source = 'whoscored' AND fe.event_type IS NOT NULL
        GROUP BY fe.event_type
        ORDER BY events DESC
        LIMIT :lim
    """, {"lim": limit})


@st.cache_data(ttl=300)
def get_player_cards_fouls(
    season_label: str,
    team: str | None = None,
    competition: str | None = None,
    min_matches: int = 1,
) -> pd.DataFrame:
    """Per-player discipline: yellow/red cards and fouls, with per-match rates.

    Detection is heuristic on event_type/outcome (same family as the referee
    query) so it is robust to the exact WhoScored strings. ``%%`` is required
    because query_df runs through psycopg2.
    """
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label, "minm": int(min_matches)}
    if competition:
        params["competition"] = competition
    team_filter = ""
    if tid is not None:
        team_filter = "AND fe.team_id = :tid"
        params["tid"] = tid
    sql = f"""
        WITH
        -- Cards: at most 1 yellow + 1 red per (match, player)
        match_player_cards AS (
            SELECT fe.match_id, fe.player_id, fe.team_id,
                   MAX(CASE WHEN {_sql_is_yellow_card('fe')} THEN 1 ELSE 0 END) AS got_yellow,
                   MAX(CASE WHEN {_sql_is_red_card('fe')} THEN 1 ELSE 0 END) AS got_red
            FROM fact_events fe
            WHERE fe.event_type IS NOT NULL
            GROUP BY fe.match_id, fe.player_id, fe.team_id
        ),
        card_totals AS (
            SELECT mpc.player_id,
                   SUM(mpc.got_yellow) AS yellow_cards,
                   SUM(mpc.got_red) AS red_cards
            FROM match_player_cards mpc
            JOIN dim_match m ON m.match_id = mpc.match_id
            {comp_join}
            WHERE m.season = :season {comp_filter}
              {team_filter.replace('fe.', 'mpc.')}
            GROUP BY mpc.player_id
        ),
        -- Fouls + match count: minute-based dedup is fine for fouls
        dedup_events AS (
            SELECT DISTINCT ON (fe.match_id, fe.player_id, fe.minute,
                                COALESCE(fe.event_type, ''))
                   fe.match_id, fe.player_id, fe.team_id, fe.minute,
                   fe.event_type
            FROM fact_events fe
            WHERE fe.event_type IS NOT NULL
            ORDER BY fe.match_id, fe.player_id, fe.minute,
                     COALESCE(fe.event_type, ''), fe.data_source
        )
        SELECT p.canonical_name AS player,
               MAX(t.canonical_name) AS team,
               COUNT(DISTINCT de.match_id) AS matches,
               COALESCE(MAX(ct.yellow_cards), 0) AS yellow_cards,
               COALESCE(MAX(ct.red_cards), 0) AS red_cards,
               SUM(CASE WHEN de.event_type ILIKE '%%foul%%' THEN 1 ELSE 0 END) AS fouls
        FROM dedup_events de
        JOIN dim_match m ON m.match_id = de.match_id
        {comp_join}
        JOIN dim_player p ON p.canonical_id = de.player_id
        JOIN dim_team t ON t.canonical_id = de.team_id
        LEFT JOIN card_totals ct ON ct.player_id = de.player_id
        WHERE m.season = :season {comp_filter}
          {team_filter.replace('fe.', 'de.')}
        GROUP BY p.canonical_id, p.canonical_name
        HAVING COUNT(DISTINCT de.match_id) >= :minm
    """
    df = query_df(sql, params)
    if df.empty:
        return df
    for c in ("matches", "yellow_cards", "red_cards", "fouls"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    df["total_cards"] = df["yellow_cards"] + df["red_cards"]
    _m = df["matches"].replace(0, pd.NA)
    df["cards_per_match"] = (df["total_cards"] / _m).astype(float).round(2)
    df["fouls_per_match"] = (df["fouls"] / _m).astype(float).round(2)
    return df.sort_values(["total_cards", "cards_per_match"], ascending=False)


@st.cache_data(ttl=300)
def get_match_events_xy(match_id: int) -> pd.DataFrame:
    """WhoScored located events for one match (for the chalkboard).

    Coordinates are stored normalised 0-1; callers scale to the 105x68 pitch.
    """
    return query_df("""
        SELECT p.canonical_name AS player,
               t.canonical_name AS team,
               fe.team_id,
               fe.event_type, fe.outcome,
               fe.x, fe.y, fe.end_x, fe.end_y
        FROM fact_events fe
        JOIN dim_player p ON p.canonical_id = fe.player_id
        JOIN dim_team t ON t.canonical_id = fe.team_id
        WHERE fe.match_id = :mid
          AND fe.data_source = 'whoscored'
          AND fe.x IS NOT NULL AND fe.y IS NOT NULL
        ORDER BY fe.minute, fe.second, fe.event_id
    """, {"mid": match_id})


def get_weather_by_venue(
    season_label: str,
    team: str | None = None,
    competition: str | None = None,
) -> pd.DataFrame:
    """Avg/min/max temperature grouped by venue (stadium).

    Useful for comparing climate conditions across stadiums.
    """
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    comp_join, comp_filter = _comp_clause(competition)
    params: dict = {"season": season_label}
    if competition:
        params["competition"] = competition
    team_filter = ""
    if tid is not None:
        team_filter = "AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    stadium_join = _match_stadium_join()
    venue_expr = (
        _match_stadium_name_expr()
        if stadium_join
        else "COALESCE(m.venue_name, 'Unknown')"
    )
    group_key = (
        f"COALESCE(ds.stadium_id::text, m.venue_name, 'unknown')"
        if stadium_join
        else "COALESCE(m.venue_name, 'unknown')"
    )
    sql = f"""
        SELECT MAX({venue_expr}) AS venue,
               MODE() WITHIN GROUP (ORDER BY ht.canonical_name) AS home_team,
               COUNT(*) AS matches,
               ROUND(AVG(m.temperature_c)::numeric, 1) AS avg_temp,
               ROUND(MIN(m.temperature_c)::numeric, 1) AS min_temp,
               ROUND(MAX(m.temperature_c)::numeric, 1) AS max_temp,
               ROUND(AVG(m.humidity_pct)::numeric, 0) AS avg_humidity,
               SUM(CASE WHEN m.precipitation_mm > 0 THEN 1 ELSE 0 END) AS rainy
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        {stadium_join}
        WHERE m.season = :season {comp_filter}
          AND m.temperature_c IS NOT NULL
          AND m.temperature_c BETWEEN -60 AND 60
          {team_filter}
        GROUP BY {group_key}
        HAVING COUNT(*) >= 1
        ORDER BY avg_temp DESC
    """
    return query_df(sql, params)


def get_venues_list(team: str | None = None) -> list[str]:
    """Return distinct match-stadium names, optionally filtered by team."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    team_filter = ""
    params: dict = {}
    if tid is not None:
        team_filter = "AND (m.home_team_id = :tid OR m.away_team_id = :tid)"
        params["tid"] = tid
    stadium_join = _match_stadium_join()
    venue_expr = (
        _match_stadium_name_expr()
        if stadium_join
        else "m.venue_name"
    )
    sql = f"""
        SELECT DISTINCT {venue_expr} AS venue
        FROM dim_match m
        {stadium_join}
        WHERE m.temperature_c IS NOT NULL
          AND {venue_expr} IS NOT NULL
          AND {venue_expr} <> 'Unknown'
          {team_filter}
        ORDER BY venue
    """
    df = query_df(sql, params)
    return df["venue"].tolist() if not df.empty else []


def get_weather_venue_across_seasons(
    venue: str | None = None,
    team: str | None = None,
) -> pd.DataFrame:
    """Temperature stats per season for a specific venue and/or team.

    Returns one row per season with avg/min/max temp, matches, humidity.
    """
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    filters = []
    params: dict = {}
    stadium_join = _match_stadium_join()
    venue_expr = (
        _match_stadium_name_expr()
        if stadium_join
        else "m.venue_name"
    )
    if venue:
        filters.append(f"AND {venue_expr} = :venue")
        params["venue"] = venue
    if tid is not None:
        filters.append("AND (m.home_team_id = :tid OR m.away_team_id = :tid)")
        params["tid"] = tid
    where_extra = " ".join(filters)
    sql = f"""
        SELECT m.season,
               COUNT(*) AS matches,
               ROUND(AVG(m.temperature_c)::numeric, 1) AS avg_temp,
               ROUND(MIN(m.temperature_c)::numeric, 1) AS min_temp,
               ROUND(MAX(m.temperature_c)::numeric, 1) AS max_temp,
               ROUND(AVG(m.humidity_pct)::numeric, 0) AS avg_humidity,
               SUM(CASE WHEN m.precipitation_mm > 0 THEN 1 ELSE 0 END) AS rainy
        FROM dim_match m
        {stadium_join}
        WHERE m.temperature_c IS NOT NULL
          AND m.temperature_c BETWEEN -60 AND 60
          {where_extra}
        GROUP BY m.season
        ORDER BY m.season
    """
    return query_df(sql, params)


def get_weather_matches_for_venue(
    venue: str | None = None,
    team: str | None = None,
) -> pd.DataFrame:
    """Match-level temperature data across all seasons for a venue/team."""
    eng = get_engine()
    with eng.connect() as conn:
        tid = _team_id(conn, team)
    filters = []
    params: dict = {}
    stadium_join = _match_stadium_join()
    venue_expr = (
        _match_stadium_name_expr()
        if stadium_join
        else "m.venue_name"
    )
    stadium_cols = f"{venue_expr} AS stadium, m.venue_name"
    if venue:
        filters.append(f"AND {venue_expr} = :venue")
        params["venue"] = venue
    if tid is not None:
        filters.append("AND (m.home_team_id = :tid OR m.away_team_id = :tid)")
        params["tid"] = tid
    where_extra = " ".join(filters)
    sql = f"""
        SELECT m.match_date, m.season,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score,
               m.temperature_c, m.humidity_pct,
               {stadium_cols}
        FROM dim_match m
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
        {stadium_join}
        WHERE m.temperature_c IS NOT NULL
          AND m.temperature_c BETWEEN -60 AND 60
          {where_extra}
        ORDER BY m.match_date
    """
    return query_df(sql, params)


def get_injury_season_trend(team: str | None) -> pd.DataFrame:
    sql = """
        SELECT fi.season,
               COUNT(*) AS injuries,
               COALESCE(SUM(fi.days_absent), 0) AS days_absent,
               COALESCE(SUM(fi.matches_missed), 0) AS matches_missed
        FROM fact_injuries fi
        WHERE fi.season IS NOT NULL
        GROUP BY fi.season
        ORDER BY fi.season DESC
    """
    return query_df(sql, {})


# ================================================================
# STADIUMS -- dim_stadium (Transfermarkt, modelo SCD2)
# ================================================================
# Granularidad: una fila por estado del estadio, con rango
# [valid_from_season, valid_to_season]. Si la informacion no cambia
# entre temporadas, hay una sola fila que cubre el rango entero.

def _stadium_table_exists() -> bool:
    """True si la tabla dim_stadium existe (evita crash si aun no migrada)."""
    eng = get_engine()
    try:
        with eng.connect() as conn:
            row = conn.execute(text(
                "SELECT to_regclass('public.dim_stadium')"
            )).fetchone()
        return row is not None and row[0] is not None
    except Exception:
        return False


_stadium_master_table: bool | None = None


def _stadium_master_table_exists() -> bool:
    """True si dim_stadium_master existe (imágenes Cloudinary / sede canónica)."""
    global _stadium_master_table
    if _stadium_master_table is not None:
        return _stadium_master_table
    eng = get_engine()
    try:
        with eng.connect() as conn:
            row = conn.execute(text(
                "SELECT to_regclass('public.dim_stadium_master')"
            )).fetchone()
        _stadium_master_table = row is not None and row[0] is not None
    except Exception:
        _stadium_master_table = False
    return _stadium_master_table


def _stadium_image_select_sql() -> tuple[str, str, str, str, str]:
    """JOIN + expresiones COALESCE (imagen, master_id, coordenadas)."""
    if not _stadium_master_table_exists():
        return "", "s.image_url", "NULL::integer", "s.latitude", "s.longitude"
    join = """
        LEFT JOIN dim_stadium_master sm_q
            ON s.wikidata_qid IS NOT NULL
           AND s.wikidata_qid <> ''
           AND sm_q.wikidata_qid = s.wikidata_qid
        LEFT JOIN dim_stadium_master sm_home
            ON sm_home.stadium_id = t.home_stadium_master_id
    """
    image_expr = "COALESCE(sm_q.image_url, sm_home.image_url, s.image_url)"
    master_expr = "COALESCE(sm_q.stadium_id, sm_home.stadium_id)"
    lat_expr = "COALESCE(sm_q.latitude, sm_home.latitude, s.latitude)"
    lon_expr = "COALESCE(sm_q.longitude, sm_home.longitude, s.longitude)"
    return join, image_expr, master_expr, lat_expr, lon_expr


_names_history_table: bool | None = None


def _stadium_names_history_exists() -> bool:
    global _names_history_table
    if _names_history_table is not None:
        return _names_history_table
    eng = get_engine()
    try:
        with eng.connect() as conn:
            row = conn.execute(text(
                "SELECT to_regclass('public.dim_stadium_names_history')"
            )).fetchone()
        _names_history_table = row is not None and row[0] is not None
    except Exception:
        _names_history_table = False
    return _names_history_table


_match_stadium_col: bool | None = None


def _match_stadium_column_exists() -> bool:
    """True si dim_match tiene match_stadium_id (migracion aplicada)."""
    global _match_stadium_col
    if _match_stadium_col is not None:
        return _match_stadium_col
    eng = get_engine()
    try:
        with eng.connect() as conn:
            row = conn.execute(text("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'dim_match'
                  AND column_name = 'match_stadium_id'
                LIMIT 1
            """)).fetchone()
        _match_stadium_col = row is not None
    except Exception:
        _match_stadium_col = False
    return _match_stadium_col


def _match_stadium_id_expr(match_alias: str = "m") -> str:
    return f"{match_alias}.match_stadium_id"


def _match_stadium_join(
    match_alias: str = "m",
    stadium_alias: str = "ds",
) -> str:
    """LEFT JOIN dim_stadium por estadio real del partido (o vacio).

    Vacio tambien si la columna match_stadium_id aun no existe (dump previo
    a la migracion add_match_stadium_id.sql), para no romper la consulta.
    """
    if not _stadium_table_exists() or not _match_stadium_column_exists():
        return ""
    sid = _match_stadium_id_expr(match_alias)
    return (
        f"LEFT JOIN dim_stadium {stadium_alias} "
        f"ON {stadium_alias}.stadium_id = {sid}"
    )


def _match_stadium_name_expr(
    stadium_alias: str = "ds",
    match_alias: str = "m",
) -> str:
    return (
        f"COALESCE({stadium_alias}.stadium_name, "
        f"{match_alias}.venue_name, 'Unknown')"
    )


def get_stadium_seasons() -> list[str]:
    """Temporadas con datos en dim_stadium (descendente).

    SCD2: cada fila cubre un RANGO [valid_from_season, valid_to_season].
    Enumeramos todas las temporadas cubiertas por algun rango.
    """
    if not _stadium_table_exists():
        return []
    sql = """
        WITH years AS (
            SELECT CAST(SPLIT_PART(valid_from_season, '/', 1) AS INTEGER) AS yfrom,
                   CAST(SPLIT_PART(valid_to_season,   '/', 1) AS INTEGER) AS yto
            FROM dim_stadium
            WHERE valid_from_season IS NOT NULL
              AND valid_to_season   IS NOT NULL
        )
        SELECT DISTINCT (y::text || '/' || (y + 1)::text) AS season
        FROM years, generate_series(yfrom, yto) AS y
        ORDER BY season DESC
    """
    eng = get_engine()
    with eng.connect() as conn:
        return [r[0] for r in conn.execute(text(sql)).fetchall()]


def get_stadium_countries() -> list[str]:
    if not _stadium_table_exists():
        return []
    sql = """
        SELECT DISTINCT country
        FROM dim_stadium
        WHERE country IS NOT NULL AND country <> ''
        ORDER BY country
    """
    eng = get_engine()
    with eng.connect() as conn:
        return [r[0] for r in conn.execute(text(sql)).fetchall()]


def get_stadiums(
    season: str | None = None,
    competition: str | None = None,
    country: str | None = None,
    search: str | None = None,
    include_match_venues: bool = False,
) -> pd.DataFrame:
    """Estadios filtrados por temporada / competicion / pais / busqueda.

    Si se filtra por temporada, se aplica filtro de rango SCD2:
    `:season BETWEEN valid_from_season AND valid_to_season`.

    Por defecto excluye filas `data_source='match-venue'` (sedes neutrales
    creadas solo para resolver el estadio del partido).
    """
    if not _stadium_table_exists():
        return pd.DataFrame()

    params: dict = {}
    where_clauses: list[str] = []
    if not include_match_venues:
        where_clauses.append(
            "(s.data_source IS NULL OR s.data_source <> 'match-venue')"
        )

    # Si filtramos por season concreta, la mostramos en la columna 'season'.
    # Si no, mostramos el rango "vfrom -> vto" o solo vfrom si son iguales.
    if season:
        season_expr = ":season AS season"
    else:
        season_expr = (
            "CASE WHEN s.valid_from_season = s.valid_to_season "
            "     THEN s.valid_from_season "
            "     ELSE s.valid_from_season || ' -> ' || s.valid_to_season "
            "END AS season"
        )

    master_join, image_expr, master_expr, lat_expr, lon_expr = _stadium_image_select_sql()

    base_sql = f"""
        SELECT
            s.stadium_id,
            COALESCE(t.canonical_name, s.team_slug) AS team,
            {season_expr},
            s.stadium_name,
            s.capacity,
            s.seats_total,
            s.built_year,
            s.owner,
            s.city,
            s.country,
            s.surface,
            s.architect,
            {lat_expr} AS latitude,
            {lon_expr} AS longitude,
            s.altitude_m,
            s.timezone,
            s.data_source,
            s.tm_url,
            {master_expr} AS master_stadium_id,
            {image_expr} AS image_url,
            s.wikipedia_url,
            s.wikidata_qid
        FROM dim_stadium s
        LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        {master_join}
    """

    if competition:
        base_sql += """
            JOIN (
                SELECT DISTINCT team_id FROM (
                    SELECT m.home_team_id AS team_id FROM dim_match m
                      JOIN dim_competition dc ON dc.canonical_id = m.competition_id
                      WHERE dc.canonical_name = :competition
                    UNION
                    SELECT m.away_team_id AS team_id FROM dim_match m
                      JOIN dim_competition dc ON dc.canonical_id = m.competition_id
                      Where dc.canonical_name = :competition
                ) x WHERE team_id IS NOT NULL
            ) ct ON ct.team_id = s.canonical_team_id
        """
        params["competition"] = competition

    if season:
        where_clauses.append(
            ":season BETWEEN s.valid_from_season AND s.valid_to_season"
        )
        params["season"] = season
    if country:
        where_clauses.append("s.country = :country")
        params["country"] = country
    if search:
        where_clauses.append(
            "(LOWER(s.stadium_name) LIKE :q OR LOWER(t.canonical_name) LIKE :q "
            "OR LOWER(s.city) LIKE :q)"
        )
        params["q"] = f"%{search.lower()}%"

    if where_clauses:
        base_sql += " WHERE " + " AND ".join(where_clauses)

    base_sql += " ORDER BY s.capacity DESC NULLS LAST, team ASC"
    return query_df(base_sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def get_stadium_name_history(master_stadium_id: int | None) -> pd.DataFrame:
    """Nombres históricos del edificio (dim_stadium_names_history)."""
    if not master_stadium_id or not _stadium_names_history_exists():
        return pd.DataFrame()
    sql = """
        SELECT stadium_name, valid_from_year, valid_to_year, is_current
        FROM dim_stadium_names_history
        WHERE stadium_id = :sid
        ORDER BY
            is_current DESC,
            valid_from_year NULLS FIRST,
            valid_to_year NULLS LAST,
            stadium_name
    """
    return query_df(sql, {"sid": int(master_stadium_id)})


def get_stadium_summary(
    season: str | None = None,
    competition: str | None = None,
    country: str | None = None,
    include_match_venues: bool = False,
) -> dict:
    """Tarjetas resumen: n estadios, aforo total, media, equipo+aforo top."""
    df = get_stadiums(
        season=season,
        competition=competition,
        country=country,
        include_match_venues=include_match_venues,
    )
    if df.empty:
        return {
            "n_stadiums": 0, "total_capacity": 0, "avg_capacity": 0,
            "max_stadium": "-", "max_capacity": 0,
        }
    caps = pd.to_numeric(df["capacity"], errors="coerce").dropna()
    if caps.empty:
        return {
            "n_stadiums": len(df), "total_capacity": 0, "avg_capacity": 0,
            "max_stadium": "-", "max_capacity": 0,
        }
    idx_max = caps.idxmax()
    return {
        "n_stadiums":     len(df),
        "total_capacity": int(caps.sum()),
        "avg_capacity":   int(caps.mean()),
        "max_stadium":    str(df.loc[idx_max, "stadium_name"] or "-"),
        "max_capacity":   int(caps.max()),
    }
