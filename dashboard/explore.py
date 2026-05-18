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
from sqlalchemy import text

from dashboard.db import get_engine, query_df


def _short_season(label: str) -> str:
    """Convert '2020/2021' → '20/21' to match fact_injuries season format."""
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


def get_competitions() -> list[str]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT canonical_name FROM dim_competition ORDER BY canonical_name"
        )).fetchall()
    return [r[0] for r in rows] or ["La Liga"]


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
    sql = f"""
        SELECT m.match_date, m.season,
               ht.canonical_name AS home_team,
               at.canonical_name AS away_team,
               m.home_score, m.away_score, m.data_source,
               m.home_team_id, m.away_team_id
        FROM dim_match m
        {comp_join}
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at ON m.away_team_id = at.canonical_id
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
    # fact_injuries has no competition link — season short-format filter only
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
        gk_team_raw AS (
            SELECT p.canonical_id AS player_id, p.canonical_name AS goalkeeper,
                   fe.team_id, COUNT(*) AS cnt
            FROM dim_player p
            JOIN fact_events fe ON fe.player_id = p.canonical_id
            JOIN dim_match m ON fe.match_id = m.match_id
            WHERE p.position IN ('Portero', 'Goalkeeper', 'GK')
              AND m.season = :season {comp_match_filter}
            GROUP BY p.canonical_id, p.canonical_name, fe.team_id
        ),
        gk_team_map AS (
            SELECT DISTINCT ON (player_id) player_id, goalkeeper, team_id
            FROM gk_team_raw
            ORDER BY player_id, cnt DESC
        ),
        gk_match_list AS (
            SELECT DISTINCT gtm.player_id, m.match_id
            FROM gk_team_map gtm
            JOIN fact_events fe ON fe.player_id = gtm.player_id
                                AND fe.team_id = gtm.team_id
            JOIN dim_match m ON fe.match_id = m.match_id
            WHERE m.season = :season {comp_match_filter}
        ),
        matches_played AS (
            SELECT player_id, COUNT(DISTINCT match_id) AS matches
            FROM gk_match_list
            GROUP BY player_id
        ),
        shots_faced AS (
            SELECT gml.player_id,
                   COUNT(fs.shot_id) AS shots_faced,
                   SUM(CASE WHEN fs.result = 'Goal' THEN 1 ELSE 0 END) AS goals_allowed,
                   ROUND(SUM(fs.xg)::numeric, 2) AS xg_conceded
            FROM gk_match_list gml
            JOIN gk_team_map gtm ON gtm.player_id = gml.player_id
            JOIN fact_shots fs ON fs.match_id = gml.match_id
                               AND fs.team_id != gtm.team_id
                               AND LOWER(fs.result) IN ('goal', 'saved', 'save', 'savedshot')
            GROUP BY gml.player_id
        ),
        clean_sheets AS (
            SELECT gml.player_id, COUNT(*) AS clean_sheets
            FROM gk_match_list gml
            JOIN gk_team_map gtm ON gtm.player_id = gml.player_id
            JOIN dim_match m ON m.match_id = gml.match_id
            WHERE (m.home_team_id = gtm.team_id AND COALESCE(m.away_score, 1) = 0)
               OR (m.away_team_id = gtm.team_id AND COALESCE(m.home_score, 1) = 0)
            GROUP BY gml.player_id
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
        card_stats AS (
            SELECT fe.player_id,
                   m.season,
                   SUM(CASE WHEN
                            (fe.event_type ILIKE '%yellow%' AND fe.event_type NOT ILIKE '%red%')
                            OR (LOWER(fe.event_type) = 'card' AND LOWER(fe.outcome) = 'yellow')
                            THEN 1 ELSE 0 END) AS yellow_cards,
                   SUM(CASE WHEN
                            (fe.event_type ILIKE '%red%')
                            OR (LOWER(fe.event_type) = 'card' AND LOWER(fe.outcome) IN ('red', 'yellowred'))
                            THEN 1 ELSE 0 END) AS red_cards
            FROM fact_events fe
            JOIN dim_match m ON fe.match_id = m.match_id
            {comp_join}
            WHERE fe.event_type IS NOT NULL
              {season_filter} {event_team_filter} {comp_filter}
            GROUP BY fe.player_id, m.season
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
