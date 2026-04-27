"""
dashboard/db.py
===============
Read-only DB queries for the Pipeline-monitoring tab.

Engine resolution:
  1. Try to import the existing engine instance from loaders.common.
  2. On ImportError, build a SQLAlchemy engine from the project-root .env.

No INSERT / UPDATE / DELETE / DDL anywhere in this module.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is not None:
        return _engine

    try:
        from loaders.common import engine as _shared_engine
        _engine = _shared_engine
        return _engine
    except ImportError:
        pass

    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, encoding="utf-8")

    required = ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    for var in required:
        if not os.getenv(var):
            raise EnvironmentError(var)

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        database=os.getenv("DB_NAME"),
    )
    _engine = create_engine(url, connect_args={"client_encoding": "utf8"})
    return _engine


def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Run a parameterised SELECT and return a DataFrame.

    Avoids `pd.read_sql(text(...))`, which is broken in pandas 2.0.3 against
    SQLAlchemy 2.x (raises 'Query must be a string unless using sqlalchemy').
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
    return pd.DataFrame(rows)


def get_db_summary() -> dict:
    eng = get_engine()
    with eng.connect() as conn:
        return {
            "players":  conn.execute(text("SELECT COUNT(*) FROM dim_player")).scalar() or 0,
            "matches":  conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar() or 0,
            "shots":    conn.execute(text("SELECT COUNT(*) FROM fact_shots")).scalar() or 0,
            "injuries": conn.execute(text("SELECT COUNT(*) FROM fact_injuries")).scalar() or 0,
        }


def get_seasons_in_db() -> set[tuple[str, str]]:
    """
    `dim_match` does not carry a competition column today, so the (competition, season)
    pair is degraded to ("La Liga", season_label) — the project's only configured
    competition. When more competitions are added this query will need a competition
    column or a join to dim_team → competition mapping.
    """
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT s.label
            FROM dim_match m
            JOIN dim_season s ON m.season_id = s.season_id
        """)).fetchall()
    return {("La Liga", r[0]) for r in rows}


_SOURCES = ("statsbomb", "understat", "whoscored", "sofascore", "transfermarkt")
_LA_LIGA_SEASON_TOTAL = 380
_TOTAL_BY_SOURCE = {
    "statsbomb":     _LA_LIGA_SEASON_TOTAL,
    "understat":     _LA_LIGA_SEASON_TOTAL,
    "whoscored":     None,
    "sofascore":     _LA_LIGA_SEASON_TOTAL,
    "transfermarkt": None,
}


def get_coverage_by_source(competition: str, season_label: str) -> list[dict]:
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(text("""
            SELECT m.data_source, COUNT(*)
            FROM dim_match m
            JOIN dim_season s ON m.season_id = s.season_id
            WHERE s.label = :season
            GROUP BY m.data_source
        """), {"season": season_label}).fetchall()
    counts = {(r[0] or "").lower(): r[1] for r in rows}
    return [
        {"source": src, "loaded": counts.get(src, 0), "total": _TOTAL_BY_SOURCE[src]}
        for src in _SOURCES
    ]


def get_recent_matches(limit: int = 20) -> pd.DataFrame:
    return query_df("""
        SELECT m.match_id, m.match_date, s.label AS season,
               ht.name_canonical AS home_team,
               at.name_canonical AS away_team,
               m.home_score, m.away_score, m.data_source
        FROM dim_match m
        JOIN dim_season s ON m.season_id = s.season_id
        LEFT JOIN dim_team ht ON m.home_team_id = ht.team_id
        LEFT JOIN dim_team at ON m.away_team_id = at.team_id
        ORDER BY m.created_at DESC
        LIMIT :lim
    """, {"lim": limit})


def get_player_review_stats() -> dict:
    eng = get_engine()
    with eng.connect() as conn:
        row = conn.execute(text("""
            SELECT
                COUNT(*),
                SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END),
                SUM(CASE WHEN resolved = TRUE  THEN 1 ELSE 0 END),
                AVG(CASE WHEN resolved = FALSE THEN similarity_score END)
            FROM player_review
        """)).fetchone()
    total, unresolved, resolved, avg = row or (0, 0, 0, None)
    return {
        "total":      int(total or 0),
        "unresolved": int(unresolved or 0),
        "resolved":   int(resolved or 0),
        "avg_score":  float(avg) if avg is not None else 0.0,
    }


def get_player_review_queue(limit: int = 50) -> pd.DataFrame:
    return query_df("""
        SELECT pr.id, pr.source_name, pr.source_system,
               p.name_canonical AS suggested_player_name,
               pr.similarity_score
        FROM player_review pr
        LEFT JOIN dim_player p ON pr.suggested_player_id = p.player_id
        WHERE pr.resolved = FALSE
        ORDER BY pr.similarity_score DESC NULLS LAST
        LIMIT :lim
    """, {"lim": limit})
