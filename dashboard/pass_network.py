"""
dashboard/pass_network.py
=========================
Read-only DB queries for the Pass Network tab.

A pass network is built from fact_events (WhoScored — the only source with
full pass-level coordinates). fact_events has no receiver column, so the
receiver is derived as the player of the *next* event by the same team
(standard technique for WhoScored data, ordered by minute/second/event_id —
event_id is a SERIAL that preserves the original chronological order).

Coordinates in fact_events are normalised 0-1; the UI scales them to a
105 m x 68 m pitch (same convention as the Shot Intelligence tab).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard.db import query_df


@st.cache_data(ttl=300)
def get_matches_with_passes(competition: str, season: str) -> pd.DataFrame:
    """Matches of a competition/season that have WhoScored pass events.

    Returns columns: match_id, match_date, home, away, home_team_id,
    away_team_id, home_score, away_score, label.
    """
    df = query_df("""
        SELECT m.match_id, m.match_date,
               th.canonical_name AS home, ta.canonical_name AS away,
               m.home_team_id, m.away_team_id,
               m.home_score, m.away_score
        FROM dim_match m
        JOIN dim_competition c ON c.canonical_id = m.competition_id
        JOIN dim_team th ON th.canonical_id = m.home_team_id
        JOIN dim_team ta ON ta.canonical_id = m.away_team_id
        WHERE c.canonical_name = :competition
          AND m.season = :season
          AND EXISTS (
              SELECT 1 FROM fact_events e
              WHERE e.match_id = m.match_id
                AND e.data_source = 'whoscored'
                AND e.event_type = 'Pass'
          )
        ORDER BY m.match_date DESC, m.match_id
    """, {"competition": competition, "season": season})

    if df.empty:
        return df

    def _label(r) -> str:
        score = ""
        if pd.notna(r["home_score"]) and pd.notna(r["away_score"]):
            score = f" {int(r['home_score'])}-{int(r['away_score'])}"
        return f"{r['match_date']} — {r['home']}{score} {r['away']}"

    df["label"] = df.apply(_label, axis=1)
    return df


@st.cache_data(ttl=300)
def get_pass_network(match_id: int, team_id: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pass network for one team in one match.

    Returns (nodes, edges):
      nodes: player_id, player, x, y, passes        (avg pass-origin location)
      edges: passer_id, receiver_id, pass_count     (bidirectional pairs merged)
    """
    passes = query_df("""
        WITH ordered AS (
            SELECT e.event_id, e.player_id, e.team_id,
                   e.event_type, e.outcome, e.x, e.y,
                   LEAD(e.player_id) OVER w AS next_player,
                   LEAD(e.team_id)   OVER w AS next_team
            FROM fact_events e
            WHERE e.match_id = :mid AND e.data_source = 'whoscored'
            WINDOW w AS (ORDER BY e.minute, e.second, e.event_id)
        )
        SELECT o.player_id, o.next_player AS receiver_id, o.x, o.y
        FROM ordered o
        WHERE o.event_type = 'Pass'
          AND o.outcome = 'Successful'
          AND o.team_id = :tid
          AND o.next_team = :tid
          AND o.next_player <> o.player_id
          AND o.x IS NOT NULL AND o.y IS NOT NULL
    """, {"mid": match_id, "tid": team_id})

    if passes.empty:
        return pd.DataFrame(), pd.DataFrame()

    # DECIMAL columns arrive as Python Decimal — cast for numpy/matplotlib
    passes = passes.astype(
        {"player_id": int, "receiver_id": int, "x": float, "y": float}
    )

    names = query_df("""
        SELECT canonical_id AS player_id, canonical_name AS player
        FROM dim_player
        WHERE canonical_id = ANY(:ids)
    """, {"ids": list({int(i) for i in (*passes["player_id"], *passes["receiver_id"])})})

    # Nodes: average pass-origin location + volume per player
    nodes = (
        passes.groupby("player_id")
        .agg(x=("x", "mean"), y=("y", "mean"), passes=("player_id", "size"))
        .reset_index()
        .merge(names, on="player_id", how="left")
    )
    nodes["player"] = nodes["player"].fillna("?")

    # Edges: merge A->B and B->A into a single undirected pair
    edges = passes.groupby(["player_id", "receiver_id"]).size().reset_index(name="n")
    pair = edges.apply(
        lambda r: tuple(sorted((int(r["player_id"]), int(r["receiver_id"])))), axis=1
    )
    edges["pair_a"] = [p[0] for p in pair]
    edges["pair_b"] = [p[1] for p in pair]
    edges = (
        edges.groupby(["pair_a", "pair_b"])["n"].sum().reset_index()
        .rename(columns={"pair_a": "passer_id", "pair_b": "receiver_id", "n": "pass_count"})
    )

    return nodes, edges
