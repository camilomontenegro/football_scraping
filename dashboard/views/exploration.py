"""
dashboard/views/exploration.py
==============================
Exploration — results, player stats, shots by source, events, standings.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import streamlit as st

from dashboard import explore
from dashboard.i18n import t
from dashboard.views.shared import _fmt, _empty_info


def render() -> None:
    st.header(t("exploration"))

    competitions = explore.get_competitions()
    c1, c2, c3 = st.columns(3)
    with c1:
        competition = st.selectbox(t("competition"), competitions, key="ex_comp")
    seasons = explore.get_seasons_for_competition(competition)
    with c2:
        season = st.selectbox(
            t("season"), seasons or ["(no seasons in DB)"], key="ex_season",
            disabled=not seasons,
        )
    teams = explore.get_teams_for_season(season, competition) if seasons else []
    with c3:
        team_choice = st.selectbox(
            t("team"), [t("all_teams")] + teams, key="ex_team",
            disabled=not teams,
        )
    team = None if team_choice == t("all_teams") else team_choice

    if not seasons:
        st.info(t("no_seasons"))
    else:
        # Metric cards
        summary = explore.get_season_summary(season, team, competition)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(t("matches"),  _fmt(summary['matches']))
        m2.metric(t("goals"),    _fmt(summary['goals']))
        m3.metric("xG",         f"{summary['xg']:.1f}")
        m4.metric(t("tab_injuries"), _fmt(summary['injuries']))

        t_results, t_player_stats, t_shots, t_events, t_standings = st.tabs(
            [t("results"), t("player_stats"), t("shots_by_source"),
             t("events"), t("tab_teams")]
        )

        # ── Results ───────────────────────────────────────
        with t_results:
            df = explore.get_results(season, team, competition)
            if df.empty:
                _empty_info()
            else:
                st.dataframe(df, width='stretch')
                if team is not None and "result" in df.columns:
                    wins   = int((df["result"] == "W").sum())
                    draws  = int((df["result"] == "D").sum())
                    losses = int((df["result"] == "L").sum())
                    w1, w2, w3 = st.columns(3)
                    w1.metric(t("wins"),   wins)
                    w2.metric(t("draws"),  draws)
                    w3.metric(t("losses"), losses)

        # ── Player stats ──────────────────────────────────
        with t_player_stats:
            df = explore.get_player_stats(season, team, competition)
            if df.empty:
                st.info("No shot data found for this selection. "
                        "Check pipeline coverage in the monitoring tab.")
            else:
                st.dataframe(df, width='stretch')
                st.caption(
                    "Source: fact_shots (all sources combined — "
                    "StatsBomb, Understat, SofaScore)."
                )

        # ── Shots by source ───────────────────────────────
        with t_shots:
            df = explore.get_shots_by_source(season, team, competition)
            if df.empty:
                _empty_info()
            else:
                st.dataframe(df, width='stretch')
                st.bar_chart(df.set_index("data_source")["shots"])
                st.caption(
                    "Each source covers different event types. Understat and StatsBomb "
                    "include xG. SofaScore shots may have NULL coordinates."
                )

        # ── Events ────────────────────────────────────────
        with t_events:
            df = explore.get_events_summary(season, team, competition)
            if df.empty:
                st.info("No event data found for this selection.")
            else:
                st.dataframe(df, width='stretch')
                st.caption(
                    "SofaScore events are incident-only (cards, substitutions, VAR) — "
                    "coordinates are NULL by design. WhoScored and StatsBomb events "
                    "include x/y coordinates."
                )

        # ── Standings (formerly Teams tab) ────────────────
        with t_standings:
            df = explore.get_team_standings(season, team, competition)
            if df.empty:
                st.info("No match data found. Run pipeline_runner.py to populate dim_match.")
            else:
                total_matches = int(df["p"].sum()) // 2
                total_goals = int(df["gf"].sum())
                avg_goals = round(total_goals / total_matches, 2) if total_matches else 0
                avg_xg = round(float(df["xg_for"].sum()) / total_matches, 2) if total_matches else 0

                sm1, sm2, sm3, sm4 = st.columns(4)
                sm1.metric(t("tab_teams"), len(df))
                sm2.metric(t("total_goals"), _fmt(total_goals))
                sm3.metric(t("avg_goals_match"), f"{avg_goals:.2f}")
                sm4.metric(t("avg_xg_match"), f"{avg_xg:.2f}")

                display_df = df.rename(columns={
                    "p": "Played", "w": "Won", "d": "Drawn", "l": "Lost",
                    "gf": "Goals For", "ga": "Goals Against", "gd": "Goal Diff",
                    "xg_for": "xG For (season total)", "xg_against": "xG Against (season total)",
                    "shots_for": "Shots For", "shots_against": "Shots Against",
                })
                st.dataframe(display_df, width='stretch')
                st.caption(
                    "Source: dim_match (all sources combined) · xG and shots: fact_shots · "
                    "xG For/Against = season-total expected goals (sum across all matches, not per-shot)"
                )
