"""
dashboard/app.py
================
Two-tab Streamlit dashboard for the football scraping project.

  - Exploration:        browse loaded data by competition / season / team
  - Pipeline monitoring: DB metrics, scanner, coverage, player review, recent matches

Read-only — no scraper or loader is triggered from this UI.

Run from project root:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Make sibling modules (loaders/, pipeline_runner.py, etc.) importable when run
# as `streamlit run dashboard/app.py` from `football_scraping/`.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dashboard import db, explore, scanner

st.set_page_config(
    page_title="Football Scraping Dashboard",
    page_icon="⚽",
    layout="wide",
)

# ─────────────────────────────────────────────
# DB-unreachable guard (runs once on each rerun)
# ─────────────────────────────────────────────
try:
    _DB_SUMMARY = db.get_db_summary()
except Exception:
    st.error("Cannot connect to the database. Check your .env file.")
    st.stop()

tab_explore, tab_monitor = st.tabs(["Exploration", "Pipeline monitoring"])


# ════════════════════════════════════════════════════════════════════
# TAB 1 — EXPLORATION
# ════════════════════════════════════════════════════════════════════
def _empty_info(message: str = "No data found for this selection. "
                                "Check pipeline coverage in the Pipeline monitoring tab."):
    st.info(message)


with tab_explore:
    st.header("Exploration")

    competitions = explore.get_competitions()
    c1, c2, c3 = st.columns(3)
    with c1:
        competition = st.selectbox("Competition", competitions, key="ex_comp")
    seasons = explore.get_seasons_for_competition(competition)
    with c2:
        season = st.selectbox(
            "Season", seasons or ["(no seasons in DB)"], key="ex_season",
            disabled=not seasons,
        )
    teams = explore.get_teams_for_season(season) if seasons else []
    with c3:
        team_choice = st.selectbox(
            "Team", ["All teams"] + teams, key="ex_team",
            disabled=not teams,
        )
    team = None if team_choice == "All teams" else team_choice

    if not seasons:
        st.info("No seasons in `dim_season` yet. Run `python pipeline_runner.py` "
                "to populate the database.")
    else:
        # Metric cards
        summary = explore.get_season_summary(season, team)
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Matches",  f"{summary['matches']:,}")
        m2.metric("Goals",    f"{summary['goals']:,}")
        m3.metric("xG",       f"{summary['xg']:.1f}")
        m4.metric("Injuries", f"{summary['injuries']:,}")

        t_results, t_players, t_shots, t_inj, t_events = st.tabs(
            ["Results", "Player stats", "Shots by source", "Injuries", "Events"]
        )

        # ── Results ───────────────────────────────────────
        with t_results:
            df = explore.get_results(season, team)
            if df.empty:
                _empty_info()
            else:
                st.dataframe(df, use_container_width=True)
                if team is not None and "result" in df.columns:
                    wins   = int((df["result"] == "W").sum())
                    draws  = int((df["result"] == "D").sum())
                    losses = int((df["result"] == "L").sum())
                    w1, w2, w3 = st.columns(3)
                    w1.metric("Wins",   wins)
                    w2.metric("Draws",  draws)
                    w3.metric("Losses", losses)

        # ── Player stats ──────────────────────────────────
        with t_players:
            df = explore.get_player_stats(season, team)
            if df.empty:
                st.info("No shot data found for this selection. "
                        "Check pipeline coverage in the monitoring tab.")
            else:
                st.dataframe(df, use_container_width=True)
                st.caption(
                    "Source: fact_shots (all sources combined — "
                    "StatsBomb, Understat, SofaScore)."
                )

        # ── Shots by source ───────────────────────────────
        with t_shots:
            df = explore.get_shots_by_source(season, team)
            if df.empty:
                _empty_info()
            else:
                st.dataframe(df, use_container_width=True)
                st.bar_chart(df.set_index("data_source")["shots"])
                st.caption(
                    "Each source covers different event types. Understat and StatsBomb "
                    "include xG. SofaScore shots may have NULL coordinates."
                )

        # ── Injuries ──────────────────────────────────────
        with t_inj:
            df = explore.get_injuries(season, team)
            if df.empty:
                _empty_info()
            else:
                df_render = df.copy()
                df_render["date_until"] = df_render["date_until"].where(
                    df_render["date_until"].notna(), "Ongoing"
                )
                st.dataframe(df_render, use_container_width=True)
                total_days   = int(pd.to_numeric(df["days_absent"], errors="coerce").fillna(0).sum())
                total_missed = int(pd.to_numeric(df["matches_missed"], errors="coerce").fillna(0).sum())
                i1, i2 = st.columns(2)
                i1.metric("Total days absent",    f"{total_days:,}")
                i2.metric("Total matches missed", f"{total_missed:,}")

        # ── Events ────────────────────────────────────────
        with t_events:
            df = explore.get_events_summary(season, team)
            if df.empty:
                st.info("No event data found for this selection.")
            else:
                st.dataframe(df, use_container_width=True)
                st.caption(
                    "SofaScore events are incident-only (cards, substitutions, VAR) — "
                    "coordinates are NULL by design. WhoScored and StatsBomb events "
                    "include x/y coordinates."
                )


# ════════════════════════════════════════════════════════════════════
# TAB 2 — PIPELINE MONITORING
# ════════════════════════════════════════════════════════════════════
with tab_monitor:
    st.header("Pipeline monitoring")

    # ── Section 1 — DB metric cards ───────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Players",         f"{_DB_SUMMARY['players']:,}")
    p2.metric("Matches",         f"{_DB_SUMMARY['matches']:,}")
    p3.metric("Shots (with xG)", f"{_DB_SUMMARY['shots']:,}")
    p4.metric("Injuries",        f"{_DB_SUMMARY['injuries']:,}")

    st.divider()

    # ── Section 2 — Season scanner ────────────────────────
    st.subheader("Season scanner")
    if st.button("Scan all sources", type="primary", key="scan_btn"):
        with st.spinner("Scanning all sources..."):
            st.session_state["scan_results"] = scanner.scan_all()

    scan_results = st.session_state.get("scan_results")
    if scan_results is not None:
        errors = scan_results.get("_errors") or {}
        if errors:
            st.warning(f"Scanner errors: {sorted(errors.keys())}")

        rows = []
        for src in ("statsbomb", "understat", "sofascore", "transfermarkt", "whoscored"):
            for r in scan_results.get(src, []):
                rows.append({
                    "source":      src,
                    "competition": r.get("competition"),
                    "season":      r.get("season"),
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
            st.info(
                "To load missing seasons, run:\n\n"
                "    python pipeline_runner.py --sources <source>\n\n"
                "Loading is intentionally CLI-only in this dashboard."
            )
        else:
            st.success("All scanned sources are up-to-date — no missing seasons.")

    st.divider()

    # ── Section 3 — Coverage ──────────────────────────────
    st.subheader("Coverage by source")
    cov_competitions = explore.get_competitions()
    cov_seasons = explore.get_seasons_for_competition(cov_competitions[0]) \
        if cov_competitions else []
    cc1, cc2 = st.columns(2)
    with cc1:
        cov_comp = st.selectbox("Competition", cov_competitions, key="cov_comp")
    with cc2:
        cov_season = st.selectbox(
            "Season", cov_seasons or ["(no seasons)"], key="cov_season",
            disabled=not cov_seasons,
        )

    if cov_seasons:
        coverage = db.get_coverage_by_source(cov_comp, cov_season)
        total_loaded = 0
        total_total = 0
        for row in coverage:
            src    = row["source"]
            loaded = row["loaded"] or 0
            total  = row["total"]
            if total is None:
                st.write(f"**{src}** — {loaded:,}")
            else:
                st.write(f"**{src}** — {loaded:,} / {total:,}")
                if total > 0:
                    st.progress(min(loaded / total, 1.0))
                total_loaded += loaded
                total_total  += total
            if src == "sofascore":
                st.caption(
                    "SofaScore events are incident-only. "
                    "Coordinates are NULL by design."
                )
        if total_total > 0:
            st.write("**Overall**")
            st.progress(min(total_loaded / total_total, 1.0))

    st.divider()

    # ── Section 4 — Player review ─────────────────────────
    st.subheader("Player review queue")
    pr_stats = db.get_player_review_stats()
    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Total",           f"{pr_stats['total']:,}")
    r2.metric("Unresolved",      f"{pr_stats['unresolved']:,}")
    r3.metric("Resolved",        f"{pr_stats['resolved']:,}")
    r4.metric("Avg similarity",  f"{pr_stats['avg_score']:.1f}")
    pr_df = db.get_player_review_queue(50)
    if pr_df.empty:
        st.info("No unresolved entries in `player_review`.")
    else:
        st.dataframe(pr_df, use_container_width=True)
    st.info(
        "To resolve a case, run:\n\n"
        "    python -m scripts.review_players --unresolved"
    )

    st.divider()

    # ── Section 5 — Recent matches ────────────────────────
    st.subheader("Recent matches")
    rm_df = db.get_recent_matches(20)
    if rm_df.empty:
        st.info("No matches in `dim_match` yet.")
    else:
        st.dataframe(rm_df, use_container_width=True)
