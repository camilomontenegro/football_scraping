"""
dashboard/app.py
================
Three-tab Streamlit dashboard for the football scraping project.

  - Exploration:        browse loaded data by competition / season / team
  - Pipeline monitoring: DB metrics, scanner, coverage, player review, recent matches
  - Shot Intelligence:  pitch heatmap, player finishing quality, shot breakdowns

Read-only — no scraper or loader is triggered from this UI.

Run from project root:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

# Make sibling modules (loaders/, pipeline_runner.py, etc.) importable when run
# as `streamlit run dashboard/app.py` from `football_scraping/`.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dashboard import analytics, db, explore, scanner, wizard_view

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

def _fmt(n) -> str:
    return f"{int(n):,}".replace(",", ".")


(tab_explore, tab_teams, tab_gk, tab_players, tab_injuries,
 tab_shot, tab_monitor, tab_wizard) = st.tabs(
    ["Exploration", "Teams", "Goalkeepers", "Players",
     "Injuries", "Shot Intelligence", "Pipeline monitoring", "Wizard"]
)


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
        m1.metric("Matches",  _fmt(summary['matches']))
        m2.metric("Goals",    _fmt(summary['goals']))
        m3.metric("xG",       f"{summary['xg']:.1f}")
        m4.metric("Injuries", _fmt(summary['injuries']))

        t_results, t_players, t_shots, t_events = st.tabs(
            ["Results", "Player stats", "Shots by source", "Events"]
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
# SHARED HELPER — 3-column selector row (reused across new tabs)
# ════════════════════════════════════════════════════════════════════
def _tab_selectors(key_prefix: str, all_seasons: bool = False):
    """Return (competition, season_or_none, team_or_none) for a new tab."""
    _comps = explore.get_competitions()
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        _comp = st.selectbox("Competition", _comps, key=f"{key_prefix}_comp")
    _seasons = explore.get_seasons_for_competition(_comp)
    season_opts = (["All seasons"] + _seasons) if all_seasons else (_seasons or ["(no seasons)"])
    with sc2:
        _season_sel = st.selectbox(
            "Season", season_opts,
            key=f"{key_prefix}_season",
            disabled=not _seasons,
        )
    _season = None if _season_sel == "All seasons" else _season_sel
    _teams = explore.get_teams_for_season(_season or (_seasons[0] if _seasons else "")) if _seasons else []
    with sc3:
        _team_sel = st.selectbox(
            "Team", ["All teams"] + _teams,
            key=f"{key_prefix}_team",
            disabled=not _teams,
        )
    _team = None if _team_sel == "All teams" else _team_sel
    return _comp, _season, _team


# ════════════════════════════════════════════════════════════════════
# TAB 2 — TEAMS
# ════════════════════════════════════════════════════════════════════
with tab_teams:
    st.header("Teams")
    _t_comp, _t_season, _t_team = _tab_selectors("teams")

    if _t_season is None:
        st.info("Select a season to view team standings.")
    else:
        df = explore.get_team_standings(_t_season, _t_team)
        if df.empty:
            st.info("No match data found. Run pipeline_runner.py to populate dim_match.")
        else:
            total_matches = int(df["p"].sum()) // 2
            total_goals = int(df["gf"].sum())
            avg_goals = round(total_goals / total_matches, 2) if total_matches else 0
            avg_xg = round(float(df["xg_for"].sum()) / total_matches, 2) if total_matches else 0

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Teams", len(df))
            m2.metric("Total goals", _fmt(total_goals))
            m3.metric("Avg goals/match", f"{avg_goals:.2f}")
            m4.metric("Avg xG/match", f"{avg_xg:.2f}")

            display_df = df.rename(columns={
                "p": "Played", "w": "Won", "d": "Drawn", "l": "Lost",
                "gf": "Goals For", "ga": "Goals Against", "gd": "Goal Diff",
                "xg_for": "xG For (season total)", "xg_against": "xG Against (season total)",
                "shots_for": "Shots For", "shots_against": "Shots Against",
            })
            st.dataframe(display_df, use_container_width=True)
            st.caption(
                "Source: dim_match (all sources combined) · xG and shots: fact_shots · "
                "xG For/Against = season-total expected goals (sum across all matches, not per-shot)"
            )


# ════════════════════════════════════════════════════════════════════
# TAB 3 — GOALKEEPERS
# ════════════════════════════════════════════════════════════════════
with tab_gk:
    st.header("Goalkeepers")
    _gk_comp, _gk_season, _gk_team = _tab_selectors("gk")

    if _gk_season is None:
        st.info("Select a season to view goalkeeper statistics.")
    else:
        df = explore.get_goalkeeper_stats(_gk_season, _gk_team)
        if df.empty:
            st.info("No goalkeeper data found for this selection.")
        else:
            gk_count = len(df)
            total_saves = int(df["saves"].sum())
            avg_save_pct = round(float(df["save_pct"].dropna().mean()), 1) if not df["save_pct"].dropna().empty else 0
            total_cs = int(df["clean_sheets"].sum())

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Goalkeepers tracked", gk_count)
            m2.metric("Total saves", _fmt(total_saves))
            m3.metric("Avg save %", f"{avg_save_pct:.1f}%")
            m4.metric("Clean sheets", _fmt(total_cs))

            display_df = df.rename(columns={
                "goalkeeper": "Goalkeeper",
                "team": "Team",
                "matches_played": "Matches",
                "goals_allowed": "Goals Allowed",
                "shots_faced": "Shots On Target Faced",
                "saves": "Saves",
                "save_pct": "Save % (saves/shots×100)",
                "xg_conceded": "xG Conceded",
                "goals_saved_above_expected": "Goals Saved Above Expected",
                "clean_sheets": "Clean Sheets",
            })
            st.dataframe(display_df, use_container_width=True)
            st.caption(
                "Stats are scoped to matches where each GK appeared in event data (substitutions, cards, etc.) — "
                "used as a proxy for matches played. "
                "Shots On Target Faced = goals + saves (blocked/missed excluded) · "
                "Save % = saves ÷ shots on target × 100 · "
                "xG Conceded = total expected-goal value of shots faced · "
                "Goals Saved Above Expected = saves − xG conceded (positive = outperforming)"
            )


# ════════════════════════════════════════════════════════════════════
# TAB 4 — PLAYERS
# ════════════════════════════════════════════════════════════════════
with tab_players:
    st.header("Players")
    _pl_comp, _pl_season, _pl_team = _tab_selectors("players", all_seasons=True)

    df = explore.get_player_discipline(_pl_season, _pl_team)
    if df.empty:
        st.info("No player data found for this selection.")
    else:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Players tracked", df["player"].nunique())
        m2.metric("Total goals", _fmt(df['goals'].sum()))
        m3.metric("Yellow cards", _fmt(df['yellow_cards'].sum()))
        m4.metric("Red cards", _fmt(df['red_cards'].sum()))

        display_df = df.copy()
        if _pl_season is not None:
            display_df = display_df.drop(columns=["season"], errors="ignore")

        st.dataframe(display_df, use_container_width=True)

        if _pl_team is None and not df.empty:
            top10 = df.groupby("player")["goals"].sum().nlargest(10).reset_index()
            if not top10.empty:
                st.bar_chart(top10.set_index("player")["goals"])

        st.caption(
            "Goals and xG: fact_shots (all sources) · Cards: fact_events (SofaScore incidents + StatsBomb)\n"
            "Rows show per-season accumulation when All seasons is selected."
        )


# ════════════════════════════════════════════════════════════════════
# TAB 5 — INJURIES
# ════════════════════════════════════════════════════════════════════
with tab_injuries:
    st.header("Injuries")
    _inj_comp, _inj_season, _inj_team = _tab_selectors("injuries", all_seasons=True)

    df = explore.get_injuries_standalone(_inj_season, _inj_team)
    if df.empty:
        st.info("No injury data found for this selection.")
    else:
        total_inj = len(df)
        total_days = int(pd.to_numeric(df["days_absent"], errors="coerce").fillna(0).sum())
        total_missed = int(pd.to_numeric(df["matches_missed"], errors="coerce").fillna(0).sum())
        ongoing = int(df["date_until"].isna().sum())

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total injuries", _fmt(total_inj))
        m2.metric("Total days absent", _fmt(total_days))
        m3.metric("Total matches missed", _fmt(total_missed))
        m4.metric("Ongoing injuries", _fmt(ongoing))

        df_render = df.copy()
        df_render["date_until"] = df_render["date_until"].where(
            df_render["date_until"].notna(), "Ongoing"
        )
        st.dataframe(df_render, use_container_width=True)

        breakdown = explore.get_injury_type_breakdown(_inj_season, _inj_team)
        if not breakdown.empty:
            st.subheader("Top injury types")
            st.bar_chart(
                breakdown.head(10).set_index("injury_type")["count"]
            )

        if _inj_season is None:
            trend = explore.get_injury_season_trend(_inj_team)
            if not trend.empty:
                st.subheader("Season trend")
                st.dataframe(trend, use_container_width=True)

        st.caption(
            "Source: fact_injuries (Transfermarkt)\n"
            "date_until = NULL means the player was still injured at time of data collection."
        )


# ════════════════════════════════════════════════════════════════════
# TAB 6 — SHOT INTELLIGENCE
# ════════════════════════════════════════════════════════════════════
with tab_shot:
    st.header("Shot Intelligence")
    st.caption("Understat data only · Pitch coordinates: 105 m × 68 m")

    # ── mplsoccer availability guard ─────────────────────────────
    try:
        from mplsoccer import Pitch as _Pitch
    except ImportError:
        st.error("Install mplsoccer: pip install mplsoccer")
        st.stop()

    # ── Shared filters ───────────────────────────────────────────
    _si_competitions = explore.get_competitions()
    _si_seasons_base = (
        explore.get_seasons_for_competition(_si_competitions[0])
        if _si_competitions else []
    )
    sf1, sf2, sf3, sf4 = st.columns(4)
    with sf1:
        si_competition = st.selectbox(
            "Competition",
            _si_competitions or ["(none)"],
            key="si_competition",
            disabled=not _si_competitions,
        )
    _si_seasons = explore.get_seasons_for_competition(si_competition) if _si_competitions else []
    with sf2:
        si_season = st.selectbox(
            "Season",
            _si_seasons or ["(no seasons in DB)"],
            key="si_season",
            disabled=not _si_seasons,
        )
    _si_teams = explore.get_teams_for_season(si_season) if _si_seasons else []
    with sf3:
        si_team_choice = st.selectbox(
            "Team", ["All teams"] + _si_teams,
            key="si_team",
            disabled=not _si_teams,
        )
    _si_team_name = None if si_team_choice == "All teams" else si_team_choice
    _si_team_id = analytics._resolve_team_id(_si_team_name)
    _si_competition_val = si_competition if _si_competitions else None

    with sf4:
        metric_choice = st.radio(
            "Metric",
            ["Average xG per shot", "Conversion rate"],
            key="si_metric",
        )
    metric_col = "avg_xg" if metric_choice == "Average xG per shot" else "conversion_rate"
    metric_label = "Avg xG" if metric_col == "avg_xg" else "Conversion Rate"

    if not _si_seasons:
        st.info("No seasons in the database yet.")
    else:
        # ── Section 1 — Pitch Danger Heatmap ─────────────────────
        st.subheader("Pitch Danger Heatmap")

        hm_df = analytics.get_heatmap_data(si_season, _si_team_id)

        if hm_df.empty:
            st.info("No shot data with coordinates for this selection.")
        else:
            scope = si_team_choice
            hm_title = f"{metric_label} by zone — {si_season} · {scope}"

            X_BANDS = list(range(0, 101, 10))
            Y_BANDS = list(range(0, 61, 10))
            grid = np.full((len(Y_BANDS), len(X_BANDS)), np.nan)
            for _, r in hm_df.iterrows():
                xb, yb = int(r["x_band"]), int(r["y_band"])
                if xb in X_BANDS and yb in Y_BANDS:
                    grid[Y_BANDS.index(yb), X_BANDS.index(xb)] = float(r[metric_col] or 0)

            x_edges = np.array(X_BANDS + [105], dtype=float)
            y_edges = np.array(Y_BANDS + [68],  dtype=float)

            pitch = _Pitch(
                pitch_type="custom", pitch_length=105, pitch_width=68,
                pitch_color="#1a472a", line_color="white", line_zorder=2,
            )
            fig, ax = pitch.draw(figsize=(12, 7))
            fig.patch.set_facecolor("#1a472a")

            hm_mesh = ax.pcolormesh(
                x_edges, y_edges, grid,
                cmap="Reds", alpha=0.75, zorder=1, vmin=0,
            )
            plt.colorbar(hm_mesh, ax=ax, shrink=0.6, label=metric_label)
            ax.set_title(hm_title, color="white", fontsize=13, pad=12)

            st.pyplot(fig)
            plt.close(fig)

            with st.expander("Zone data table"):
                st.dataframe(
                    hm_df[["x_band", "y_band", "shots", "goals", "avg_xg", "conversion_rate"]],
                    use_container_width=True,
                )

        st.divider()

        # ── Section 2 — Player Finishing Quality ──────────────────
        st.subheader("Player Finishing Quality")
        st.caption("Min. 20 shots to qualify · Goals − xG: positive = overperforming")

        pf_df = analytics.get_player_finishing(si_season, _si_team_id)

        if pf_df.empty:
            st.info("No players with 20+ Understat shots for this selection.")
        else:
            bar_colors = [
                "#2ecc71" if v >= 0 else "#e74c3c"
                for v in pf_df["goals_minus_xg"]
            ]
            fig2, ax2 = plt.subplots(figsize=(10, max(4, len(pf_df) * 0.45)))
            fig2.patch.set_facecolor("#0e1117")
            ax2.set_facecolor("#0e1117")
            ax2.barh(pf_df["player"], pf_df["goals_minus_xg"], color=bar_colors)
            ax2.axvline(0, color="white", linewidth=0.8, linestyle="--")
            ax2.set_xlabel("Goals − xG", color="white")
            ax2.tick_params(colors="white")
            for spine in ax2.spines.values():
                spine.set_color("#444")
            ax2.invert_yaxis()
            plt.tight_layout()

            st.pyplot(fig2)
            plt.close(fig2)

            st.dataframe(
                pf_df[["player", "shots", "goals", "total_xg", "goals_minus_xg"]],
                use_container_width=True,
            )

        st.divider()

        # ── Section 3 — Set-piece Specialists ────────────────────
        st.subheader("Set-piece Specialists")

        sp_df = analytics.get_setpiece_goals(si_season, _si_team_id)

        if sp_df.empty:
            st.info("No set-piece goal data for this selection.")
        else:
            display_sp = sp_df.rename(columns={
                "player":        "Player",
                "team":          "Team",
                "penalty_goals": "Penalty Goals",
                "freekick_goals":"Free Kick Goals",
                "openplay_goals":       "Open Play Goals",
                "setpiece_other_goals": "Set Piece / Other",
                "total_goals":          "Total Goals",
            }).sort_values("Penalty Goals", ascending=False)
            st.dataframe(display_sp, use_container_width=True)

            _sp_players = explore.get_players_for_season(si_season, _si_team_id)
            _sp_labels = ["All players"] + [name for name, _ in _sp_players]
            _sp_id_map = {name: pid for name, pid in _sp_players}

            si_player_name = st.selectbox(
                "Player drill-down", _sp_labels, key="si_player"
            )
            si_player_id = _sp_id_map.get(si_player_name)

            if si_player_id is not None:
                bucket_df = analytics.get_setpiece_goals(
                    si_season, _si_team_id, player_id=si_player_id
                )
                if not bucket_df.empty:
                    st.bar_chart(bucket_df.set_index("situation_bucket")["goals"])

            st.caption(
                "Source: fact_shots (Understat) · "
                "Penalty = situation 'penalty' · "
                "Free Kick = 'direct freekick' / 'free-kick'"
            )

        st.divider()


# ════════════════════════════════════════════════════════════════════
# TAB 7 — PIPELINE MONITORING
# ════════════════════════════════════════════════════════════════════
with tab_monitor:
    st.header("Pipeline monitoring")

    # ── Section 1 — DB metric cards ───────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Players",         _fmt(_DB_SUMMARY['players']))
    p2.metric("Matches",         _fmt(_DB_SUMMARY['matches']))
    p3.metric("Shots (with xG)", _fmt(_DB_SUMMARY['shots']))
    p4.metric("Injuries",        _fmt(_DB_SUMMARY['injuries']))

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
                st.write(f"**{src}** — {_fmt(loaded)}")
            else:
                st.write(f"**{src}** — {_fmt(loaded)} / {_fmt(total)}")
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
    r1.metric("Total",           _fmt(pr_stats['total']))
    r2.metric("Unresolved",      _fmt(pr_stats['unresolved']))
    r3.metric("Resolved",        _fmt(pr_stats['resolved']))
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


# ════════════════════════════════════════════════════════════════════
# TAB 8 — WIZARD (writes to the database — read-only exception)
# ════════════════════════════════════════════════════════════════════
with tab_wizard:
    wizard_view.render()

