"""
dashboard/views/shot_intelligence.py
====================================
Shot Intelligence — pitch heatmap, player finishing, set-piece specialists.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from dashboard import analytics, explore, pass_network
from dashboard.i18n import t


def render() -> None:
    st.header(t("shot_intelligence"))
    st.caption("All sources · Pitch coordinates: 105 m × 68 m · Coordinates normalised to metres")

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
    _si_teams = explore.get_teams_for_season(si_season, si_competition) if _si_seasons else []
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
        # ── Section 1 — Pitch view: Heatmap / Shot Map / Goal Mouth ──
        si_view = st.selectbox(
            t("pitch_view"),
            [t("pitch_danger_heatmap"), t("shot_map"), t("goal_mouth")],
            key="si_view",
        )

        if si_view in (t("shot_map"), t("goal_mouth")):
            # ── Per-match views (WhoScored events) ───────────────
            st.subheader(si_view)

            from mplsoccer import VerticalPitch as _SmPitch

            _sm_matches = pass_network.get_matches_with_passes(si_competition, si_season)
            if _sm_matches.empty:
                st.info(t("no_pass_matches"))
            else:
                sm_match_label = st.selectbox(
                    t("match"), _sm_matches["label"].tolist(), key="si_sm_match"
                )
                _sm_row = _sm_matches[_sm_matches["label"] == sm_match_label].iloc[0]
                _sm_mid = int(_sm_row["match_id"])

                _SM_COLORS = {
                    "Goal":        "#2ecc71",
                    "MissedShots": "#e74c3c",
                    "SavedShot":   "#3498db",
                    "ShotOnPost":  "#f39c12",
                }

                def _draw_shot_map(team_name: str, team_id: int) -> None:
                    shots = analytics.get_match_shots(_sm_mid, team_id)
                    if shots.empty:
                        st.info(t("no_shot_data"))
                        return

                    pitch = _SmPitch(
                        pitch_type="custom", pitch_length=105, pitch_width=68,
                        half=True, pitch_color="grass", stripe=True,
                        line_color="white", line_zorder=2,
                    )
                    fig, ax = pitch.draw(figsize=(6, 5))

                    for etype, color in _SM_COLORS.items():
                        sub = shots[shots["event_type"] == etype]
                        if sub.empty:
                            continue
                        is_goal = etype == "Goal"
                        # Trajectory to the goal-line crossing point — only
                        # for shots whose end coords have been populated.
                        traj = sub.dropna(subset=["end_x", "end_y"])
                        if not traj.empty:
                            pitch.arrows(
                                traj["x"] * 105, traj["y"] * 68,
                                traj["end_x"] * 105, traj["end_y"] * 68,
                                width=1.5, headwidth=6, headlength=5,
                                headaxislength=4.5,
                                color=color, alpha=0.85, zorder=3, ax=ax,
                            )
                        pitch.scatter(
                            sub["x"] * 105, sub["y"] * 68,
                            s=160 if is_goal else 90,
                            color=color, edgecolors="white",
                            linewidths=1.0 if is_goal else 0.6,
                            alpha=0.95, zorder=5 if is_goal else 4,
                            label=etype, ax=ax,
                        )
                    ax.legend(loc="lower left", fontsize=8, framealpha=0.85)
                    ax.set_title(team_name, fontsize=12, pad=10)
                    st.pyplot(fig)
                    plt.close(fig)

                    n_goals = int((shots["event_type"] == "Goal").sum())
                    n_target = int(shots["event_type"].isin(["Goal", "SavedShot"]).sum())
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Shots", len(shots))
                    c2.metric("On target", n_target)
                    c3.metric("Goals", n_goals)

                    with st.expander(f"{team_name} — shot list"):
                        st.dataframe(
                            shots[["minute", "player", "event_type"]],
                            width="stretch", hide_index=True,
                        )

                def _draw_goal_mouth(team_name: str, team_id: int) -> None:
                    """Front view of the goal: where each shot crossed the
                    goal line, in real metres (goal: 7.32 m × 2.44 m)."""
                    shots = analytics.get_match_shots(_sm_mid, team_id)
                    gm = (
                        shots.dropna(subset=["end_y", "end_z"])
                        if not shots.empty else shots
                    )
                    if gm.empty:
                        st.info(t("no_goalmouth_data"))
                        return

                    # WhoScored units → metres:
                    #   goalMouthY is % of pitch width (68 m), goal centred at 50
                    #   goalMouthZ: crossbar (2.44 m) ≈ 38 units
                    y_m = (gm["end_y"] * 100 - 50) * 0.68
                    z_m = gm["end_z"] * 100 * (2.44 / 38.0)

                    # Fixed window so the goal keeps its real proportions;
                    # extreme wide/high misses are counted, not plotted.
                    _X_WIN, _Z_WIN = 8.0, 4.2
                    _in_view = (y_m.abs() <= _X_WIN) & (z_m <= _Z_WIN)
                    n_off = int((~_in_view).sum())
                    gm, y_m, z_m = gm[_in_view], y_m[_in_view], z_m[_in_view]

                    fig, ax = plt.subplots(figsize=(7, 3.4))
                    ax.set_facecolor("#5d8c3f")
                    # net
                    ax.add_patch(plt.Rectangle(
                        (-3.66, 0), 7.32, 2.44, fill=False, hatch="++",
                        edgecolor="white", linewidth=0, alpha=0.25, zorder=1,
                    ))
                    # ground + goal frame
                    ax.axhline(0, color="white", lw=2, zorder=2)
                    ax.plot([-3.66, -3.66, 3.66, 3.66], [0, 2.44, 2.44, 0],
                            color="white", lw=4, zorder=3,
                            solid_capstyle="round")

                    for etype, color in _SM_COLORS.items():
                        m = (gm["event_type"] == etype).to_numpy()
                        if not m.any():
                            continue
                        is_goal = etype == "Goal"
                        ax.scatter(
                            y_m[m], z_m[m], s=130 if is_goal else 70,
                            color=color, edgecolors="white",
                            linewidths=1.0 if is_goal else 0.6,
                            alpha=0.95, zorder=5 if is_goal else 4,
                            label=etype,
                        )

                    ax.set_xlim(-_X_WIN, _X_WIN)
                    ax.set_ylim(-0.15, _Z_WIN)
                    ax.set_aspect("equal")
                    ax.set_xticks([])
                    ax.set_yticks([])
                    for spine in ax.spines.values():
                        spine.set_visible(False)
                    ax.legend(loc="upper right", fontsize=7, framealpha=0.85)
                    if n_off:
                        ax.text(
                            -_X_WIN + 0.3, _Z_WIN - 0.25,
                            f"+{n_off} off view", color="white",
                            fontsize=8, va="top",
                        )
                    ax.set_title(team_name, fontsize=12, pad=10)
                    st.pyplot(fig)
                    plt.close(fig)

                    n_goals = int((gm["event_type"] == "Goal").sum())
                    n_target = int(gm["event_type"].isin(["Goal", "SavedShot"]).sum())
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Shots plotted", len(gm) + n_off)
                    c2.metric("On target", n_target)
                    c3.metric("Goals", n_goals)

                _draw_team = (
                    _draw_shot_map if si_view == t("shot_map") else _draw_goal_mouth
                )
                sm_home, sm_away = st.columns(2)
                with sm_home:
                    _draw_team(str(_sm_row["home"]), int(_sm_row["home_team_id"]))
                with sm_away:
                    _draw_team(str(_sm_row["away"]), int(_sm_row["away_team_id"]))

                if si_view == t("shot_map"):
                    st.caption(
                        "Source: fact_events (WhoScored) · Dot = shot origin · "
                        "Goal / MissedShots / SavedShot / ShotOnPost are WhoScored outcomes · "
                        "On target = Goal + SavedShot · Penalty shootouts excluded"
                    )
                else:
                    st.caption(
                        "Source: fact_events (WhoScored goalMouthY/goalMouthZ) · "
                        "Front view of the goal in real metres (7.32 m × 2.44 m) · "
                        "Dot = where the shot crossed the goal-line plane · "
                        "Penalty shootouts excluded"
                    )

            st.divider()

        else:
            # ── Pitch Danger Heatmap ─────────────────────────────
            st.subheader(t("pitch_danger_heatmap"))

            hm_df = analytics.get_heatmap_data(si_season, _si_team_id, _si_competition_val)

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
                    pitch_color="grass", stripe=True, line_color="white", line_zorder=2,
                )
                fig, ax = pitch.draw(figsize=(12, 7))

                hm_mesh = ax.pcolormesh(
                    x_edges, y_edges, grid,
                    cmap="Reds", alpha=0.75, zorder=1, vmin=0,
                )
                plt.colorbar(hm_mesh, ax=ax, shrink=0.6, label=metric_label)
                ax.set_title(hm_title, fontsize=13, pad=12)

                st.pyplot(fig)
                plt.close(fig)

                with st.expander("Zone data table"):
                    st.dataframe(
                        hm_df[["x_band", "y_band", "shots", "goals", "avg_xg", "conversion_rate"]],
                        width='stretch',
                    )

            st.divider()

        # ── Section 2 — Player Finishing Quality ──────────────────
        st.subheader(t("player_finishing"))
        st.caption("Min. 20 shots to qualify · Goals − xG: positive = overperforming")

        pf_df = analytics.get_player_finishing(si_season, _si_team_id, _si_competition_val)

        if pf_df.empty:
            st.info("No players with 20+ shots for this selection.")
        else:
            _pf_sort_asc = st.radio(
                "Sort order", ["Descending", "Ascending"],
                horizontal=True, key="si_finishing_sort",
            ) == "Ascending"
            pf_df = pf_df.sort_values("goals_minus_xg", ascending=_pf_sort_asc)
            goals_minus_xg = pd.to_numeric(
                pf_df["goals_minus_xg"], errors="coerce"
            ).fillna(0)

            bar_colors = [
                "#2ecc71" if v >= 0 else "#e74c3c"
                for v in goals_minus_xg
            ]
            fig2, ax2 = plt.subplots(figsize=(10, max(4, len(pf_df) * 0.45)))
            fig2.patch.set_facecolor("#0e1117")
            ax2.set_facecolor("#0e1117")
            ax2.barh(pf_df["player"], goals_minus_xg, color=bar_colors)
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
                width='stretch',
            )

        st.divider()

        # ── Section 3 — Set-piece Specialists ────────────────────
        st.subheader(t("setpiece_specialists"))

        sp_df = analytics.get_setpiece_goals(si_season, _si_team_id, competition=_si_competition_val)

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
            st.dataframe(display_sp, width='stretch')

            _sp_players = explore.get_players_for_season(si_season, _si_team_id, _si_competition_val)
            _sp_labels = ["All players"] + [name for name, _ in _sp_players]
            _sp_id_map = {name: pid for name, pid in _sp_players}

            si_player_name = st.selectbox(
                "Player drill-down", _sp_labels, key="si_player"
            )
            si_player_id = _sp_id_map.get(si_player_name)

            if si_player_id is not None:
                bucket_df = analytics.get_setpiece_goals(
                    si_season, _si_team_id, player_id=si_player_id,
                    competition=_si_competition_val,
                )
                if not bucket_df.empty:
                    st.bar_chart(bucket_df.set_index("situation_bucket")["goals"])

            st.caption(
                "Source: fact_shots (all sources) · "
                "Penalty = situation 'penalty' · "
                "Free Kick = 'direct freekick' / 'free-kick'"
            )

        st.divider()
