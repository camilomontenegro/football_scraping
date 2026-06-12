"""
dashboard/views/players.py
==========================
Players — discipline, goalkeepers, player detail, injuries.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st

from dashboard import explore, player_detail
from dashboard.i18n import t
from dashboard.views.shared import _fmt, _tab_selectors


def render() -> None:
    st.header(t("tab_players"))

    t_discipline, t_gk, t_detail, t_injuries = st.tabs(
        [t("tab_players"), t("tab_goalkeepers"), "Player Detail", t("tab_injuries")]
    )

    # ── Discipline ──────────────────────────────────────────
    with t_discipline:
        _pl_comp, _pl_season, _pl_team = _tab_selectors("players", all_seasons=True)

        df = explore.get_player_discipline(_pl_season, _pl_team, _pl_comp)
        if df.empty:
            st.info("No player data found for this selection.")
        else:
            m1, m2, m3, m4 = st.columns(4)
            m1.metric(t("players_tracked"), df["player"].nunique())
            m2.metric(t("total_goals"), _fmt(df['goals'].sum()))
            m3.metric(t("yellow_cards"), _fmt(df['yellow_cards'].sum()))
            m4.metric(t("red_cards"), _fmt(df['red_cards'].sum()))

            display_df = df.copy()
            if _pl_season is not None:
                display_df = display_df.drop(columns=["season"], errors="ignore")

            st.dataframe(display_df, width='stretch')

            if _pl_team is None and not df.empty:
                top10 = df.groupby("player")["goals"].sum().nlargest(10).reset_index()
                if not top10.empty:
                    _pl_sort_asc = st.radio(
                        "Sort order", ["Descending", "Ascending"],
                        horizontal=True, key="pl_top_sort",
                    ) == "Ascending"
                    top10 = top10.sort_values("goals", ascending=_pl_sort_asc)
                    fig_pl, ax_pl = plt.subplots(figsize=(10, max(3, len(top10) * 0.45)))
                    fig_pl.patch.set_facecolor("#0e1117")
                    ax_pl.set_facecolor("#0e1117")
                    ax_pl.barh(top10["player"], top10["goals"], color="#3498db")
                    ax_pl.set_xlabel("Goals", color="white")
                    ax_pl.tick_params(colors="white")
                    for spine in ax_pl.spines.values():
                        spine.set_color("#444")
                    ax_pl.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_pl)
                    plt.close(fig_pl)

            st.caption(
                "Goals and xG: fact_shots (all sources) · Cards: fact_events (SofaScore incidents + StatsBomb)\n"
                "Rows show per-season accumulation when All seasons is selected."
            )

    # ── Goalkeepers ─────────────────────────────────────────
    with t_gk:
        _gk_comp, _gk_season, _gk_team = _tab_selectors("gk")

        if _gk_season is None:
            st.info(t("select_season"))
        else:
            df = explore.get_goalkeeper_stats(_gk_season, _gk_team, _gk_comp)
            if df.empty:
                st.info("No goalkeeper data found for this selection.")
            else:
                gk_count = len(df)
                total_saves = int(df["saves"].sum())
                avg_save_pct = round(float(df["save_pct"].dropna().mean()), 1) if not df["save_pct"].dropna().empty else 0
                total_cs = int(df["clean_sheets"].sum())

                m1, m2, m3, m4 = st.columns(4)
                m1.metric(t("gk_tracked"), gk_count)
                m2.metric(t("total_saves"), _fmt(total_saves))
                m3.metric(t("avg_save_pct"), f"{avg_save_pct:.1f}%")
                m4.metric(t("clean_sheets"), _fmt(total_cs))

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
                st.dataframe(display_df, width='stretch')
                st.caption(
                    "Stats are scoped to matches where each GK appeared in event data (substitutions, cards, etc.) — "
                    "used as a proxy for matches played. "
                    "Shots On Target Faced = goals + saves (blocked/missed excluded) · "
                    "Save % = saves ÷ shots on target × 100 · "
                    "xG Conceded = total expected-goal value of shots faced · "
                    "Goals Saved Above Expected = saves − xG conceded (positive = outperforming)"
                )

    # ── Player Detail ───────────────────────────────────────
    with t_detail:
        try:
            from mplsoccer import Pitch as _PlayerPitch
        except ImportError:
            st.error("Install mplsoccer: pip install mplsoccer")
            st.stop()

        _pd_all = player_detail.get_all_players()
        _pd_search = st.text_input("Search player", key="pd_search", placeholder="Type a name…")
        _pd_filtered = _pd_all[
            _pd_all["canonical_name"].str.contains(_pd_search, case=False, na=False)
        ] if _pd_search else _pd_all
        _pd_names = _pd_filtered["canonical_name"].tolist()

        _pd_selected_name = st.selectbox(
            "Select player", options=_pd_names if _pd_names else ["(no match)"],
            key="pd_select", disabled=not _pd_names,
        )

        _pd_row = (
            _pd_filtered[_pd_filtered["canonical_name"] == _pd_selected_name]
            if _pd_names else pd.DataFrame()
        )

        if not _pd_row.empty:
            _pd = _pd_row.iloc[0]
            _pd_cid = int(_pd["canonical_id"])

            # ═══════════════════════════════════════════════════════
            # SECTION 1 — PLAYER CARD (LaLiga-style header)
            # ═══════════════════════════════════════════════════════
            _sm_seasons_all = ["All"] + player_detail.get_player_shot_seasons(_pd_cid)
            _pd_season_sel = st.selectbox(
                "Season", _sm_seasons_all, key="pd_season_global",
            )
            _pd_season_val = None if _pd_season_sel == "All" else _pd_season_sel

            _summary = player_detail.get_player_summary_stats(_pd_cid, _pd_season_sel)

            # ── Header row: photo + ficha técnica ────────────────
            _photo_col, _info_col = st.columns([1, 3])
            with _photo_col:
                _photo = _pd.get("photo_url")
                if pd.notna(_photo) and str(_photo).strip():
                    st.image(str(_photo), width=120)
                else:
                    st.markdown(
                        '<div style="width:120px;height:120px;border-radius:50%;'
                        'background:#2c3e50;display:flex;align-items:center;'
                        'justify-content:center;font-size:2.5em;color:#7f8c8d">'
                        '?</div>',
                        unsafe_allow_html=True,
                    )
            with _info_col:
                # Player name large
                st.markdown(
                    f'<h1 style="margin:0;padding:0;font-size:2em">'
                    f'{_pd["canonical_name"]}</h1>',
                    unsafe_allow_html=True,
                )
                # Team badge
                _team_name = _summary.get("team") or "—"
                st.markdown(
                    f'<span style="font-size:1.1em;color:#aaa">{_team_name}</span>',
                    unsafe_allow_html=True,
                )
                # Ficha técnica row
                _ft_items = []
                _bd = _pd["birth_date"]
                if _bd:
                    from datetime import date as _date_cls
                    _age = (_date_cls.today() - _bd).days // 365
                    _ft_items.append(f"**Born:** {_bd.strftime('%d/%m/%Y')} ({_age} yrs)")
                if _pd["nationality"]:
                    _ft_items.append(f"**Nationality:** {_pd['nationality']}")
                if _pd["position"]:
                    _ft_items.append(f"**Position:** {_pd['position']}")
                st.markdown(" · ".join(_ft_items) if _ft_items else "—")

                # Source badges
                _sources_map = {
                    "StatsBomb": _pd["id_statsbomb"], "Understat": _pd["id_understat"],
                    "SofaScore": _pd["id_sofascore"], "Transfermarkt": _pd["id_transfermarkt"],
                    "WhoScored": _pd["id_whoscored"],
                }
                _badge_parts = []
                for _src, _sid in _sources_map.items():
                    _has = _sid is not None and str(_sid) not in ("", "None", "0")
                    _color = "#27ae60" if _has else "#555"
                    _badge_parts.append(
                        f'<span style="background:{_color};color:white;padding:2px 8px;'
                        f'border-radius:4px;margin-right:4px;font-size:0.75em">{_src}</span>'
                    )
                st.markdown(" ".join(_badge_parts), unsafe_allow_html=True)

            st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 2 — STATS GRID (LaLiga-style numbers)
            # ═══════════════════════════════════════════════════════
            _season_label = _pd_season_sel if _pd_season_sel != "All" else "All seasons"
            st.markdown(
                f'<h3 style="margin-bottom:0.3em">Statistics — {_season_label}</h3>',
                unsafe_allow_html=True,
            )

            def _stat_card(label: str, value, col):
                """Render a single stat as a large number + label."""
                col.markdown(
                    f'<div style="text-align:center;padding:8px 0">'
                    f'<div style="font-size:2.2em;font-weight:700;color:#f0f0f0">{value}</div>'
                    f'<div style="font-size:0.85em;color:#999">{label}</div></div>',
                    unsafe_allow_html=True,
                )

            # Row 1: Goals · Shots · xG · Matches
            _r1c1, _r1c2, _r1c3, _r1c4 = st.columns(4)
            _stat_card(t("goals"),   _summary["goals"],   _r1c1)
            _stat_card("Shots",      _summary["shots"],   _r1c2)
            _stat_card("xG",        f'{_summary["xg"]:.2f}', _r1c3)
            _stat_card(t("matches"), _summary["matches"], _r1c4)

            # Row 2: Penalties · Penalty Goals · Yellow · Red
            _r2c1, _r2c2, _r2c3, _r2c4 = st.columns(4)
            _stat_card("Penalties",      _summary["penalties"],     _r2c1)
            _stat_card("Penalty Goals",  _summary["penalty_goals"], _r2c2)
            _stat_card(t("yellow_cards"), _summary["yellows"],      _r2c3)
            _stat_card(t("red_cards"),    _summary["reds"],         _r2c4)

            # Row 3: Derived metrics
            _conv = round(
                (_summary["goals"] / _summary["shots"] * 100) if _summary["shots"] else 0, 1
            )
            _g_minus_xg_total = round(_summary["goals"] - _summary["xg"], 2)
            _gpm = round(
                _summary["goals"] / _summary["matches"] if _summary["matches"] else 0, 2
            )
            _r3c1, _r3c2, _r3c3, _r3c4 = st.columns(4)
            _stat_card("Conversion %", f"{_conv}%", _r3c1)
            _stat_card("Goals − xG",   f"{_g_minus_xg_total:+.2f}", _r3c2)
            _stat_card("Goals/Match",   f"{_gpm:.2f}", _r3c3)
            _r3c4.write("")  # empty cell

            st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 3 — RADAR CHART (player vs league / another player)
            # ═══════════════════════════════════════════════════════
            _comp_info = player_detail.get_player_primary_competition(_pd_cid, _pd_season_sel)
            _p_vals = player_detail._player_radar_row(_pd_cid, _pd_season_sel,
                        _comp_info[0] if _comp_info else None) if _comp_info else None

            if _p_vals is not None and _comp_info is not None:
                _comp_id_radar, _comp_name = _comp_info
                _labels = player_detail._RADAR_METRICS

                # ── Compare-to selector ──────────────────────────
                _cmp_col1, _cmp_col2 = st.columns([1, 2])
                with _cmp_col1:
                    _cmp_mode = st.radio(
                        "Compare against",
                        [f"{_comp_name} average", "Another player"],
                        key="pd_cmp_mode",
                        horizontal=True,
                    )

                _cmp_label = f"{_comp_name} avg"
                _cmp_vals = None

                if _cmp_mode == "Another player":
                    with _cmp_col2:
                        _cmp_search = st.text_input(
                            "Search rival", key="pd_cmp_search", placeholder="Type a name…"
                        )
                        _cmp_candidates = _pd_all[
                            (_pd_all["canonical_name"].str.contains(_cmp_search, case=False, na=False))
                            & (_pd_all["canonical_id"] != _pd_cid)
                        ] if _cmp_search else _pd_all[_pd_all["canonical_id"] != _pd_cid]
                        _cmp_names = _cmp_candidates["canonical_name"].tolist()
                        _cmp_selected = st.selectbox(
                            "Select player to compare",
                            _cmp_names if _cmp_names else ["(no match)"],
                            key="pd_cmp_select",
                            disabled=not _cmp_names,
                        )
                    if _cmp_names and _cmp_selected != "(no match)":
                        _cmp_row = _cmp_candidates[
                            _cmp_candidates["canonical_name"] == _cmp_selected
                        ]
                        if not _cmp_row.empty:
                            _cmp_cid = int(_cmp_row.iloc[0]["canonical_id"])
                            _cmp_vals = player_detail._player_radar_row(
                                _cmp_cid, _pd_season_sel,
                                competition_id=_comp_id_radar,
                            )
                            _cmp_label = _cmp_selected
                else:
                    _cmp_vals = player_detail.get_league_avg_radar(
                        _comp_id_radar, _pd_season_sel, exclude_player=_pd_cid,
                    )

                if _cmp_vals is not None:
                    st.subheader(f"{_pd['canonical_name']} vs {_cmp_label}")
                    _n = len(_labels)

                    # ── Normalise each axis independently (0-100) ──
                    # Use max of both + 20% headroom so neither touches the edge
                    _axis_max = [
                        max(abs(_p_vals[i]), abs(_cmp_vals[i]), 1e-9) * 1.2
                        for i in range(_n)
                    ]
                    _p_norm = [(_p_vals[i] / _axis_max[i]) * 100 for i in range(_n)]
                    _c_norm = [(_cmp_vals[i] / _axis_max[i]) * 100 for i in range(_n)]

                    _angles = np.linspace(0, 2 * np.pi, _n, endpoint=False).tolist()
                    _p_norm += [_p_norm[0]]
                    _c_norm += [_c_norm[0]]
                    _angles_closed = _angles + [_angles[0]]

                    _fig_r, _ax_r = plt.subplots(
                        figsize=(5, 5), subplot_kw=dict(polar=True),
                    )
                    _fig_r.patch.set_facecolor("#0e1117")
                    _ax_r.set_facecolor("#0e1117")

                    # Concentric reference rings
                    _ring_vals = [25, 50, 75, 100]
                    for _rv in _ring_vals:
                        _ax_r.plot(
                            _angles_closed,
                            [_rv] * (_n + 1),
                            color="#333", linewidth=0.4, linestyle="-", zorder=0,
                        )

                    # Player 1 (red)
                    _ax_r.plot(
                        _angles_closed, _p_norm, "o-",
                        linewidth=2.2, color="#e74c3c", markersize=6,
                        label=_pd["canonical_name"], zorder=3,
                    )
                    _ax_r.fill(_angles_closed, _p_norm, alpha=0.20, color="#e74c3c")

                    # Player 2 / League avg (blue)
                    _ax_r.plot(
                        _angles_closed, _c_norm, "o-",
                        linewidth=2.2, color="#3498db", markersize=6,
                        label=_cmp_label, zorder=3,
                    )
                    _ax_r.fill(_angles_closed, _c_norm, alpha=0.20, color="#3498db")

                    # Value annotations next to each vertex
                    _fmt_val = lambda v, i: (
                        f"{v:.1f}%" if _labels[i] == "Conversion %" else f"{v:.2f}"
                    )
                    for i in range(_n):
                        _offset_r = max(_p_norm[i], _c_norm[i]) + 12
                        _ax_r.text(
                            _angles[i], _offset_r,
                            f"{_fmt_val(_p_vals[i], i)}",
                            ha="center", va="center",
                            fontsize=7.5, fontweight="bold", color="#e74c3c",
                        )
                        _ax_r.text(
                            _angles[i], _offset_r + 9,
                            f"{_fmt_val(_cmp_vals[i], i)}",
                            ha="center", va="center",
                            fontsize=7.5, fontweight="bold", color="#3498db",
                        )

                    # Axis labels
                    _ax_r.set_xticks(_angles)
                    _ax_r.set_xticklabels(
                        _labels, color="white", fontsize=10, fontweight="600",
                    )
                    _ax_r.set_yticklabels([])  # hide radial ticks
                    _ax_r.set_ylim(0, 130)     # room for annotations
                    _ax_r.spines["polar"].set_color("#444")
                    _ax_r.grid(color="#444", linewidth=0.3)

                    _ax_r.legend(
                        loc="upper center", bbox_to_anchor=(0.5, -0.06),
                        ncol=2, frameon=True,
                        facecolor="#1a1a2e", edgecolor="#444",
                        labelcolor="white", fontsize=10,
                    )
                    _fig_r.tight_layout()

                    _rc1, _rc2 = st.columns([3, 2])
                    with _rc1:
                        st.pyplot(_fig_r)
                        plt.close(_fig_r)
                    with _rc2:
                        _rv_df = pd.DataFrame({
                            "Metric": _labels,
                            _pd["canonical_name"]: [
                                _fmt_val(_p_vals[i], i) for i in range(_n)
                            ],
                            _cmp_label: [
                                _fmt_val(_cmp_vals[i], i) for i in range(_n)
                            ],
                        })
                        st.dataframe(_rv_df, width="stretch", hide_index=True)
                        _caption = (
                            "Per-match averages. Conversion % is per-shot."
                            if _cmp_mode == "Another player"
                            else "Per-match averages vs league (excluding this player). "
                                 "Conversion % is per-shot."
                        )
                        st.caption(_caption)
                else:
                    if _cmp_mode == "Another player":
                        st.info("No shot data for this player in the same competition/season.")
                    else:
                        st.info("Not enough league data to compute average.")

                st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 4 — SHOT MAP  (kept from before)
            # ═══════════════════════════════════════════════════════
            st.subheader("Shot Map")
            _sm_sources = ["All"] + player_detail.get_player_shot_sources(_pd_cid)
            _smc1, _smc2 = st.columns(2)
            with _smc1:
                _sm_source = st.selectbox("Source", _sm_sources, key="pd_sm_source")
            _sm_matches_df = player_detail.get_player_shot_matches(
                _pd_cid, _pd_season_sel, _sm_source
            )
            _sm_match_options = {"All": None}
            for _match in _sm_matches_df.itertuples(index=False):
                _date = (
                    _match.match_date.strftime("%Y-%m-%d")
                    if pd.notna(_match.match_date) else "Unknown date"
                )
                _home = _match.home_team or "Home"
                _away = _match.away_team or "Away"
                _score = (
                    f" {_match.home_score}-{_match.away_score}"
                    if pd.notna(_match.home_score) and pd.notna(_match.away_score) else ""
                )
                _comp = f" | {_match.competition}" if _match.competition else ""
                _shots_label = f" | {_match.shots} shots"
                _label = f"{_date} | {_home}{_score} {_away}{_comp}{_shots_label}"
                _sm_match_options[_label] = int(_match.match_id)
            with _smc2:
                _sm_match_label = st.selectbox(
                    "Match",
                    list(_sm_match_options.keys()),
                    key="pd_sm_match",
                    disabled=_sm_matches_df.empty,
                )
            _sm_match_id = _sm_match_options.get(_sm_match_label)

            _shots_df = player_detail.get_player_shots(
                _pd_cid, _pd_season_sel, _sm_source, _sm_match_id
            )
            if _shots_df.empty:
                st.info("No shot data found for this selection.")
            else:
                _pitch = _PlayerPitch(
                    pitch_type="custom",
                    pitch_length=105,
                    pitch_width=68,
                    pitch_color="grass",
                    stripe=True,
                    line_color="white",
                    line_zorder=2,
                )
                _fig_sm, _ax_sm = _pitch.draw(figsize=(7, 4.5))
                _x      = _shots_df["x"].to_numpy(dtype=float, na_value=np.nan)
                _y      = _shots_df["y"].to_numpy(dtype=float, na_value=np.nan)
                _xg_arr = _shots_df["xg"].fillna(0.05).to_numpy(dtype=float)
                _sizes  = np.clip(_xg_arr * 300, 20, 200)
                _is_goal = (_shots_df["result"] == "Goal").to_numpy()
                _pitch.scatter(_x[~_is_goal], _y[~_is_goal], s=_sizes[~_is_goal],
                    color="white", edgecolors="#666666", linewidths=0.4, alpha=0.75,
                    ax=_ax_sm, zorder=3, label="No goal")
                _pitch.scatter(_x[_is_goal], _y[_is_goal], s=_sizes[_is_goal],
                    color="red", edgecolors="white", linewidths=0.5, alpha=0.95,
                    ax=_ax_sm, zorder=4, label="Goal")
                _ax_sm.legend(loc="upper right", fontsize=8, framealpha=0.85)
                st.pyplot(_fig_sm)
                plt.close(_fig_sm)
                _st1, _st2, _st3, _st4 = st.columns(4)
                _total_shots = len(_shots_df)
                _total_goals = int(_is_goal.sum())
                _total_xg = round(float(_xg_arr.sum()), 2)
                _g_minus_xg = round(_total_goals - _total_xg, 2)
                _st1.metric("Shots", _total_shots)
                _st2.metric("Goals", _total_goals)
                _st3.metric("xG", _total_xg)
                _st4.metric("Goals − xG", f"{_g_minus_xg:+.2f}")
            st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 5 — SEASONAL STATS + INJURIES + MDM
            # ═══════════════════════════════════════════════════════
            st.subheader("Seasonal Stats")
            _ss_df = player_detail.get_player_seasonal_stats(_pd_cid)
            if _ss_df.empty:
                st.info("No shot data available for this player.")
            else:
                _ss_df.columns = ["Season", "Competition", "Shots", "Goals", "xG"]
                st.dataframe(_ss_df, width="stretch", hide_index=True)
            st.divider()

            st.subheader("Injury History")
            _inj_df = player_detail.get_player_injuries(_pd_cid)
            if _inj_df.empty:
                st.info("No injury records found for this player.")
            else:
                _inj_df.columns = ["Season", "Injury type", "Date from", "Date until",
                                    "Days absent", "Matches missed"]
                st.dataframe(_inj_df, width="stretch", hide_index=True)
            st.divider()

            with st.expander("Source Identity (MDM)"):
                _mdm_df = player_detail.get_player_mdm(_pd_cid)
                if _mdm_df.empty:
                    st.info("No source aliases recorded for this player.")
                else:
                    _mdm_df.columns = ["Source", "Name used", "Source ID", "Score", "Resolved"]
                    st.dataframe(_mdm_df, width="stretch", hide_index=True)

    # ── Injuries (aggregate) ────────────────────────────────
    with t_injuries:
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
            m1.metric(t("total_injuries"), _fmt(total_inj))
            m2.metric(t("total_days_absent"), _fmt(total_days))
            m3.metric(t("total_matches_missed"), _fmt(total_missed))
            m4.metric(t("ongoing_injuries"), _fmt(ongoing))

            df_render = df.copy()
            df_render["date_until"] = df_render["date_until"].fillna("Ongoing").astype(str)
            st.dataframe(df_render, width='stretch')

            breakdown = explore.get_injury_type_breakdown(_inj_season, _inj_team)
            if not breakdown.empty:
                st.subheader(t("top_injury_types"))
                _inj_sort_asc = st.radio(
                    "Sort order", ["Descending", "Ascending"],
                    horizontal=True, key="inj_type_sort",
                ) == "Ascending"
                breakdown = breakdown.sort_values("count", ascending=_inj_sort_asc).head(10)
                fig_inj, ax_inj = plt.subplots(figsize=(10, max(3, len(breakdown) * 0.45)))
                fig_inj.patch.set_facecolor("#0e1117")
                ax_inj.set_facecolor("#0e1117")
                ax_inj.barh(breakdown["injury_type"], breakdown["count"], color="#e67e22")
                ax_inj.set_xlabel("Count", color="white")
                ax_inj.tick_params(colors="white")
                for spine in ax_inj.spines.values():
                    spine.set_color("#444")
                ax_inj.invert_yaxis()
                plt.tight_layout()
                st.pyplot(fig_inj)
                plt.close(fig_inj)

            if _inj_season is None:
                trend = explore.get_injury_season_trend(_inj_team)
                if not trend.empty:
                    st.subheader(t("season_trend"))
                    st.dataframe(trend, width='stretch')

            st.caption(
                "Source: fact_injuries (Transfermarkt)\n"
                "date_until = NULL means the player was still injured at time of data collection."
            )
