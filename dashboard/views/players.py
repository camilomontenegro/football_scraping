"""
dashboard/views/players.py
==========================
Players — discipline, goalkeepers, player detail, injuries,
market value and transfer history.
 
Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations
 
from datetime import date as _date_cls
 
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta
from matplotlib.lines import Line2D
import plotly.graph_objects as go
 
from dashboard import explore, player_detail
from dashboard.i18n import t
from dashboard.views.shared import (
    _fmt,
    _fmt_eur,
    _fmt_team_history_date_to,
    _generate_team_colors,
    _tab_selectors,
)
 
 
def render() -> None:
    st.header(t("tab_players"))
 
    t_discipline, t_cards, t_gk, t_detail, t_injuries, t_market_value, t_transfer_history = st.tabs(
        [
            t("tab_players"),
            t("cards_fouls_section"),
            t("tab_goalkeepers"),
            "Player Detail",
            t("tab_injuries"),
            t("tab_market_value"),
            t("tab_transfer_history"),
        ]
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
            m2.metric(t("total_goals"), _fmt(df["goals"].sum()))
            m3.metric(t("yellow_cards"), _fmt(df["yellow_cards"].sum()))
            m4.metric(t("red_cards"), _fmt(df["red_cards"].sum()))
 
            display_df = df.copy()
            if _pl_season is not None:
                display_df = display_df.drop(columns=["season"], errors="ignore")
 
            st.dataframe(display_df, width="stretch")
 
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

    # ── Cards & fouls ───────────────────────────────────────
    with t_cards:
        _cf_comp, _cf_season, _cf_team = _tab_selectors("cards_fouls")
        if _cf_season is None:
            st.info(t("select_season"))
        else:
            _cf_min = st.slider(t("min_matches"), 1, 20, 3, key="cf_min_matches")
            _df_cf = explore.get_player_cards_fouls(
                _cf_season, _cf_team, _cf_comp, _cf_min
            )
            if _df_cf.empty:
                st.info(t("no_data"))
            else:
                _cf1, _cf2, _cf3, _cf4 = st.columns(4)
                _cf1.metric(t("players_tracked"), _df_cf["player"].nunique())
                _cf2.metric(t("yellow_cards"), _fmt(_df_cf["yellow_cards"].sum()))
                _cf3.metric(t("red_cards"), _fmt(_df_cf["red_cards"].sum()))
                _cf4.metric(t("fouls"), _fmt(_df_cf["fouls"].sum()))

                st.dataframe(
                    _df_cf.rename(columns={
                        "player": t("col_player"), "team": t("team"), "matches": t("col_matches"),
                        "yellow_cards": t("yellow_cards"), "red_cards": t("red_cards"),
                        "total_cards": t("total_cards"),
                        "cards_per_match": t("cards_per_match"),
                        "fouls": t("fouls"), "fouls_per_match": t("fouls_per_match"),
                    }),
                    width="stretch", hide_index=True,
                )

                _topc = _df_cf[_df_cf["matches"] >= _cf_min].sort_values(
                    "cards_per_match", ascending=False
                ).head(15)
                if not _topc.empty:
                    st.subheader(t("cards_per_match"))
                    fig_cf, ax_cf = plt.subplots(figsize=(10, max(3, len(_topc) * 0.45)))
                    fig_cf.patch.set_facecolor("#0e1117")
                    ax_cf.set_facecolor("#0e1117")
                    _cf_lbl = [f"{r.player} ({r.team})" for r in _topc.itertuples()]
                    _cf_y = _topc["yellow_cards"].to_numpy(float) / _topc["matches"].to_numpy(float)
                    _cf_r = _topc["red_cards"].to_numpy(float) / _topc["matches"].to_numpy(float)
                    ax_cf.barh(_cf_lbl, _cf_y, color="#f1c40f", label=t("yellow_cards"))
                    ax_cf.barh(_cf_lbl, _cf_r, left=_cf_y, color="#e74c3c", label=t("red_cards"))
                    ax_cf.set_xlabel(t("cards_per_match"), color="white")
                    ax_cf.tick_params(colors="white")
                    ax_cf.legend(facecolor="#1a1a2e", edgecolor="#444",
                                 labelcolor="white", fontsize=9)
                    for _sp in ax_cf.spines.values():
                        _sp.set_color("#444")
                    ax_cf.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_cf)
                    plt.close(fig_cf)

                st.caption(t("caption_cards_fouls"))

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
                st.dataframe(display_df, width="stretch")
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
                        "?</div>",
                        unsafe_allow_html=True,
                    )
 
            with _info_col:
                # Player name with active/retired badge
                _is_retired = player_detail.is_player_retired(_pd_cid)
                _status_label = "Retired" if _is_retired else "Active"
                _status_color = "#e74c3c" if _is_retired else "#2ecc71"
                st.markdown(
                    f'<h1 style="margin:0;padding:0;font-size:2em">'
                    f'{_pd["canonical_name"]} '
                    f'<span style="font-size:0.45em;background:{_status_color};color:white;'
                    f'padding:2px 8px;border-radius:4px;vertical-align:middle">{_status_label}</span>'
                    f"</h1>",
                    unsafe_allow_html=True,
                )
 
                # current team from fact_transfers — shown below the name
                _current_team_df = player_detail.get_player_team_history(_pd_cid, all_time=True)
                if not _current_team_df.empty and "team" in _current_team_df.columns:
                    _current_team_df = _current_team_df[_current_team_df["team"] != "Retirado"]
                    _team_name = _current_team_df.iloc[0]["team"] if not _current_team_df.empty else "—"
                else:
                    _team_name = "—"
                st.markdown(
                    f'<span style="font-size:1.1em;color:#aaa">{_team_name}</span>',
                    unsafe_allow_html=True,
                )
 
                # Ficha técnica row — born, nationality, position
                _ft_items = []
                _bd = _pd["birth_date"]
                if _bd:
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
 
            # ── Career History ────────────────────────────────────
            # shows clubs the player has been at, filtered by season if one is selected
            # uses LEAD() to compute periods — date_to NULL means still at that club
            st.subheader(t("tab_career_history"))
            _team_history = player_detail.get_player_team_history(
                _pd_cid,
                season=_pd_season_val,
                all_time=(_pd_season_val is None),
            )
            if _team_history.empty:
                st.caption(t("career_no_data"))
            else:
                _team_history["date_from"] = pd.to_datetime(
                    _team_history["date_from"], errors="coerce"
                ).dt.strftime("%d/%m/%Y")
                # format date_to — NULL means Present or Retired depending on team
                _team_history["date_to"] = _team_history.apply(_fmt_team_history_date_to, axis=1)
                # remove Retirado rows — retirement is shown in the player name badge
                _team_history = _team_history[_team_history["team"] != "Retirado"]
                _team_history.columns = [
                    "Season", t("career_date_from"), t("career_date_to"), t("career_team")
                ]
                st.dataframe(_team_history, width="stretch", hide_index=True)
 
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
            _stat_card("Penalties",       _summary["penalties"],     _r2c1)
            _stat_card("Penalty Goals",   _summary["penalty_goals"], _r2c2)
            _stat_card(t("yellow_cards"), _summary["yellows"],       _r2c3)
            _stat_card(t("red_cards"),    _summary["reds"],          _r2c4)
 
            # Row 3: Derived metrics
            _conv = round(
                (_summary["goals"] / _summary["shots"] * 100) if _summary["shots"] else 0, 1
            )
            _g_minus_xg_total = round(_summary["goals"] - _summary["xg"], 2)
            _gpm = round(
                _summary["goals"] / _summary["matches"] if _summary["matches"] else 0, 2
            )
            _r3c1, _r3c2, _r3c3, _r3c4 = st.columns(4)
            _stat_card("Conversion %", f"{_conv}%",              _r3c1)
            _stat_card("Goals − xG",   f"{_g_minus_xg_total:+.2f}", _r3c2)
            _stat_card("Goals/Match",   f"{_gpm:.2f}",           _r3c3)
            _r3c4.write("")  # empty cell
 
            st.divider()
 
            # ═══════════════════════════════════════════════════════
            # SECTION 3 — RADAR CHART (player vs league / another player)
            # ═══════════════════════════════════════════════════════
            _comp_info = player_detail.get_player_primary_competition(_pd_cid, _pd_season_sel)
            _p_vals = player_detail._player_radar_row(
                _pd_cid, _pd_season_sel,
                _comp_info[0] if _comp_info else None,
            ) if _comp_info else None
 
            if _p_vals is not None and _comp_info is not None:
                _comp_id_radar, _comp_name = _comp_info
                _labels = player_detail._RADAR_METRICS
 
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
 
                    _fig_r, _ax_r = plt.subplots(figsize=(5, 5), subplot_kw=dict(polar=True))
                    _fig_r.patch.set_facecolor("#0e1117")
                    _ax_r.set_facecolor("#0e1117")
 
                    for _rv in [25, 50, 75, 100]:
                        _ax_r.plot(
                            _angles_closed, [_rv] * (_n + 1),
                            color="#333", linewidth=0.4, linestyle="-", zorder=0,
                        )
 
                    _ax_r.plot(
                        _angles_closed, _p_norm, "o-",
                        linewidth=2.2, color="#e74c3c", markersize=6,
                        label=_pd["canonical_name"], zorder=3,
                    )
                    _ax_r.fill(_angles_closed, _p_norm, alpha=0.20, color="#e74c3c")
 
                    _ax_r.plot(
                        _angles_closed, _c_norm, "o-",
                        linewidth=2.2, color="#3498db", markersize=6,
                        label=_cmp_label, zorder=3,
                    )
                    _ax_r.fill(_angles_closed, _c_norm, alpha=0.20, color="#3498db")
 
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
 
                    _ax_r.set_xticks(_angles)
                    _ax_r.set_xticklabels(_labels, color="white", fontsize=10, fontweight="600")
                    _ax_r.set_yticklabels([])
                    _ax_r.set_ylim(0, 130)
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
                            _pd["canonical_name"]: [_fmt_val(_p_vals[i], i) for i in range(_n)],
                            _cmp_label: [_fmt_val(_cmp_vals[i], i) for i in range(_n)],
                        })
                        st.dataframe(_rv_df, width="stretch", hide_index=True)
                        st.caption(
                            "Per-match averages. Conversion % is per-shot."
                            if _cmp_mode == "Another player"
                            else "Per-match averages vs league (excluding this player). Conversion % is per-shot."
                        )
                else:
                    if _cmp_mode == "Another player":
                        st.info("No shot data for this player in the same competition/season.")
                    else:
                        st.info("Not enough league data to compute average.")
 
                st.divider()
 
            # ═══════════════════════════════════════════════════════
            # SECTION 4 — SHOT MAP
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

            # ── Action heatmap (WhoScored events) ─────────────────
            st.subheader(t("action_heatmap"))
            _hm_loc = player_detail.get_player_event_locations(_pd_cid, _pd_season_sel)
            if _hm_loc.empty:
                st.info(t("no_event_data"))
            else:
                _hx = pd.to_numeric(_hm_loc["x"], errors="coerce").to_numpy(dtype=float)
                _hy = pd.to_numeric(_hm_loc["y"], errors="coerce").to_numpy(dtype=float)
                _pitch_hm = _PlayerPitch(
                    pitch_type="custom", pitch_length=105, pitch_width=68,
                    pitch_color="#1a472a", line_color="white", line_zorder=2,
                )
                _fig_hm, _ax_hm = _pitch_hm.draw(figsize=(7, 4.5))
                _fig_hm.patch.set_facecolor("#1a472a")
                try:
                    _pitch_hm.kdeplot(
                        _hx, _hy, ax=_ax_hm, fill=True, levels=50,
                        thresh=0.05, cmap="hot", alpha=0.7, zorder=1,
                    )
                except Exception:
                    _pitch_hm.hexbin(
                        _hx, _hy, ax=_ax_hm, gridsize=20, cmap="hot", zorder=1,
                    )
                st.pyplot(_fig_hm)
                plt.close(_fig_hm)
                st.caption(t("caption_action_heatmap").format(n=len(_hm_loc)))
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
            st.dataframe(df_render, width="stretch")
 
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
                    st.dataframe(trend, width="stretch")
 
            st.caption(
                "Source: fact_injuries (Transfermarkt)\n"
                "date_until = NULL means the player was still injured at time of data collection."
            )
 
    # ── Market Value ─────────────────────────────────────────
    with t_market_value:
        st.subheader(t("mv_title"))
 
        _mv_all_players = player_detail.get_all_players()
        _mv_search = st.text_input("Search player", key="mv_search", placeholder="Type a name…")
 
        if _mv_search:
            _mv_filtered = _mv_all_players[
                _mv_all_players["canonical_name"].str.contains(_mv_search, case=False, na=False)
            ]
        else:
            _mv_filtered = _mv_all_players
 
        _mv_names = _mv_filtered["canonical_name"].tolist()
        _mv_selected = st.selectbox(
            "Select player",
            options=_mv_names if _mv_names else ["(no match)"],
            key="mv_select",
            disabled=not _mv_names,
        )
 
        if _mv_names:
            _mv_row = _mv_filtered[_mv_filtered["canonical_name"] == _mv_selected]
        else:
            _mv_row = pd.DataFrame()
 
        if _mv_row.empty:
            st.info("No player selected.")
        else:
            _mv_player = _mv_row.iloc[0]
            _mv_cid    = int(_mv_player["canonical_id"])
            _mv_pos    = _mv_player.get("position")
            _mv_bdate  = _mv_player.get("birth_date")
 
            _mv_history   = player_detail.get_market_value_history(_mv_cid)
            _mv_transfers = player_detail.get_transfer_history(_mv_cid)
            _mv_injuries  = player_detail.get_player_injuries(_mv_cid)
            _mv_kpis      = player_detail.get_market_value_kpis(_mv_cid)
 
            if _mv_history.empty:
                st.info(t("mv_no_data"))
            else:
                _mv_history["value_date"] = pd.to_datetime(_mv_history["value_date"])
 
                # ── Market value table for main player ───────────
                with st.expander(f"Market value data — {_mv_selected}"):
                    _mv_display = _mv_history[["value_date", "market_value", "club_name"]].copy()
                    _mv_display["value_date"] = _mv_display["value_date"].dt.strftime("%d/%m/%Y")
                    if _mv_bdate is not None:
                        _mv_display["age"] = _mv_history["value_date"].apply(
                            lambda d: relativedelta(d, pd.Timestamp(_mv_bdate)).years
                        )
                        _mv_display["age"] = _mv_display["age"].astype(str)
                        _mv_display = _mv_display[["value_date", "age", "market_value", "club_name"]]
                    _mv_display["market_value"] = _mv_display["market_value"].apply(_fmt_eur)
                    if _mv_bdate is not None:
                        _mv_display.columns = ["Date", "Age", "Market Value", "Club"]
                    else:
                        _mv_display.columns = ["Date", "Market Value", "Club"]
                    st.dataframe(_mv_display, width="stretch", hide_index=True)
 
                # ── KPI cards ─────────────────────────────────
                k1, k2, k3, k4, k5, k6 = st.columns(6)
                k1.metric(t("mv_current_value"), _fmt_eur(_mv_kpis["current_value"]))
                k2.metric(
                    t("mv_peak_value"),
                    _fmt_eur(_mv_kpis["peak_value"]),
                    delta=_mv_kpis["peak_date"].strftime("%b %Y")
                    if _mv_kpis["peak_date"] is not None else None,
                    delta_color="off",
                )
                k3.metric(
                    t("mv_from_peak"),
                    f"{_mv_kpis['pct_from_peak']:.1f}%"
                    if _mv_kpis["pct_from_peak"] is not None else "—",
                    delta=_fmt_eur(_mv_kpis["current_value"] - _mv_kpis["peak_value"])
                    if _mv_kpis["peak_value"] else None,
                    delta_color="inverse",
                )
                k4.metric(
                    t("mv_last_year_change"),
                    _fmt_eur(_mv_kpis["change_last_year"]),
                    delta_color="normal" if (_mv_kpis["change_last_year"] or 0) >= 0 else "inverse",
                )
                k5.metric(t("mv_transfers"), _mv_kpis["num_transfers"])
                k6.metric(t("position"), _mv_pos or "—")
 
                st.divider()
 
                # ── Comparison player selector (optional) ─────
                _mv_cmp_search = st.text_input(
                    t("mv_compare_player"), key="mv_cmp_search", placeholder="Type a name…"
                )
                if _mv_cmp_search:
                    _mv_cmp_filtered = _mv_all_players[
                        (_mv_all_players["canonical_name"].str.contains(_mv_cmp_search, case=False, na=False))
                        & (_mv_all_players["canonical_id"] != _mv_cid)
                    ]
                else:
                    _mv_cmp_filtered = pd.DataFrame()
 
                _mv_cmp_history   = pd.DataFrame()
                _mv_cmp_transfers = pd.DataFrame()
                _mv_cmp_injuries  = pd.DataFrame()
                _mv_cmp_name      = None
 
                if not _mv_cmp_filtered.empty:
                    _mv_cmp_names    = _mv_cmp_filtered["canonical_name"].tolist()
                    _mv_cmp_selected = st.selectbox(
                        t("mv_select_comparison"), options=_mv_cmp_names, key="mv_cmp_select"
                    )
                    _mv_cmp_row = _mv_cmp_filtered[
                        _mv_cmp_filtered["canonical_name"] == _mv_cmp_selected
                    ]
                    if not _mv_cmp_row.empty:
                        _mv_cmp_cid  = int(_mv_cmp_row.iloc[0]["canonical_id"])
                        _mv_cmp_name = _mv_cmp_selected
 
                        _mv_cmp_history   = player_detail.get_market_value_history(_mv_cmp_cid)
                        _mv_cmp_transfers = player_detail.get_transfer_history(_mv_cmp_cid)
                        _mv_cmp_injuries  = player_detail.get_player_injuries(_mv_cmp_cid)
 
                        if not _mv_cmp_history.empty:
                            _mv_cmp_history["value_date"] = pd.to_datetime(_mv_cmp_history["value_date"])
 
                        if not _mv_cmp_history.empty:
                            _mv_cmp_bdate = _mv_cmp_row.iloc[0].get("birth_date")
 
                            with st.expander(f"Market value data — {_mv_cmp_selected}"):
                                _mv_cmp_display = _mv_cmp_history[["value_date", "market_value", "club_name"]].copy()
                                _mv_cmp_display["value_date"] = _mv_cmp_display["value_date"].dt.strftime("%d/%m/%Y")
                                if _mv_cmp_bdate is not None:
                                    _mv_cmp_display["age"] = _mv_cmp_history["value_date"].apply(
                                        lambda d: relativedelta(d, pd.Timestamp(_mv_cmp_bdate)).years
                                    )
                                    _mv_cmp_display["age"] = _mv_cmp_display["age"].astype(str)
                                    _mv_cmp_display = _mv_cmp_display[["value_date", "age", "market_value", "club_name"]]
                                _mv_cmp_display["market_value"] = _mv_cmp_display["market_value"].apply(_fmt_eur)
                                if _mv_cmp_bdate is not None:
                                    _mv_cmp_display.columns = ["Date", "Age", "Market Value", "Club"]
                                else:
                                    _mv_cmp_display.columns = ["Date", "Market Value", "Club"]
                                st.dataframe(_mv_cmp_display, width="stretch", hide_index=True)
 
                            _mv_cmp_kpis = player_detail.get_market_value_kpis(_mv_cmp_cid)
                            ck1, ck2, ck3, ck4, ck5, ck6 = st.columns(6)
                            ck1.metric(t("mv_current_value"), _fmt_eur(_mv_cmp_kpis["current_value"]))
                            ck2.metric(
                                t("mv_peak_value"),
                                _fmt_eur(_mv_cmp_kpis["peak_value"]),
                                delta=_mv_cmp_kpis["peak_date"].strftime("%b %Y")
                                if _mv_cmp_kpis["peak_date"] is not None else None,
                                delta_color="off",
                            )
                            ck3.metric(
                                t("mv_from_peak"),
                                f"{_mv_cmp_kpis['pct_from_peak']:.1f}%"
                                if _mv_cmp_kpis["pct_from_peak"] is not None else "—",
                                delta=_fmt_eur(_mv_cmp_kpis["current_value"] - _mv_cmp_kpis["peak_value"])
                                if _mv_cmp_kpis["peak_value"] else None,
                                delta_color="inverse",
                            )
                            ck4.metric(
                                t("mv_last_year_change"),
                                _fmt_eur(_mv_cmp_kpis["change_last_year"]),
                                delta_color="normal" if (_mv_cmp_kpis["change_last_year"] or 0) >= 0 else "inverse",
                            )
                            ck5.metric(t("mv_transfers"), _mv_cmp_kpis["num_transfers"])
                            ck6.metric(t("position"), _mv_cmp_row.iloc[0].get("position") or "—")
 
                # ── Benchmark band toggle ─────────────────────
                # initialize session state before creating the widget to avoid conflicts
                if "mv_benchmark_toggle" not in st.session_state:
                    st.session_state["mv_benchmark_toggle"] = True

                _mv_show_benchmark = st.checkbox(
                    t("mv_show_benchmark"),
                    key="mv_benchmark_toggle",
                ) if _mv_pos else False
                
                _mv_benchmark = pd.DataFrame()
                if _mv_show_benchmark and _mv_pos:
                    _mv_benchmark = player_detail.get_market_value_benchmark(_mv_pos)
 
                # ── Chart ─────────────────────────────────────
                # Plotly replaces matplotlib here to enable interactive hover
                # on transfer triangles and injury markers.
                fig_mv = go.Figure()

                # layer 1 — benchmark band (P25-P75 shaded area + median dashed line)
                # The benchmark is indexed by age, so we convert age → date using
                # the player's birth_date to align it with the x-axis (dates)
                if not _mv_benchmark.empty and _mv_bdate is not None:
                    _mv_bdate_ts = pd.Timestamp(_mv_bdate)
                    _mv_benchmark["date"] = _mv_bdate_ts + pd.to_timedelta(
                        _mv_benchmark["age"] * 365.25, unit="D"
                    )
                    date_min = _mv_history["value_date"].min()
                    date_max = _mv_history["value_date"].max()
                    _mv_benchmark = _mv_benchmark[
                        (_mv_benchmark["date"] >= date_min) &
                        (_mv_benchmark["date"] <= date_max)
                    ]

                    # P25 line (invisible — used as base for fill)
                    fig_mv.add_trace(go.Scatter(
                        x=_mv_benchmark["date"],
                        y=_mv_benchmark["p25"],
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                    ))
                    # P75 line with fill down to P25
                    fig_mv.add_trace(go.Scatter(
                        x=_mv_benchmark["date"],
                        y=_mv_benchmark["p75"],
                        mode="lines",
                        line=dict(width=0),
                        fill="tonexty",
                        fillcolor="rgba(46,204,113,0.15)",
                        name=f"{_mv_pos} benchmark (P25–P75)",
                        hoverinfo="skip",
                    ))
                    # median dashed line
                    fig_mv.add_trace(go.Scatter(
                        x=_mv_benchmark["date"],
                        y=_mv_benchmark["median"],
                        mode="lines",
                        line=dict(color="#2ecc71", width=1.5, dash="dash"),
                        name=f"{_mv_pos} median",
                        hoverinfo="skip",
                    ))

                    # layer 2 — main player step chart
                    fig_mv.add_trace(go.Scatter(
                        x=_mv_history["value_date"],
                        y=_mv_history["market_value"],
                        mode="lines",
                        line=dict(color="#e74c3c", width=2.2, shape="hv"),
                        name=_mv_selected,
                        hovertemplate=(
                            f"<b style='font-size:15px'>{_mv_selected}</b><br><br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_date')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{x|%d/%m/%Y}}<br>"
                            f"<span style='color:#aaa'>{t('mv_hover_value')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[0]}}<br>"
                            f"<span style='color:#aaa'>{t('mv_hover_club')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[1]}}"
                            "<extra></extra>"
                        ),
                        customdata=list(zip(
                            _mv_history["market_value"].apply(_fmt_eur),
                            _mv_history["club_name"].fillna("—"),
                        )),
                        
                    ))

                # layer 3 — comparison player step chart (no milestone markers)
                if not _mv_cmp_history.empty and _mv_cmp_name:
                    fig_mv.add_trace(go.Scatter(
                        x=_mv_cmp_history["value_date"],
                        y=_mv_cmp_history["market_value"],
                        mode="lines",
                        line=dict(color="#3498db", width=1.8, shape="hv"),
                        name=_mv_cmp_name,
                        hovertemplate=(
                            f"<b style='font-size:15px'>{_mv_cmp_name}</b><br><br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_date')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{x|%d/%m/%Y}}<br>"
                            f"<span style='color:#aaa'>{t('mv_hover_value')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[0]}}<br>"
                            f"<span style='color:#aaa'>{t('mv_hover_club')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[1]}}"
                            "<extra></extra>"
                        ),
                        customdata=list(zip(
                            _mv_cmp_history["market_value"].apply(_fmt_eur),
                            _mv_cmp_history["club_name"].fillna("—"),
                        )),
                    ))

                # layer 4 — transfer milestones (triangles with hover)
                # purple = permanent transfer, orange = loan/end_of_loan, green = free
                # unknown transfers are not shown — no economic information
                TRANSFER_COLORS = {
                    "transfer":    "#9b59b6",
                    "loan":        "#f39c12",
                    "end_of_loan": "#f39c12",
                    "free":        "#2ecc71",
                }
                TRANSFER_LABELS = {
                    "transfer":    t("mv_legend_transfer"),
                    "loan":        t("mv_legend_loan"),
                    "end_of_loan": t("mv_legend_end_of_loan"),
                    "free":        t("mv_legend_free"),
                }

                if not _mv_transfers.empty:
                    _mv_transfers["transfer_date"] = pd.to_datetime(
                        _mv_transfers["transfer_date"], errors="coerce"
                    )
                    # group by transfer_type so each group gets one legend entry
                    for _tr_type, _tr_color in TRANSFER_COLORS.items():
                        _tr_group = _mv_transfers[_mv_transfers["transfer_type"] == _tr_type].copy()
                        _tr_group = _tr_group.dropna(subset=["transfer_date"])

                        _tr_x, _tr_y, _tr_custom = [], [], []
                        for _, tr in _tr_group.iterrows():
                            before = _mv_history[_mv_history["value_date"] <= tr["transfer_date"]]
                            if before.empty:
                                continue
                            _tr_x.append(tr["transfer_date"])
                            _tr_y.append(before.iloc[-1]["market_value"])
                            _tr_custom.append({
                                "from":   tr.get("from_team_name") or "—",
                                "to":     tr.get("to_team_name")   or "—",
                                "fee":    _fmt_eur(tr.get("fee_euros")),
                                "season": tr.get("season") or "—",
                            })

                        if not _tr_x:
                            continue

                        fig_mv.add_trace(go.Scatter(
                            x=_tr_x,
                            y=_tr_y,
                            mode="markers",
                            marker=dict(
                                symbol="triangle-up",
                                size=12,
                                color=_tr_color,
                                line=dict(width=1, color="white"),
                            ),
                            name=TRANSFER_LABELS[_tr_type],
                            customdata=[
                                [c["from"], c["to"], c["fee"], c["season"]]
                                for c in _tr_custom
                            ],
                            hovertemplate=(
                                f"<b style='font-size:15px'>{TRANSFER_LABELS[_tr_type]}</b><br><br>"
                                f"<span style='color:#aaa'>{t('mv_hover_from_team')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[0]}}<br>"
                                f"<span style='color:#aaa'>{t('mv_hover_to_team')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[1]}}<br>"
                                f"<span style='color:#aaa'>{t('transfer_hover_fee')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[2]}}<br>"
                                f"<span style='color:#aaa'>{t('mv_hover_season')}</span>&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[3]}}"
                                "<extra></extra>"
                            ),
                        ))

                # layer 5 — injury milestones (X markers with hover)
                if not _mv_injuries.empty:
                    _mv_injuries["date_from"] = pd.to_datetime(
                        _mv_injuries["date_from"], errors="coerce"
                    )
                    _inj_x, _inj_y, _inj_custom = [], [], []
                    for _, inj in _mv_injuries.iterrows():
                        id_ = inj["date_from"]
                        if pd.isna(id_):
                            continue
                        before = _mv_history[_mv_history["value_date"] <= id_]
                        if before.empty:
                            continue
                        _inj_x.append(id_)
                        _inj_y.append(before.iloc[-1]["market_value"])
                        _inj_custom.append([
                            inj.get("injury_type") or "—",
                            inj["date_from"].strftime("%d/%m/%Y"),
                            str(int(inj["days_absent"])) + " days" if pd.notna(inj.get("days_absent")) else "—",
                        ])

                    if _inj_x:
                        fig_mv.add_trace(go.Scatter(
                            x=_inj_x,
                            y=_inj_y,
                            mode="markers",
                            marker=dict(
                                symbol="x",
                                size=10,
                                color="#e74c3c",
                                line=dict(width=2, color="#e74c3c"),
                            ),
                            name=t("mv_legend_injury"),
                            customdata=_inj_custom,
                            hovertemplate=(
                                f"<b style='font-size:15px'>{t('mv_hover_injury')}</b><br><br>"
                                f"<span style='color:#aaa'>{t('transfer_hover_type')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[0]}}<br>"
                                f"<span style='color:#aaa'>{t('transfer_hover_date')}</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[1]}}<br>"
                                f"<span style='color:#aaa'>{t('mv_hover_absent')}</span>&nbsp;&nbsp;&nbsp;&nbsp;%{{customdata[2]}}"
                                "<extra></extra>"
                            ),
                        ))

                # ── Axis and layout formatting ─────────────────
                fig_mv.update_layout(
                    xaxis=dict(
                        color="white",
                        gridcolor="#333",
                        tickfont=dict(size=18),
                        tickformat="%Y",
                    ),
                    yaxis=dict(
                        color="white",
                        gridcolor="#333",
                        tickfont=dict(size=18),
                        tickformat=".2s",
                        tickprefix="€",
                    ),
                    plot_bgcolor="#0e1117",
                    paper_bgcolor="#0e1117",
                    font=dict(color="white"),
                    legend=dict(
                        bgcolor="#1a1a2e",
                        bordercolor="#444",
                        font=dict(size=16, color="white"),
                    ),
                    hoverlabel=dict(
                        bgcolor="#1a1a2e",
                        bordercolor="#444",
                        font=dict(size=16, color="white"),
                        align="left",
                    ),
                    margin=dict(l=60, r=20, t=30, b=40),
                    height=450,
                )

                st.plotly_chart(fig_mv, width="stretch")

                st.caption(t("mv_caption"))
                if _mv_show_benchmark:
                    st.markdown(t("mv_benchmark_explain"))
 
    # ── Transfer History ─────────────────────────────────────

    with t_transfer_history:
        st.subheader(t("tab_transfer_history"))
 
        _th_all_players = player_detail.get_all_players()
        _th_search = st.text_input("Search player", key="th_search", placeholder="Type a name…")
 
        if _th_search:
            _th_filtered = _th_all_players[
                _th_all_players["canonical_name"].str.contains(_th_search, case=False, na=False)
            ]
        else:
            _th_filtered = _th_all_players
 
        _th_names = _th_filtered["canonical_name"].tolist()
        _th_selected = st.selectbox(
            "Select player",
            options=_th_names if _th_names else ["(no match)"],
            key="th_select",
            disabled=not _th_names,
        )
 
        if _th_names:
            _th_row = _th_filtered[_th_filtered["canonical_name"] == _th_selected]
        else:
            _th_row = pd.DataFrame()
 
        if _th_row.empty:
            st.info("No player selected.")
        else:
            _th_cid = int(_th_row.iloc[0]["canonical_id"])
 
            _th_transfers = player_detail.get_transfer_history(_th_cid)
            _th_kpis      = player_detail.get_transfer_history_kpis(_th_cid)
 
            if _th_transfers.empty:
                st.info(t("transfer_no_data"))
            else:
                # ── KPI cards ─────────────────────────────────
                # current team — exclude Retirado
                _th_current = player_detail.get_player_team_history(_th_cid, all_time=True)
                _th_current = _th_current[_th_current["team"] != "Retirado"]
                _th_current_team = _th_current.iloc[0]["team"] if not _th_current.empty else "—"
 
                # number of distinct teams — exclude Retirado
                _th_num_teams = _th_transfers["to_team_name"][
                    _th_transfers["to_team_name"] != "Retirado"
                ].nunique()
 
                _max_fee_delta = None
                if _th_kpis["max_fee_team"] and _th_kpis["max_fee_date"]:
                    _max_date_str = pd.Timestamp(_th_kpis["max_fee_date"]).strftime("%b %Y")
                    _max_fee_delta = f"{_th_kpis['max_fee_team']} · {_max_date_str}"
 
                tk1, tk2, tk3, tk4 = st.columns(4)
                tk1.metric(
                    t("transfer_total_fees"),
                    _fmt_eur(_th_kpis["total_fees"]) if _th_kpis["total_fees"] else "—",
                )
                tk2.metric(
                    t("transfer_most_expensive"),
                    _fmt_eur(_th_kpis["max_fee"]) if _th_kpis["max_fee"] else "—",
                    delta=_max_fee_delta,
                    delta_color="off",
                )
                tk3.metric(t("transfer_current_team"), _th_current_team)
                tk4.metric(t("transfer_num_teams"), _th_num_teams)
 
                st.divider()
 
                # ── Transfer history table ────────────────────
                _th_display = _th_transfers.copy()
                _th_display = _th_display.sort_values("transfer_date", ascending=False)
                _th_display["transfer_date"] = pd.to_datetime(
                    _th_display["transfer_date"], errors="coerce"
                ).dt.strftime("%d/%m/%Y")
                _th_display["fee_euros"] = _th_display["fee_euros"].apply(_fmt_eur)
 
                # translate transfer_type values to current language
                _type_map = {
                    "transfer":    t("transfer_type_transfer"),
                    "loan":        t("transfer_type_loan"),
                    "end_of_loan": t("transfer_type_end_of_loan"),
                    "free":        t("transfer_type_free"),
                    "unknown":     t("transfer_type_unknown"),
                }
                _th_display["transfer_type"] = (
                    _th_display["transfer_type"].map(_type_map).fillna(_th_display["transfer_type"])
                )
 
                _th_display = _th_display[[
                    "season", "transfer_date", "from_team_name",
                    "to_team_name", "fee_euros", "transfer_type"
                ]]
                _th_display.columns = [
                    t("transfer_col_season"), t("transfer_col_date"),
                    t("transfer_col_from"), t("transfer_col_to"),
                    t("transfer_col_fee"), t("transfer_col_type"),
                ]
                st.dataframe(_th_display, width="stretch", hide_index=True)
                st.caption(t("transfer_caption"))

                st.divider()

                # ══════════════════════════════════════════════════════════
                # VISUALIZATION 1 — GANTT CHART: full career timeline
                # Each horizontal bar represents a spell at a club.
                # Bar starts at date_from (arrival) and ends at date_to (departure).
                # If date_to is NULL the player is still at that club — filled with today.
                # Hover shows club name, arrival/departure dates and total duration..
                # ══════════════════════════════════════════════════════════
                st.subheader(t("transfer_career_timeline"))

                _gantt_df = player_detail.get_player_team_history(_th_cid, all_time=True)
                # exclude retirement entries — handled separately via the Active/Retired badge
                _gantt_df = _gantt_df[_gantt_df["team"] != "Retirado"].copy()

                if not _gantt_df.empty:
                    # normalize dates — can arrive as str, date or datetime from the DB
                    _gantt_df["date_from"] = pd.to_datetime(_gantt_df["date_from"], errors="coerce")
                    _gantt_df["date_to"]   = pd.to_datetime(_gantt_df["date_to"],   errors="coerce")

                    # fill NULL date_to with today — player is still at that club
                    _gantt_df["date_to"] = _gantt_df["date_to"].fillna(pd.Timestamp.today())

                    # drop rows where date_from could not be parsed — nothing to draw
                    _gantt_df = _gantt_df.dropna(subset=["date_from"])

                    # sort ascending — autorange reversed will show most recent at top
                    _gantt_df = _gantt_df.sort_values("date_from", ascending=False)

                    # compute human-readable duration for the hover tooltip
                    _gantt_df["duration_days"] = (_gantt_df["date_to"] - _gantt_df["date_from"]).dt.days
                    _gantt_df["duration_str"] = _gantt_df["duration_days"].apply(
                        lambda d: f"{d // 365} years {(d % 365) // 30} months" if d >= 365
                        else f"{d // 30} months"
                    )

                    # make team labels unique to avoid plotly grouping repeated clubs
                    # on the same Y axis position (e.g. Lukaku returning to Chelsea or Inter)
                    _team_counts = {}
                    _unique_teams = []
                    for team in _gantt_df["team"].tolist():
                        if team not in _team_counts:
                            _team_counts[team] = 1
                            _unique_teams.append(team)
                        else:
                            _team_counts[team] += 1
                            _unique_teams.append(f"{team} ({_team_counts[team]})")
                    _gantt_df["team_label"] = _unique_teams

                    fig_gantt = go.Figure()

                    # generate one color per unique team — same team always gets same color across spells
                    _team_colors = _generate_team_colors(_gantt_df["team"].tolist())

                    # build single trace with all bars — avoids Y axis ordering issues
                    fig_gantt.add_trace(go.Bar(
                        x=[(row["date_to"] - row["date_from"]).total_seconds() * 1000
                        for _, row in _gantt_df.iterrows()],
                        y=_gantt_df["team_label"].tolist(),
                        base=[row["date_from"].strftime("%Y-%m-%d")
                            for _, row in _gantt_df.iterrows()],
                        orientation="h",
                        marker_color=[_team_colors[team] for team in _gantt_df["team"].tolist()],
                        customdata=list(zip(
                            _gantt_df["date_from"].dt.strftime("%d/%m/%Y"),
                            _gantt_df.apply(
                                lambda r: t("transfer_present")
                                if r["date_to"].date() >= pd.Timestamp.today().date()
                                else r["date_to"].strftime("%d/%m/%Y"),
                                axis=1,
                            ),
                            _gantt_df["duration_str"],
                            _gantt_df["team"],  # original name without suffix for hover
                        )),
                        hovertemplate=(
                            "<b style='font-size:14px'>%{customdata[3]}</b><br><br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_from')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[0]}<br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_to')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[1]}<br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_duration')}</span>"
                            "&nbsp;&nbsp;%{customdata[2]}"
                            "<extra></extra>"
                        ),
                        showlegend=False,
                    ))

                    fig_gantt.update_layout(
                        barmode="overlay",
                        xaxis=dict(
                            type="date",
                            tickformat="%Y",
                            gridcolor="#333",
                            color="white",
                            tickfont=dict(size=18),
                        ),
                        yaxis=dict(
                            color="white",
                            autorange="reversed",
                            tickvals=_gantt_df["team_label"].tolist(),  # valores únicos internos
                            ticktext=_gantt_df["team"].tolist(), 
                            tickfont=dict(size=18),         
                        ),
                        plot_bgcolor="#0e1117",
                        paper_bgcolor="#0e1117",
                        font=dict(color="white"),
                        margin=dict(l=120, r=20, t=30, b=40),
                        height=max(200, len(_gantt_df) * 40),
                        hoverlabel=dict(
                            bgcolor="#1a1a2e",
                            bordercolor="#444",
                            font=dict(size=16, color="white"),
                            align="left",
                        ),
                    )

                    st.plotly_chart(fig_gantt, width="stretch")

               
                st.divider()

                # ══════════════════════════════════════════════════════════
                # VISUALIZATION 2 — BAR CHART: transfer fees over time
                # Only permanent transfers with a known fee (fee_euros > 0) are shown.
                # Loans, free transfers and unknown fees are excluded.
                # Each bar represents one transfer — height = fee in euros.
                # Hover shows origin club, destination club, fee and transfer date.
                # ══════════════════════════════════════════════════════════
                st.subheader(t("transfer_fees_chart"))

                # filter to permanent transfers with a known, non-zero fee
                # filter to transfers with a known, non-zero fee — includes loans with fee and end of loan with fee
                _fee_df = _th_transfers[
                    (_th_transfers["fee_euros"].notna()) &
                    (_th_transfers["fee_euros"] > 0)
                ].copy()

                if _fee_df.empty:
                        st.caption(t("transfer_no_fee_data"))
                else:
                    _fee_df["transfer_date"] = pd.to_datetime(_fee_df["transfer_date"], errors="coerce")
                    # drop rows where transfer_date could not be parsed
                    _fee_df = _fee_df.dropna(subset=["transfer_date"])
                    # sort chronologically so bars read left to right in time order
                    _fee_df = _fee_df.sort_values("transfer_date", ascending=True)

                    # x-axis label: destination club + season for context
                    _fee_df["label"] = _fee_df.apply(
                        lambda r: f"{r['to_team_name']} ({r['season']})" if pd.notna(r["season"])
                        else r["to_team_name"],
                        axis=1,
                    )

                    fig_fees = go.Figure()
                    fig_fees.add_trace(go.Bar(
                        x=_fee_df["label"],
                        y=_fee_df["fee_euros"],
                        marker_color=[
                            _team_colors.get(team, "#666666")
                            for team in _fee_df["to_team_name"].fillna("—")
                        ],
                        hovertemplate=(
                            "<b>%{customdata[3]}</b><br><br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_from')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[0]}<br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_fee')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[1]}<br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_date')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[2]}<br>"
                            f"<span style='color:#aaa'>{t('transfer_hover_type')}</span>"
                            "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;%{customdata[4]}"
                            "<extra></extra>"
                        ),
                        # customdata passes extra fields to the hover template
                        customdata=list(zip(
                            _fee_df["from_team_name"].fillna("—"),
                            _fee_df["fee_euros"].apply(_fmt_eur),
                            _fee_df["transfer_date"].dt.strftime("%d/%m/%Y"),
                            _fee_df["to_team_name"].fillna("—"),   # nombre del equipo para el título del hover
                            _fee_df["transfer_type"].map({
                                "transfer":    "Transfer",
                                "loan":        "Loan",
                                "end_of_loan": "End of loan",
                                "free":        "Free",
                            }).fillna("Unknown"),
                        )),
                        showlegend=False,
                    ))

                    fig_fees.update_layout(
                        xaxis=dict(
                            color="white",
                            gridcolor="#333",
                            tickfont=dict(size=18),
                        ),
                        yaxis=dict(
                            color="white",
                            gridcolor="#333",
                            tickformat=".2s",
                            tickprefix="€",
                            tickfont=dict(size=18),
                        ),
                        plot_bgcolor="#0e1117",
                        paper_bgcolor="#0e1117",
                        font=dict(color="white"),
                        margin=dict(l=60, r=20, t=30, b=80),
                        height=350,
                        hoverlabel=dict(
                            bgcolor="#1a1a2e",
                            bordercolor="#444",
                            font=dict(size=16, color="white"),
                            align="left",
                        ),
                    )

                    st.plotly_chart(fig_fees, width="stretch")
