"""
dashboard/views/match_context.py
================================
Match Context — weather, attendance, referees, managers.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from dashboard import explore
from dashboard.i18n import t
from dashboard.views.shared import _fmt, _tab_selectors


def render() -> None:
    st.header(t("tab_match_context"))
    _mc_comp, _mc_season, _mc_team = _tab_selectors("match_ctx")

    t_weather, t_attendance, t_referees, t_managers = st.tabs(
        [t("weather_section"), t("attendance_section"),
         t("referees_section"), t("managers_section")]
    )

    # ── Weather ──────────────────────────────────────────────
    with t_weather:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            ws = explore.get_weather_summary(_mc_season, _mc_team, _mc_comp)
            if ws["matches_with_weather"] == 0:
                st.info(t("no_weather_data"))
            else:
                wm1, wm2, wm3, wm4 = st.columns(4)
                wm1.metric(t("matches_with_weather"), _fmt(ws["matches_with_weather"]))
                wm2.metric(t("avg_temp"), f"{ws['avg_temp']:.1f} °C")
                wm3.metric(t("min_temp") + " / " + t("max_temp"),
                           f"{ws['min_temp']:.0f}° / {ws['max_temp']:.0f}°")
                wm4.metric(t("rainy_matches"), _fmt(ws["rainy_matches"]))

                df_w = explore.get_weather_by_match(_mc_season, _mc_team, _mc_comp)
                if not df_w.empty and "match_date" in df_w.columns:
                    st.subheader(t("temp_over_season"))
                    df_w["match_date"] = pd.to_datetime(df_w["match_date"])
                    df_w["temperature_c"] = pd.to_numeric(
                        df_w["temperature_c"], errors="coerce"
                    )
                    # Average when multiple matches share a date
                    chart_df = (
                        df_w.groupby("match_date")["temperature_c"]
                        .mean()
                        .sort_index()
                        .rename("°C")
                        .to_frame()
                    )
                    st.line_chart(chart_df)

                    with st.expander(t("results")):
                        st.dataframe(df_w, width="stretch")

    # ── Attendance ───────────────────────────────────────────
    with t_attendance:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            df_att = explore.get_attendance_by_match(_mc_season, _mc_team, _mc_comp)
            if df_att.empty:
                st.info(t("no_attendance_data"))
            else:
                att_vals = pd.to_numeric(df_att["attendance"], errors="coerce").dropna()
                am1, am2, am3, am4 = st.columns(4)
                am1.metric(t("matches"), _fmt(len(df_att)))
                am2.metric(t("avg_attendance"), _fmt(att_vals.mean()))
                am3.metric(t("max_attendance"), _fmt(att_vals.max()))
                am4.metric(t("total_attendance"), _fmt(att_vals.sum()))

                # Attendance by team chart (home)
                if _mc_team is None:
                    df_att_team = explore.get_attendance_by_team(_mc_season, _mc_comp)
                    if not df_att_team.empty:
                        st.subheader(t("attendance_by_team"))
                        top_att = df_att_team.head(20)
                        fig_att, ax_att = plt.subplots(
                            figsize=(10, max(3, len(top_att) * 0.45))
                        )
                        fig_att.patch.set_facecolor("#0e1117")
                        ax_att.set_facecolor("#0e1117")
                        ax_att.barh(
                            top_att["team"], top_att["avg_attendance"], color="#1abc9c"
                        )
                        ax_att.set_xlabel(t("avg_attendance"), color="white")
                        ax_att.tick_params(colors="white")
                        for spine in ax_att.spines.values():
                            spine.set_color("#444")
                        ax_att.invert_yaxis()
                        plt.tight_layout()
                        st.pyplot(fig_att)
                        plt.close(fig_att)

                with st.expander(t("results")):
                    st.dataframe(df_att, width="stretch")

    # ── Referees ─────────────────────────────────────────────
    with t_referees:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            df_ref = explore.get_referee_stats(_mc_season, _mc_comp)
            if df_ref.empty:
                st.info(t("no_referee_data"))
            else:
                rm1, rm2, rm3, rm4 = st.columns(4)
                rm1.metric(t("referees_section"), len(df_ref))
                rm2.metric(t("matches"),
                           _fmt(df_ref["matches_officiated"].sum()))
                rm3.metric(t("yellow_cards"),
                           _fmt(df_ref["yellow_cards"].sum()))
                rm4.metric(t("red_cards"),
                           _fmt(df_ref["red_cards"].sum()))

                display_ref = df_ref.rename(columns={
                    "referee": "Referee",
                    "matches_officiated": "Matches",
                    "yellow_cards": "Yellows",
                    "red_cards": "Reds",
                    "total_cards": "Total Cards",
                    "cards_per_match": "Cards/Match",
                })
                st.dataframe(display_ref, width="stretch")

                # Top 10 referees by cards per match (min 5 matches)
                strict_ref = df_ref[df_ref["matches_officiated"] >= 5].copy()
                if not strict_ref.empty:
                    st.subheader(t("referees_section") + " — Cards/Match")
                    top_strict = strict_ref.sort_values(
                        "cards_per_match", ascending=False
                    ).head(15)
                    fig_ref, ax_ref = plt.subplots(
                        figsize=(10, max(3, len(top_strict) * 0.45))
                    )
                    fig_ref.patch.set_facecolor("#0e1117")
                    ax_ref.set_facecolor("#0e1117")
                    ax_ref.barh(
                        top_strict["referee"],
                        top_strict["cards_per_match"],
                        color="#e74c3c",
                    )
                    ax_ref.set_xlabel("Cards per match", color="white")
                    ax_ref.tick_params(colors="white")
                    for spine in ax_ref.spines.values():
                        spine.set_color("#444")
                    ax_ref.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_ref)
                    plt.close(fig_ref)

                st.caption(
                    "Source: dim_referee + dim_match (referee_id FK) + fact_events (cards). "
                    "Min. 5 matches for chart. Cards/Match = (yellows + reds) / matches."
                )

    # ── Managers ──────────────────────────────────────────────
    with t_managers:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            df_mgr = explore.get_manager_stats(_mc_season, _mc_comp)
            if df_mgr.empty:
                st.info(t("no_manager_data"))
            else:
                mm1, mm2 = st.columns(2)
                mm1.metric(t("managers_section"), len(df_mgr))
                mm2.metric(t("matches"),
                           _fmt(df_mgr["matches"].sum() // 2))

                display_mgr = df_mgr.rename(columns={
                    "manager": "Manager",
                    "team": "Team (most recent)",
                    "matches": "Matches",
                    "wins": "W", "draws": "D", "losses": "L",
                    "goals_for": "GF", "goals_against": "GA",
                    "avg_gf": "Avg GF", "points_pct": "Points %",
                })
                st.dataframe(display_mgr, width="stretch")

                # Top 15 managers by points %
                top_mgr = df_mgr.sort_values("points_pct", ascending=False).head(15)
                if not top_mgr.empty:
                    st.subheader(t("manager_record"))
                    fig_mgr, ax_mgr = plt.subplots(
                        figsize=(10, max(3, len(top_mgr) * 0.45))
                    )
                    fig_mgr.patch.set_facecolor("#0e1117")
                    ax_mgr.set_facecolor("#0e1117")
                    labels_mgr = [
                        f"{r.manager} ({r.team})"
                        for r in top_mgr.itertuples()
                    ]
                    ax_mgr.barh(labels_mgr, top_mgr["points_pct"], color="#e67e22")
                    ax_mgr.set_xlabel(t("points_pct"), color="white")
                    ax_mgr.tick_params(colors="white")
                    for spine in ax_mgr.spines.values():
                        spine.set_color("#444")
                    ax_mgr.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_mgr)
                    plt.close(fig_mgr)

                st.caption(
                    "Source: dim_match (manager_home / manager_away text columns, "
                    "populated from WhoScored match_enrichment). "
                    "Min. 3 matches to appear. Points % = points won / max possible × 100."
                )
