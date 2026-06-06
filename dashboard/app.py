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

from dashboard import analytics, db, explore, player_detail, scanner, wizard_view
from dashboard.i18n import t, get_lang, LANGUAGES

st.set_page_config(
    page_title="Football Scraping Dashboard",
    page_icon="⚽",
    layout="wide",
)

# ── Language selector (sidebar) ──────────────────────────
_lang_label = st.sidebar.selectbox(
    "🌐 Language / Idioma",
    list(LANGUAGES.keys()),
    index=0,
    key="lang_selector",
)
st.session_state["app_language"] = LANGUAGES[_lang_label]

# ─────────────────────────────────────────────
# DB-unreachable guard (runs once on each rerun)
# ─────────────────────────────────────────────
try:
    _DB_SUMMARY = db.get_db_summary()
except Exception:
    st.error(t("db_error"))
    st.stop()

def _fmt(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def _render_stadium_detail(row: pd.Series) -> None:
    """Detail panel: photo, metadata and external links for one stadium row."""
    st.subheader(t("stadium_detail"))
    img_col, info_col = st.columns([1, 1.4])

    with img_col:
        image_url = row.get("image_url")
        lat, lon = row.get("latitude"), row.get("longitude")
        if pd.notna(image_url) and str(image_url).strip():
            st.image(str(image_url), caption=str(row.get("stadium_name") or ""), width="stretch")
        elif pd.notna(lat) and pd.notna(lon):
            st.caption(t("stadium_map_fallback"))
            st.map(pd.DataFrame({"lat": [float(lat)], "lon": [float(lon)]}), zoom=12)
        else:
            st.info(t("stadium_no_photo"))

    with info_col:
        title = str(row.get("stadium_name") or "-")
        team = str(row.get("team") or "-")
        st.markdown(f"**{title}** — {team}")
        meta = []
        if pd.notna(row.get("season")):
            meta.append(f"**{t('season')}:** {row['season']}")
        if pd.notna(row.get("capacity")):
            meta.append(f"**{t('total_capacity')}:** {_fmt(row['capacity'])}")
        if pd.notna(row.get("built_year")):
            meta.append(f"**Built:** {int(row['built_year'])}")
        if pd.notna(row.get("city")) or pd.notna(row.get("country")):
            city = str(row.get("city") or "")
            country = str(row.get("country") or "")
            loc = ", ".join(p for p in (city, country) if p)
            meta.append(f"**{t('country')}:** {loc}")
        if pd.notna(row.get("surface")):
            meta.append(f"**Surface:** {row['surface']}")
        if pd.notna(row.get("architect")):
            meta.append(f"**Architect:** {row['architect']}")
        if pd.notna(row.get("owner")):
            meta.append(f"**Owner:** {row['owner']}")
        if pd.notna(lat) and pd.notna(lon):
            meta.append(f"**Coords:** {float(lat):.4f}, {float(lon):.4f}")
        if meta:
            st.markdown("  \n".join(meta))

        link_cols = st.columns(3)
        wiki = row.get("wikipedia_url")
        if pd.notna(wiki) and str(wiki).strip():
            link_cols[0].link_button(t("stadium_wikipedia"), str(wiki))
        qid = row.get("wikidata_qid")
        if pd.notna(qid) and str(qid).strip():
            link_cols[1].link_button(
                t("stadium_wikidata"),
                f"https://www.wikidata.org/wiki/{qid}",
            )
        tm = row.get("tm_url")
        if pd.notna(tm) and str(tm).strip():
            link_cols[2].link_button("Transfermarkt", str(tm))



(tab_explore, tab_teams, tab_gk, tab_players, tab_player_detail, tab_injuries,
 tab_shot, tab_stadiums, tab_monitor, tab_wizard) = st.tabs(
    [t("tab_exploration"), t("tab_teams"), t("tab_goalkeepers"), t("tab_players"),
     "Player Detail", t("tab_injuries"), t("tab_shot_intelligence"), t("tab_stadiums"),
     t("tab_pipeline"), t("tab_wizard")]
)


# ════════════════════════════════════════════════════════════════════
# TAB 1 — EXPLORATION
# ════════════════════════════════════════════════════════════════════
def _empty_info(message: str | None = None):
    st.info(message or t("no_data"))


with tab_explore:
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

        t_results, t_players, t_shots, t_events = st.tabs(
            [t("results"), t("player_stats"), t("shots_by_source"), t("events")]
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
        with t_players:
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


# ════════════════════════════════════════════════════════════════════
# SHARED HELPER — 3-column selector row (reused across new tabs)
# ════════════════════════════════════════════════════════════════════
def _tab_selectors(key_prefix: str, all_seasons: bool = False):
    """Return (competition, season_or_none, team_or_none) for a new tab."""
    _comps = explore.get_competitions()
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        _comp = st.selectbox(t("competition"), _comps, key=f"{key_prefix}_comp")
    _seasons = explore.get_seasons_for_competition(_comp)
    season_opts = ([t("all_seasons")] + _seasons) if all_seasons else (_seasons or ["(no seasons)"])
    with sc2:
        _season_sel = st.selectbox(
            t("season"), season_opts,
            key=f"{key_prefix}_season",
            disabled=not _seasons,
        )
    _season = None if (_season_sel in (t("all_seasons"), "(no seasons)") or not _seasons) else _season_sel
    _teams = explore.get_teams_for_season(_season or (_seasons[0] if _seasons else ""), _comp) if _seasons else []
    with sc3:
        _team_sel = st.selectbox(
            t("team"), [t("all_teams")] + _teams,
            key=f"{key_prefix}_team",
            disabled=not _teams,
        )
    _team = None if _team_sel == t("all_teams") else _team_sel
    return _comp, _season, _team


# ════════════════════════════════════════════════════════════════════
# TAB 2 — TEAMS
# ════════════════════════════════════════════════════════════════════
with tab_teams:
    st.header(t("tab_teams"))
    _t_comp, _t_season, _t_team = _tab_selectors("teams")

    if _t_season is None:
        st.info(t("select_season"))
    else:
        df = explore.get_team_standings(_t_season, _t_team, _t_comp)
        if df.empty:
            st.info("No match data found. Run pipeline_runner.py to populate dim_match.")
        else:
            total_matches = int(df["p"].sum()) // 2
            total_goals = int(df["gf"].sum())
            avg_goals = round(total_goals / total_matches, 2) if total_matches else 0
            avg_xg = round(float(df["xg_for"].sum()) / total_matches, 2) if total_matches else 0

            m1, m2, m3, m4 = st.columns(4)
            m1.metric(t("tab_teams"), len(df))
            m2.metric(t("total_goals"), _fmt(total_goals))
            m3.metric(t("avg_goals_match"), f"{avg_goals:.2f}")
            m4.metric(t("avg_xg_match"), f"{avg_xg:.2f}")

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


# ════════════════════════════════════════════════════════════════════
# TAB 3 — GOALKEEPERS
# ════════════════════════════════════════════════════════════════════
with tab_gk:
    st.header(t("goalkeepers"))
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


# ════════════════════════════════════════════════════════════════════
# TAB 4 — PLAYERS
# ════════════════════════════════════════════════════════════════════
with tab_players:
    st.header(t("tab_players"))
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


# ════════════════════════════════════════════════════════════════════
# TAB 5 — PLAYER DETAIL
# ════════════════════════════════════════════════════════════════════
with tab_player_detail:
    st.header("Player Detail")

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

        st.subheader(_pd["canonical_name"])
        _hc1, _hc2, _hc3 = st.columns(3)
        with _hc1:
            st.markdown(f"**Position:** {_pd['position'] or '—'}")
        with _hc2:
            st.markdown(f"**Nationality:** {_pd['nationality'] or '—'}")
        with _hc3:
            _bd = _pd["birth_date"]
            st.markdown(f"**Born:** {_bd.strftime('%d %b %Y') if _bd else '—'}")

        _sources_map = {
            "StatsBomb": _pd["id_statsbomb"], "Understat": _pd["id_understat"],
            "SofaScore": _pd["id_sofascore"], "Transfermarkt": _pd["id_transfermarkt"],
            "WhoScored": _pd["id_whoscored"],
        }
        _badge_parts = []
        for _src, _sid in _sources_map.items():
            _has = _sid is not None and str(_sid) not in ("", "None", "0")
            _color = "green" if _has else "gray"
            _badge_parts.append(
                f'<span style="background:{_color};color:white;padding:2px 8px;'
                f'border-radius:4px;margin-right:4px;font-size:0.8em">{_src}</span>'
            )
        st.markdown(" ".join(_badge_parts), unsafe_allow_html=True)
        st.divider()

        st.subheader("Source Identity (MDM)")
        _mdm_df = player_detail.get_player_mdm(_pd_cid)
        if _mdm_df.empty:
            st.info("No source aliases recorded for this player.")
        else:
            _mdm_df.columns = ["Source", "Name used", "Source ID", "Score", "Resolved"]
            st.dataframe(_mdm_df, width="stretch", hide_index=True)
        st.divider()

        st.subheader("Shot Map")
        _sm_seasons = ["All"] + player_detail.get_player_shot_seasons(_pd_cid)
        _sm_sources = ["All"] + player_detail.get_player_shot_sources(_pd_cid)
        _smc1, _smc2, _smc3 = st.columns(3)
        with _smc1:
            _sm_season = st.selectbox("Season", _sm_seasons, key="pd_sm_season")
        with _smc2:
            _sm_source = st.selectbox("Source", _sm_sources, key="pd_sm_source")
        _sm_matches_df = player_detail.get_player_shot_matches(_pd_cid, _sm_season, _sm_source)
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
            _shots = f" | {_match.shots} shots"
            _label = f"{_date} | {_home}{_score} {_away}{_comp}{_shots}"
            _sm_match_options[_label] = int(_match.match_id)
        with _smc3:
            _sm_match_label = st.selectbox(
                "Match",
                list(_sm_match_options.keys()),
                key="pd_sm_match",
                disabled=_sm_matches_df.empty,
            )
        _sm_match_id = _sm_match_options.get(_sm_match_label)

        _shots_df = player_detail.get_player_shots(
            _pd_cid, _sm_season, _sm_source, _sm_match_id
        )
        if _shots_df.empty:
            st.info("No shot data found for this selection.")
        else:
            _pitch = _PlayerPitch(
                pitch_type="custom",
                pitch_length=105,
                pitch_width=68,
                pitch_color="#1a472a",
                line_color="white",
                line_zorder=2,
            )
            _fig_sm, _ax_sm = _pitch.draw(figsize=(7, 4.5))
            _fig_sm.patch.set_facecolor("#1a472a")
            _x      = _shots_df["x"].to_numpy(dtype=float, na_value=np.nan)
            _y      = _shots_df["y"].to_numpy(dtype=float, na_value=np.nan)
            _xg_arr = _shots_df["xg"].fillna(0.05).to_numpy(dtype=float)
            _sizes  = np.clip(_xg_arr * 300, 20, 200)
            _is_goal = (_shots_df["result"] == "Goal").to_numpy()
            _pitch.scatter(_x[~_is_goal], _y[~_is_goal], s=_sizes[~_is_goal],
                color="white", edgecolors="#cccccc", linewidths=0.4, alpha=0.6,
                ax=_ax_sm, zorder=3, label="No goal")
            _pitch.scatter(_x[_is_goal], _y[_is_goal], s=_sizes[_is_goal],
                color="red", edgecolors="white", linewidths=0.5, alpha=0.9,
                ax=_ax_sm, zorder=4, label="Goal")
            _ax_sm.legend(facecolor="#1a472a", labelcolor="white", loc="upper right", fontsize=8)
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


# ════════════════════════════════════════════════════════════════════
# TAB 6 — INJURIES
# ════════════════════════════════════════════════════════════════════
with tab_injuries:
    st.header(t("tab_injuries"))
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


# ════════════════════════════════════════════════════════════════════
# TAB 6 — SHOT INTELLIGENCE
# ════════════════════════════════════════════════════════════════════
with tab_shot:
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
        # ── Section 1 — Pitch Danger Heatmap ─────────────────────
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


# ════════════════════════════════════════════════════════════════════
# TAB 7 — PIPELINE MONITORING

# ════════════════════════════════════════════════════════════════════
# TAB — STADIUMS  (dim_stadium · Transfermarkt · SCD2)
# ════════════════════════════════════════════════════════════════════
with tab_stadiums:
    st.header(t("stadiums"))
    st.caption(
        "Estadios por equipo — fuente: Transfermarkt. "
        "Modelo SCD2: una fila por estado del estadio, con rango de "
        "temporadas. Lanza la descarga desde la pestaña Wizard."
    )

    if not explore._stadium_table_exists():
        st.warning(
            "La tabla `dim_stadium` no existe todavía. "
            "Aplica la migración:\n\n"
            "    psql -U postgres -d football_db -f db/add_dim_stadium.sql\n\n"
            "Y luego carga datos desde el wizard (\"Descargar estadios por temporada\")."
        )
    else:
        # ── Filtros ──────────────────────────────────────────────
        st_seasons   = explore.get_stadium_seasons()
        st_comps     = explore.get_competitions()
        st_countries = explore.get_stadium_countries()

        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        with f1:
            st_season = st.selectbox(
                "Season",
                ["All seasons"] + st_seasons,
                key="st_season",
                disabled=not st_seasons,
            )
        with f2:
            st_comp = st.selectbox(
                "Competition",
                ["All competitions"] + st_comps,
                key="st_comp",
                disabled=not st_comps,
            )
        with f3:
            st_country = st.selectbox(
                "Country",
                ["All countries"] + st_countries,
                key="st_country",
                disabled=not st_countries,
            )
        with f4:
            st_search = st.text_input(
                "Search (stadium / team / city)",
                value="", key="st_search",
            ).strip() or None

        season_q  = None if st_season  == "All seasons"      else st_season
        comp_q    = None if st_comp    == "All competitions" else st_comp
        country_q = None if st_country == "All countries"    else st_country

        # ── Tarjetas resumen ─────────────────────────────────────
        summary = explore.get_stadium_summary(
            season=season_q, competition=comp_q, country=country_q,
        )
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(t("stadiums"),        _fmt(summary["n_stadiums"]))
        m2.metric(t("total_capacity"),  _fmt(summary["total_capacity"]))
        m3.metric(t("avg_capacity"),    _fmt(summary["avg_capacity"]))
        m4.metric(
            f"{t('largest')} ({summary['max_stadium']})",
            _fmt(summary["max_capacity"]),
        )

        # ── Tabla ────────────────────────────────────────────────
        df_st = explore.get_stadiums(
            season=season_q, competition=comp_q,
            country=country_q, search=st_search,
        )
        if df_st.empty:
            st.info(
                "No hay estadios para esta combinación de filtros. Si "
                "acabas de migrar la tabla, lanza desde el wizard "
                "\"Descargar estadios por temporada\" para poblarla."
            )
        else:
            st.caption(t("stadium_select_hint"))
            display_df = df_st.copy()
            display_df.columns = [
                "stadium_id", "Team", "Season", "Stadium", "Capacity",
                "Built", "Owner", "City", "Country", "Surface",
                "Architect", "Lat", "Lon", "Transfermarkt URL",
                "image_url", "wikipedia_url", "wikidata_qid",
            ]
            table_df = display_df.drop(
                columns=["stadium_id", "image_url", "wikipedia_url", "wikidata_qid"],
            )
            selection = st.dataframe(
                table_df,
                width="stretch",
                on_select="rerun",
                selection_mode="single-row",
                key="stadium_picker",
                column_config={
                    "Transfermarkt URL": st.column_config.LinkColumn(
                        "Transfermarkt", display_text="abrir"
                    ),
                    "Capacity": st.column_config.NumberColumn(format="%d"),
                    "Lat": st.column_config.NumberColumn(format="%.4f"),
                    "Lon": st.column_config.NumberColumn(format="%.4f"),
                },
            )
            selected_rows = (
                selection.selection.rows
                if selection is not None and hasattr(selection, "selection")
                else []
            )
            if selected_rows:
                st.divider()
                _render_stadium_detail(df_st.iloc[selected_rows[0]])

            # ── Grafico top-15 por aforo ─────────────────────────
            top = (
                df_st.dropna(subset=["capacity"])
                     .sort_values("capacity", ascending=False)
                     .head(15)
            )
            if not top.empty:
                st.subheader(t("top_15_capacity"))
                fig_st, ax_st = plt.subplots(figsize=(10, max(4, len(top) * 0.4)))
                fig_st.patch.set_facecolor("#0e1117")
                ax_st.set_facecolor("#0e1117")
                labels = [
                    f"{r.stadium_name} ({r.team})" for r in top.itertuples()
                ]
                ax_st.barh(labels, top["capacity"], color="#9b59b6")
                ax_st.set_xlabel("Capacity", color="white")
                ax_st.tick_params(colors="white")
                for spine in ax_st.spines.values():
                    spine.set_color("#444")
                ax_st.invert_yaxis()
                plt.tight_layout()
                st.pyplot(fig_st)
                plt.close(fig_st)

        st.caption(
            "Source: dim_stadium (Transfermarkt, SCD2). Granularidad: una "
            "fila por estado del estadio. Si la fila tiene "
            "valid_from_season=valid_to_season, sólo cubre esa temporada; "
            "si cubre un rango, no hubo cambio en esos años."
        )


# ════════════════════════════════════════════════════════════════════
with tab_monitor:
    st.header(t("pipeline_monitoring"))

    # ── Section 1 — DB metric cards ───────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric(t("tab_players"),    _fmt(_DB_SUMMARY['players']))
    p2.metric(t("matches"),       _fmt(_DB_SUMMARY['matches']))
    p3.metric("Shots (xG)",       _fmt(_DB_SUMMARY['shots']))
    p4.metric(t("tab_injuries"),  _fmt(_DB_SUMMARY['injuries']))

    st.divider()

    # ── Section 2 — Season scanner ────────────────────────
    st.subheader(t("season_scanner"))
    if st.button(t("scan_all_sources"), type="primary", key="scan_btn"):
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
            st.dataframe(pd.DataFrame(rows), width='stretch')
            st.info(
                "To load missing seasons, run:\n\n"
                "    python pipeline_runner.py --sources <source>\n\n"
                "Loading is intentionally CLI-only in this dashboard."
            )
        else:
            st.success("All scanned sources are up-to-date — no missing seasons.")

    st.divider()

    # ── Section 3 — Coverage ──────────────────────────────
    st.subheader(t("coverage_by_source"))
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
    st.subheader(t("player_review_queue"))
    pr_stats = db.get_player_review_stats()
    r1, r2, r3, r4 = st.columns(4)
    r1.metric(t("total"),          _fmt(pr_stats['total']))
    r2.metric(t("unresolved"),     _fmt(pr_stats['unresolved']))
    r3.metric(t("resolved"),       _fmt(pr_stats['resolved']))
    r4.metric(t("avg_similarity"), f"{pr_stats['avg_score']:.1f}")
    pr_df = db.get_player_review_queue(50)
    if pr_df.empty:
        st.info("No unresolved entries in `player_review`.")
    else:
        st.dataframe(pr_df, width='stretch')
    st.info(
        "To resolve a case, run:\n\n"
        "    python -m scripts.review_players --unresolved"
    )

    st.divider()

    # ── Section 5 — Recent matches ────────────────────────
    st.subheader(t("recent_matches"))
    rm_df = db.get_recent_matches(20)
    if rm_df.empty:
        st.info("No matches in `dim_match` yet.")
    else:
        st.dataframe(rm_df, width='stretch')


# ════════════════════════════════════════════════════════════════════
# TAB 8 — WIZARD (writes to the database — read-only exception)
# ════════════════════════════════════════════════════════════════════
with tab_wizard:
    wizard_view.render()
