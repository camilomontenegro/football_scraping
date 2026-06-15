"""
dashboard/app.py
================
Streamlit dashboard for the football scraping project.

Tabs:
  1. Exploration:        results, player stats, shots by source, events, standings
  2. Players:            discipline, goalkeepers, player detail, injuries
  3. Shot Intelligence:  pitch heatmap, player finishing, set-piece specialists
  4. Pass Network:       WhoScored pass maps per match
  5. Match Context:      weather, attendance, referees, managers
  6. Stadiums:           dim_stadium browser (Transfermarkt, SCD2)
  7. Pipeline:           DB metrics, scanner, coverage, player review, recent matches
  8. Wizard:             interactive DB operations

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
# Used to work out player's age from birth date in  market value tab 
from dateutil.relativedelta import relativedelta
# used to create custom legend entries for scatter marker in market value chart
from matplotlib.lines import Line2D  

# Make sibling modules (loaders/, pipeline_runner.py, etc.) importable when run
# as `streamlit run dashboard/app.py` from `football_scraping/`.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dashboard import analytics, db, explore, pass_network, player_detail, scanner, wizard_view
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

def _fmt_eur(v) -> str:
    """
    Formats a euro value as a human-readable string.
    Used throughout the market value tab to display prices consistently.

    Examples:
        _fmt_eur(38000000) → "€38.0M"
        _fmt_eur(500000)   → "€500K"
        _fmt_eur(None)     → "—"
    """
    if v is None:
        return "—"
    if isinstance(v, float) and pd.isna(v):
        return "—"
    if abs(v) >= 1_000_000:
        return f"€{v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"€{v/1_000:.0f}K"
    return f"€{v:,}"

def _fmt_team_history_date_to(row: pd.Series) -> str:
    """
    Formats the date_to column for the Career History table.
    
    - If date_to is NULL and the team is 'Retirado', the player retired
      at that club so we show 'Retired' instead of a date.
    - If date_to is NULL and the team is not 'Retirado', the player is
      still at that club so we show 'Present'.
    - Otherwise, format the date normally as dd/mm/yyyy.
    """
    if pd.isna(row["date_to"]):
        return "Retired" if row["team"] == "Retirado" else "Present"
    return pd.Timestamp(row["date_to"]).strftime("%d/%m/%Y")

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


# ════════════════════════════════════════════════════════════════════
# SHARED HELPER — 3-column selector row (reused across tabs)
# ════════════════════════════════════════════════════════════════════
def _tab_selectors(key_prefix: str, all_seasons: bool = False):
    """Return (competition, season_or_none, team_or_none) for a tab."""
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

def _empty_info(message: str | None = None):
    st.info(message or t("no_data"))


# ════════════════════════════════════════════════════════════════════
# MAIN TAB BAR  (8 tabs)
# ════════════════════════════════════════════════════════════════════
(tab_explore, tab_players, tab_shot, tab_passnet, tab_match_ctx,
 tab_stadiums, tab_monitor, tab_wizard) = st.tabs(
    [t("tab_exploration"), t("tab_players"), t("tab_shot_intelligence"),
     t("tab_pass_network"), t("tab_match_context"), t("tab_stadiums"),
     t("tab_pipeline"), t("tab_wizard")]
)


# ════════════════════════════════════════════════════════════════════
# TAB 1 — EXPLORATION  (Results · Player Stats · Shots · Events · Standings)
# ════════════════════════════════════════════════════════════════════
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


# ════════════════════════════════════════════════════════════════════
# TAB 2 — PLAYERS  (Discipline · Goalkeepers · Player Detail · Injuries . Market Value)
# ════════════════════════════════════════════════════════════════════
with tab_players:
    st.header(t("tab_players"))

    t_discipline, t_gk, t_detail, t_injuries, t_market_value,t_transfer_history = st.tabs(
        [t("tab_players"), t("tab_goalkeepers"), "Player Detail", t("tab_injuries"),t("tab_market_value"), t("tab_transfer_history")]
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
                # Player name with active/retired badge
                _is_retired = player_detail.is_player_retired(_pd_cid)
                _status_label = "Retired" if _is_retired else "Active"
                _status_color = "#e74c3c" if _is_retired else "#2ecc71"
                st.markdown(
                    f'<h1 style="margin:0;padding:0;font-size:2em">'
                    f'{_pd["canonical_name"]} '
                    f'<span style="font-size:0.45em;background:{_status_color};color:white;'
                    f'padding:2px 8px;border-radius:4px;vertical-align:middle">{_status_label}</span>'
                    f'</h1>',
                    unsafe_allow_html=True,
                )
                # Team badge
                 # current team from fact_transfers
                _current_team_df = player_detail.get_player_team_history(_pd_cid, all_time=True)
                _current_team_df = _current_team_df[_current_team_df["team"] != "Retirado"]

                _team_name = _current_team_df.iloc[0]["team"] if not _current_team_df.empty else "—"
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

           # ── Team history  ──────────────────────────
            # shows clubs the player has been at, filtered by season if one is selected
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
                _team_history.columns = ["Season", t("career_date_from"), t("career_date_to"), t("career_team")]
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

    # ── Market Value ─────────────────────────────────────────
    with t_market_value:
        st.subheader(t("mv_title"))

        # ── Player selector ──────────────────────────────────
        # Load all players and filter by search input
        _mv_all_players = player_detail.get_all_players()

        # text introduced by user
        _mv_search = st.text_input(
            "Search player",
            key="mv_search",
            placeholder="Type a name…"
        )

        # filter players by search input — if empty, show all players
        if _mv_search:
            _mv_filtered = _mv_all_players[
                _mv_all_players["canonical_name"].str.contains(_mv_search, case=False, na=False)
            ]
        else:
            _mv_filtered = _mv_all_players

        _mv_names = _mv_filtered["canonical_name"].tolist()

        # selectbox showing the filtered player names
        # if no players match the search, show "(no match)" and disable the selector
        _mv_selected = st.selectbox(
            "Select player",
            options=_mv_names if _mv_names else ["(no match)"],
            key="mv_select",
            disabled=not _mv_names,
        )

        # get the full row of the selected player from the filtered DataFrame
        # if no players match, return an empty DataFrame to avoid errors downstream
        if _mv_names:
            _mv_row = _mv_filtered[_mv_filtered["canonical_name"] == _mv_selected]
        else:
            _mv_row = pd.DataFrame()

        if _mv_row.empty:
            st.info("No player selected.")
        else:
            _mv_player  = _mv_row.iloc[0]
            _mv_cid     = int(_mv_player["canonical_id"])
            _mv_pos     = _mv_player.get("position")    # used for benchmark query
            _mv_bdate   = _mv_player.get("birth_date")  # used to convert age → date on x-axis

            # ── Load data from DB ─────────────────────────────
            # All four queries run once when the player is selected
            _mv_history     = player_detail.get_market_value_history(_mv_cid)
            _mv_transfers   = player_detail.get_transfer_history(_mv_cid)
            _mv_injuries    = player_detail.get_player_injuries(_mv_cid)
            _mv_kpis        = player_detail.get_market_value_kpis(_mv_cid)

            if _mv_history.empty:
                st.info(t("mv_no_data"))
            else:
                _mv_history["value_date"] = pd.to_datetime(_mv_history["value_date"])

                # ── Market value table for main player ───────────
                # shows the raw valuation history: date, age, value and club
                with st.expander(f"Market value data — {_mv_selected}"):
                    _mv_display = _mv_history[["value_date", "market_value", "club_name"]].copy()
                    _mv_display["value_date"] = _mv_display["value_date"].dt.strftime("%d/%m/%Y")
                    # calculate age at the time of each valuation using birth_date from dim_player
                    # relativedelta handles leap years correctly unlike simple day division
                    if _mv_bdate is not None:
                        _mv_display["age"] = _mv_history["value_date"].apply(
                            lambda d: relativedelta(d, pd.Timestamp(_mv_bdate)).years
                        )
                        # cast age to string to avoid default right-alignment inside cells
                        _mv_display["age"] = _mv_display["age"].astype(str)
                        # reorder columns: date, age, market_value, club
                        _mv_display = _mv_display[["value_date", "age", "market_value", "club_name"]]

                    _mv_display["market_value"] = _mv_display["market_value"].apply(_fmt_eur)

                    if _mv_bdate is not None:
                        _mv_display.columns = ["Date", "Age", "Market Value", "Club"]
                    else:
                        _mv_display.columns = ["Date", "Market Value", "Club"]

                    st.dataframe(_mv_display, width="stretch", hide_index=True)

                # ── KPI cards ─────────────────────────────────
                # Six summary metrics shown above the chart
                k1, k2, k3, k4, k5, k6 = st.columns(6)

                # latest valuation in fact_market_value
                k1.metric(t("mv_current_value"), _fmt_eur(_mv_kpis["current_value"]))

                # highest valuation ever, with the date as delta label
                k2.metric(
                    t("mv_peak_value"),
                    _fmt_eur(_mv_kpis["peak_value"]),
                    delta=_mv_kpis["peak_date"].strftime("%b %Y")
                    if _mv_kpis["peak_date"] is not None else None,
                    delta_color="off",
                )

                # % drop from peak — negative means the player lost value since peak
                k3.metric(
                    t("mv_from_peak"),
                    f"{_mv_kpis['pct_from_peak']:.1f}%"
                    if _mv_kpis["pct_from_peak"] is not None else "—",
                    delta=_fmt_eur(_mv_kpis["current_value"] - _mv_kpis["peak_value"])
                    if _mv_kpis["peak_value"] else None,
                    delta_color="inverse",
                )

                # absolute change vs the valuation ~12 months ago
                k4.metric(
                    t("mv_last_year_change"),
                    _fmt_eur(_mv_kpis["change_last_year"]),
                    delta_color="normal" if (_mv_kpis["change_last_year"] or 0) >= 0 else "inverse",
                )

                # number of transfers from fact_transfers
                k5.metric(t("mv_transfers"), _mv_kpis["num_transfers"])

                # position from dim_player — shown for context
                k6.metric(t("position"), _mv_pos or "—")

                st.divider()

                # ── Comparison player selector (optional) ─────
                # If the user types a name, a second selectbox appears
                # to pick the comparison player. The comparison curve
                # is drawn on the same chart without milestone markers.
                _mv_cmp_search = st.text_input(
                    t("mv_compare_player"),
                    key="mv_cmp_search",
                    placeholder="Type a name…",
                )
                # filter players by search input excluding the main player already selected
                # if empty, return an empty DataFrame — no comparison player selected
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
                        t("mv_select_comparison"),
                        options=_mv_cmp_names,
                        key="mv_cmp_select",
                    )
                    _mv_cmp_row = _mv_cmp_filtered[
                        _mv_cmp_filtered["canonical_name"] == _mv_cmp_selected
                    ]
                    if not _mv_cmp_row.empty:
                        _mv_cmp_cid       = int(_mv_cmp_row.iloc[0]["canonical_id"])
                        _mv_cmp_name      = _mv_cmp_selected

                        # load market value history, transfers and injuries for the comparison player
                        _mv_cmp_history   = player_detail.get_market_value_history(_mv_cmp_cid)
                        _mv_cmp_transfers = player_detail.get_transfer_history(_mv_cmp_cid)
                        _mv_cmp_injuries  = player_detail.get_player_injuries(_mv_cmp_cid)

                        if not _mv_cmp_history.empty:
                            _mv_cmp_history["value_date"] = pd.to_datetime(_mv_cmp_history["value_date"])

                        # ── Market value table for comparison player ──────
                        # shows the raw valuation history: date, value and club
                        if not _mv_cmp_history.empty:
                            _mv_cmp_bdate = _mv_cmp_row.iloc[0].get("birth_date")

                            with st.expander(f"Market value data — {_mv_cmp_selected}"):
                                _mv_cmp_display = _mv_cmp_history[["value_date", "market_value", "club_name"]].copy()
                                _mv_cmp_display["value_date"] = _mv_cmp_display["value_date"].dt.strftime("%d/%m/%Y")
                                # calculate age at the time of each valuation using birth_date from dim_player
                                # relativedelta handles leap years correctly unlike simple day division
                                if _mv_cmp_bdate is not None:
                                    _mv_cmp_display["age"] = _mv_cmp_history["value_date"].apply(
                                        lambda d: relativedelta(d, pd.Timestamp(_mv_cmp_bdate)).years
                                    )
                                    # cast age to string to avoid default right-alignment inside cells
                                    _mv_cmp_display["age"] = _mv_cmp_display["age"].astype(str)
                                    _mv_cmp_display = _mv_cmp_display[["value_date", "age", "market_value", "club_name"]]
                                _mv_cmp_display["market_value"] = _mv_cmp_display["market_value"].apply(_fmt_eur)
                                if _mv_cmp_bdate is not None:
                                    _mv_cmp_display.columns = ["Date", "Age", "Market Value", "Club"]
                                else:
                                    _mv_cmp_display.columns = ["Date", "Market Value", "Club"]
                                st.dataframe(_mv_cmp_display, width="stretch", hide_index=True)

                            # ── KPI cards for comparison player ──────────────
                            # same metrics as main player for direct comparison
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
                # Loads percentile 25/50/75 by age for the player's position.
                # Only available if position is known in dim_player.
                _mv_show_benchmark = st.checkbox(
                    t("mv_show_benchmark"), value=True, key="mv_benchmark"
                ) if _mv_pos else False

                _mv_benchmark = pd.DataFrame()
                if _mv_show_benchmark and _mv_pos:
                    _mv_benchmark = player_detail.get_market_value_benchmark(_mv_pos)

                # ── Chart ─────────────────────────────────────
                fig_mv, ax_mv = plt.subplots(figsize=(12, 5))
                fig_mv.patch.set_facecolor("#0e1117")
                ax_mv.set_facecolor("#0e1117")

                # layer 1 — benchmark band (drawn first so it sits behind everything)
                # The benchmark is indexed by age, so we convert age → date using
                # the player's birth_date to align it with the x-axis (dates)
                if not _mv_benchmark.empty and _mv_bdate is not None:
                    _mv_bdate_ts = pd.Timestamp(_mv_bdate)
                    _mv_benchmark["date"] = _mv_bdate_ts + pd.to_timedelta(
                        _mv_benchmark["age"] * 365.25, unit="D"
                    )
                    # limit benchmark to the player's actual data range
                    date_min = _mv_history["value_date"].min()
                    date_max = _mv_history["value_date"].max()
                    _mv_benchmark = _mv_benchmark[
                        (_mv_benchmark["date"] >= date_min) &
                        (_mv_benchmark["date"] <= date_max)
                    ]

                    ax_mv.fill_between(
                        _mv_benchmark["date"],
                        _mv_benchmark["p25"],
                        _mv_benchmark["p75"],
                        alpha=0.15, color="#2ecc71",
                        label=f"{_mv_pos} benchmark (P25–P75)",
                    )
                    ax_mv.plot(
                        _mv_benchmark["date"],
                        _mv_benchmark["median"],
                        color="#2ecc71", linewidth=1.2,
                        linestyle="--", alpha=0.6,
                        label=f"{_mv_pos} median",
                    )

                # layer 2 — main player curve
                # Step chart (where="post"): value stays flat until the next valuation,
                # which is faithful to how Transfermarkt actually works
                ax_mv.step(
                    _mv_history["value_date"],
                    _mv_history["market_value"],
                    where="post",
                    color="#e74c3c", linewidth=2.2,
                    label=_mv_selected, zorder=3,
                )

                # layer 3 — comparison player curve (no milestone markers to avoid clutter)
                if not _mv_cmp_history.empty and _mv_cmp_name:
                    ax_mv.step(
                        _mv_cmp_history["value_date"],
                        _mv_cmp_history["market_value"],
                        where="post",
                        color="#3498db", linewidth=1.8,
                        linestyle="-", alpha=0.8,
                        label=_mv_cmp_name, zorder=2,
                    )

                # layer 4 — transfer milestones (triangles on the main player's curve)
                # purple = permanent transfer, orange = loan/end_of_loan, green = free
                # unknown transfers are not shown — no economic information
                TRANSFER_COLORS = {
                    "transfer":    "#9b59b6",
                    "loan":        "#f39c12",
                    "end_of_loan": "#f39c12",
                    "free":        "#2ecc71",
                }

                if not _mv_transfers.empty:
                    _mv_transfers["transfer_date"] = pd.to_datetime(_mv_transfers["transfer_date"])
                    for _, tr in _mv_transfers.iterrows():
                        td = tr["transfer_date"]
                        if pd.isna(td):
                            continue
                        # skip unknown transfers — no economic information to show
                        transfer_color = TRANSFER_COLORS.get(tr["transfer_type"])
                        if transfer_color is None:
                            continue
                        before = _mv_history[_mv_history["value_date"] <= td]
                        if before.empty:
                            continue
                        mv_at_date = before.iloc[-1]["market_value"]
                        ax_mv.scatter(td, mv_at_date, marker="^", s=80, color=transfer_color, zorder=4)

                # layer 5 — injury milestones (red X on the main player's curve)
                if not _mv_injuries.empty:
                    _mv_injuries["date_from"] = pd.to_datetime(
                        _mv_injuries["date_from"], errors="coerce"
                    )
                    for _, inj in _mv_injuries.iterrows():
                        id_ = inj["date_from"]
                        if pd.isna(id_):
                            continue
                        # find the market value at the time of the injury
                        before = _mv_history[_mv_history["value_date"] <= id_]
                        if before.empty:
                            continue
                        mv_at_date = before.iloc[-1]["market_value"]
                        ax_mv.scatter(
                            id_, mv_at_date,
                            marker="x", s=60, color="#e74c3c",
                            linewidths=1.5, zorder=4,
                        )

                # ── Axis formatting ───────────────────────────
                ax_mv.yaxis.set_major_formatter(
                    plt.FuncFormatter(
                        lambda x, _: f"€{x/1_000_000:.1f}M" if x >= 1_000_000
                        else f"€{x/1_000:.0f}K"
                    )
                )
                ax_mv.tick_params(colors="white")
                ax_mv.xaxis.label.set_color("white")
                ax_mv.yaxis.label.set_color("white")
                for spine in ax_mv.spines.values():
                    spine.set_color("#444")
                ax_mv.grid(axis="y", color="#333", linewidth=0.5, linestyle="--")

                # ── Legend ────────────────────────────────────
                # Milestone markers (transfer/loan/injury) are not in the
                # automatic legend so we add them manually
                _mv_legend_extra = [
                    Line2D([0], [0], marker="^", color="w", markerfacecolor="#9b59b6",
                        markersize=8, label="Transfer", linestyle="None"),
                    Line2D([0], [0], marker="^", color="w", markerfacecolor="#f39c12",
                        markersize=8, label="Loan / End of loan", linestyle="None"),
                    Line2D([0], [0], marker="^", color="w", markerfacecolor="#2ecc71",
                        markersize=8, label="Free transfer", linestyle="None"),
                    Line2D([0], [0], marker="x", color="#e74c3c",
                        markersize=8, label="Injury", linestyle="None",
                        markeredgewidth=1.5),
                ]

                handles, labels = ax_mv.get_legend_handles_labels()
                ax_mv.legend(
                    handles + _mv_legend_extra,
                    labels + [line.get_label() for line in _mv_legend_extra],
                    facecolor="#1a1a2e", edgecolor="#444",
                    labelcolor="white", fontsize=9,
                    loc="upper left",
                )

                plt.tight_layout()
                st.pyplot(fig_mv)
                plt.close(fig_mv)

                st.caption(t("mv_caption"))

                if _mv_show_benchmark:
                    st.markdown(t("mv_benchmark_explain"))

     # ── Transfer History ─────────────────────────────────────

    with t_transfer_history:
        st.subheader(t("tab_transfer_history"))

        # ── Player selector ──────────────────────────────────
        # Load all players and filter by search input
        _th_all_players = player_detail.get_all_players()

        _th_search = st.text_input(
            "Search player",
            key="th_search",
            placeholder="Type a name…"
        )

        # filter players by search input — if empty, show all players
        if _th_search:
            _th_filtered = _th_all_players[
                _th_all_players["canonical_name"].str.contains(_th_search, case=False, na=False)
            ]
        else:
            _th_filtered = _th_all_players

        _th_names = _th_filtered["canonical_name"].tolist()

        # selectbox showing the filtered player names
        # if no players match the search, show "(no match)" and disable the selector
        _th_selected = st.selectbox(
            "Select player",
            options=_th_names if _th_names else ["(no match)"],
            key="th_select",
            disabled=not _th_names,
        )

        # get the full row of the selected player from the filtered DataFrame
        if _th_names:
            _th_row = _th_filtered[_th_filtered["canonical_name"] == _th_selected]
        else:
            _th_row = pd.DataFrame()

        if _th_row.empty:
            st.info("No player selected.")
        else:
            _th_cid = int(_th_row.iloc[0]["canonical_id"])

            # ── Load data from DB ─────────────────────────────
            _th_transfers = player_detail.get_transfer_history(_th_cid)
            _th_kpis      = player_detail.get_transfer_history_kpis(_th_cid)

            if _th_transfers.empty:
                st.info(t("transfer_no_data"))
            else:
                # ── KPI cards ─────────────────────────────────
                # total fees paid across all permanent transfers
                # most expensive single transfer with destination team and date
                # current team and number of distinct teams the player has been at

                # current team — exclude Retirado
                _th_current = player_detail.get_player_team_history(_th_cid, all_time=True)
                _th_current = _th_current[_th_current["team"] != "Retirado"]
                _th_current_team = _th_current.iloc[0]["team"] if not _th_current.empty else "—"

                # number of distinct teams the player has been at — exclude Retirado
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
                # shows full transfer history with fee and type
                # sorted by date descending — most recent first
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
                _th_display["transfer_type"] = _th_display["transfer_type"].map(_type_map).fillna(_th_display["transfer_type"])

                _th_display = _th_display[[
                    "season", "transfer_date", "from_team_name",
                    "to_team_name", "fee_euros", "transfer_type"
                ]]
                _th_display.columns = [
                    t("transfer_col_season"), t("transfer_col_date"),
                    t("transfer_col_from"), t("transfer_col_to"),
                    t("transfer_col_fee"), t("transfer_col_type")
                ]
                st.dataframe(_th_display, width="stretch", hide_index=True)

                st.caption(t("transfer_caption"))


# ════════════════════════════════════════════════════════════════════
# TAB 3 — SHOT INTELLIGENCE
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
# TAB 5 — MATCH CONTEXT (Weather · Attendance · Referees · Managers)
# ════════════════════════════════════════════════════════════════════
with tab_match_ctx:
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


# ════════════════════════════════════════════════════════════════════
# TAB 4 — PASS NETWORK  (fact_events · WhoScored)
# ════════════════════════════════════════════════════════════════════
with tab_passnet:
    st.header(t("pass_network"))
    st.caption(
        "WhoScored · Pitch: 105 m × 68 m · Home attacks →, away attacks ← · "
        "Node = avg pass-origin location (full match, subs included) · "
        "Receiver = next same-team event"
    )

    try:
        from mplsoccer import Pitch as _PnPitch
    except ImportError:
        st.error("Install mplsoccer: pip install mplsoccer")
        st.stop()

    _pn_comps = explore.get_competitions()
    pnf1, pnf2, pnf3 = st.columns([1, 1, 2])
    with pnf1:
        pn_comp = st.selectbox(
            t("competition"), _pn_comps or ["(none)"],
            key="pn_comp", disabled=not _pn_comps,
        )
    _pn_seasons = explore.get_seasons_for_competition(pn_comp) if _pn_comps else []
    with pnf2:
        pn_season = st.selectbox(
            t("season"), _pn_seasons or ["(no seasons)"],
            key="pn_season", disabled=not _pn_seasons,
        )

    _pn_matches = (
        pass_network.get_matches_with_passes(pn_comp, pn_season)
        if _pn_seasons else pd.DataFrame()
    )
    with pnf3:
        pn_match_label = st.selectbox(
            t("match"),
            _pn_matches["label"].tolist() if not _pn_matches.empty else ["(no matches)"],
            key="pn_match", disabled=_pn_matches.empty,
        )

    if _pn_matches.empty:
        st.info(t("no_pass_matches"))
    else:
        _pn_row = _pn_matches[_pn_matches["label"] == pn_match_label].iloc[0]
        _pn_mid = int(_pn_row["match_id"])

        pn_min_passes = st.slider(t("min_passes"), 1, 10, 3, key="pn_min_passes")

        def _draw_pass_network(
            team_name: str, team_id: int, node_color: str, flip: bool = False,
        ) -> None:
            nodes, edges = pass_network.get_pass_network(_pn_mid, team_id)
            if nodes.empty:
                st.info(t("no_pass_data"))
                return

            # Scale normalised 0-1 coords to the 105x68 custom pitch.
            # The away team is mirrored (180° rotation) so the two teams
            # face each other: home attacks →, away attacks ←.
            _nx = (1 - nodes["x"]) if flip else nodes["x"]
            _ny = (1 - nodes["y"]) if flip else nodes["y"]
            nodes = nodes.assign(px=_nx * 105, py=_ny * 68)
            pos = nodes.set_index("player_id")[["px", "py"]]

            edges_f = edges[edges["pass_count"] >= pn_min_passes]

            pitch = _PnPitch(
                pitch_type="custom", pitch_length=105, pitch_width=68,
                pitch_color="#1a472a", line_color="white", line_zorder=2,
            )
            fig, ax = pitch.draw(figsize=(8, 5.5))
            fig.patch.set_facecolor("#1a472a")

            max_count = int(edges_f["pass_count"].max()) if not edges_f.empty else 1
            for r in edges_f.itertuples():
                a, b = pos.loc[r.passer_id], pos.loc[r.receiver_id]
                pitch.lines(
                    a["px"], a["py"], b["px"], b["py"],
                    lw=0.5 + 4.5 * (r.pass_count / max_count),
                    color="white", alpha=0.25 + 0.55 * (r.pass_count / max_count),
                    zorder=2, ax=ax,
                )

            sizes = 200 + 900 * (nodes["passes"] / nodes["passes"].max())
            pitch.scatter(
                nodes["px"], nodes["py"], s=sizes,
                color=node_color, edgecolors="white", linewidths=1.2,
                alpha=0.95, zorder=3, ax=ax,
            )
            for r in nodes.itertuples():
                surname = str(r.player).split()[-1]
                pitch.annotate(
                    surname, xy=(r.px, r.py), ax=ax,
                    ha="center", va="center", color="white",
                    fontsize=7, fontweight="bold", zorder=4,
                )
            ax.set_title(team_name, color="white", fontsize=12, pad=10)
            st.pyplot(fig)
            plt.close(fig)

            id2name = nodes.set_index("player_id")["player"]
            m1, m2, m3 = st.columns(3)
            m1.metric(t("total_passes"), _fmt(nodes["passes"].sum()))
            m2.metric(t("pass_pairs"), _fmt(len(edges)))
            if not edges.empty:
                top = edges.loc[edges["pass_count"].idxmax()]
                m3.metric(
                    t("top_connection"),
                    f"{int(top['pass_count'])}",
                    delta=(
                        f"{id2name.get(int(top['passer_id']), '?')} ↔ "
                        f"{id2name.get(int(top['receiver_id']), '?')}"
                    ),
                    delta_color="off",
                )

        pn_home, pn_away = st.columns(2)
        with pn_home:
            _draw_pass_network(str(_pn_row["home"]), int(_pn_row["home_team_id"]), "#3498db")
        with pn_away:
            _draw_pass_network(str(_pn_row["away"]), int(_pn_row["away_team_id"]), "#e74c3c", flip=True)

        st.caption(
            "Source: fact_events (WhoScored) · Only successful passes where the next "
            "event belongs to the same team · Edge width/opacity ∝ passes between the "
            "pair (both directions combined) · Node size ∝ passes made"
        )


# ════════════════════════════════════════════════════════════════════
# TAB 6 — STADIUMS  (dim_stadium · Transfermarkt · SCD2)
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
# TAB 6 — PIPELINE MONITORING
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
# TAB 7 — WIZARD (writes to the database — read-only exception)
# ════════════════════════════════════════════════════════════════════
with tab_wizard:
    wizard_view.render()
