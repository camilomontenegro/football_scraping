"""
dashboard/app.py
================
Streamlit dashboard entrypoint for the football scraping project.

Pages (dashboard/views/ — one module per page, each exposing render()):
  1. Exploration:        results, player stats, shots by source, events, standings
  2. Players:            discipline, goalkeepers, player detail, injuries
  3. Shot Intelligence:  pitch heatmap, player finishing, set-piece specialists
  4. Pass Network:       WhoScored pass maps per match
  5. Match Context:      weather, attendance, referees, managers
  6. Stadiums:           dim_stadium browser (Transfermarkt, SCD2)
  7. Pipeline:           DB metrics, scanner, coverage, player review, recent matches
  8. Wizard:             interactive DB operations

Navigation uses st.navigation(position="top") instead of st.tabs: st.tabs
executed every tab's queries on every rerun, while st.navigation only runs
the selected page — this is what keeps load times down.

Read-only — no scraper or loader is triggered from this UI.

Run from project root:
    streamlit run dashboard/app.py

Smoke test (bare mode): `python dashboard/app.py` renders every page
sequentially, like the old single-file app (exit 0 = no render errors).
"""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
from streamlit import runtime as _st_runtime

# Make sibling modules (loaders/, pipeline_runner.py, etc.) importable when run
# as `streamlit run dashboard/app.py` from `football_scraping/`.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

<<<<<<< HEAD
from dashboard import analytics, db, explore, pass_network, player_detail, scanner, stadium_fill, stadium_fill_svg, wizard_view
from dashboard.i18n import t, get_lang, LANGUAGES, DEFAULT_LANG

if "app_language" not in st.session_state:
    st.session_state["app_language"] = DEFAULT_LANG
=======
from dashboard import db
from dashboard.i18n import t, LANGUAGES
from dashboard.views import (
    exploration, players, shot_intelligence, pass_network,
    match_context, stadiums, pipeline, wizard,
)
>>>>>>> origin/main

st.set_page_config(
    page_title=t("page_title"),
    page_icon="⚽",
    layout="wide",
)

# Unlike st.tabs, st.navigation drops the widget state of pages that are not
# rendered on the current rerun. Re-assigning every session_state entry pins
# it, so filters keep their values when the user switches pages. Button-like
# widgets (e.g. dataframe row selections) refuse assignment — skip those.
for _k in list(st.session_state.keys()):
    try:
        st.session_state[_k] = st.session_state[_k]
    except Exception:
        pass

# ── Language selector (sidebar) ──────────────────────────
_lang_label = st.sidebar.selectbox(
    t("lang_label"),
    list(LANGUAGES.keys()),
    index=0,
    key="lang_selector",
)
st.session_state["app_language"] = LANGUAGES[_lang_label]

# ─────────────────────────────────────────────
# DB-unreachable guard (runs once on each rerun)
# ─────────────────────────────────────────────
try:
    st.session_state["_db_summary"] = db.get_db_summary()
except Exception:
    st.error(t("db_error"))
    st.stop()

_PAGES = [
    (exploration,       "tab_exploration",       "exploration"),
    (players,           "tab_players",           "players"),
    (shot_intelligence, "tab_shot_intelligence", "shot-intelligence"),
    (pass_network,      "tab_pass_network",      "pass-network"),
    (match_context,     "tab_match_context",     "match-context"),
    (stadiums,          "tab_stadiums",          "stadiums"),
    (pipeline,          "tab_pipeline",          "pipeline"),
    (wizard,            "tab_wizard",            "wizard"),
]

<<<<<<< HEAD

def _format_name_era_span(vf, vt, is_current: bool) -> str:
    """Human-readable year span for a stadium name era."""
    if is_current and pd.isna(vf) and pd.isna(vt):
        return t("stadium_name_current")
    parts: list[str] = []
    if pd.notna(vf):
        parts.append(f"{t('stadium_name_from')} {int(vf)}")
    if pd.notna(vt):
        parts.append(f"{t('stadium_name_until')} {int(vt)}")
    if parts:
        return " · ".join(parts)
    return "—"


def _render_stadium_name_history(row: pd.Series) -> None:
    master_id = row.get("master_stadium_id")
    if pd.isna(master_id):
        return
    hist = explore.get_stadium_name_history(int(master_id))
    st.markdown(f"**{t('stadium_name_history')}**")
    if hist.empty:
        st.caption(t("stadium_name_history_none"))
        return
    has_timeline = len(hist) > 1 or hist["valid_from_year"].notna().any() or hist["valid_to_year"].notna().any()
    if not has_timeline:
        st.caption(t("stadium_name_history_none"))
        return
    for _, h in hist.iterrows():
        name = str(h["stadium_name"])
        span = _format_name_era_span(h["valid_from_year"], h["valid_to_year"], bool(h["is_current"]))
        if bool(h["is_current"]) and span != t("stadium_name_current"):
            line = f"- **{name}** — {span} · *{t('stadium_name_current')}*"
        elif bool(h["is_current"]):
            line = f"- **{name}** — *{t('stadium_name_current')}*"
        else:
            line = f"- {name} — {span}"
        st.markdown(line)


def _render_stadium_location_map(lat, lon) -> None:
    """Mapa con la ubicación del estadio (coordenadas en BD)."""
    if pd.isna(lat) or pd.isna(lon):
        st.markdown(f"**{t('stadium_location')}**")
        st.caption(t("stadium_no_coords"))
        return

    lat_f, lon_f = float(lat), float(lon)
    map_col, _ = st.columns([0.48, 1.52])
    with map_col:
        st.markdown(f"**{t('stadium_location')}**")
        st.caption(f"{lat_f:.5f}, {lon_f:.5f}")
        st.map(
            pd.DataFrame({"lat": [lat_f], "lon": [lon_f]}),
            zoom=14,
            height=240,
            use_container_width=True,
        )
        st.link_button(
            t("stadium_open_maps"),
            f"https://www.google.com/maps?q={lat_f},{lon_f}",
            use_container_width=True,
        )


def _render_stadium_detail(row: pd.Series) -> None:
    """Detail panel: photo, metadata and external links for one stadium row."""
    st.subheader(t("stadium_detail"))
    img_col, info_col = st.columns([0.55, 1.45])

    with img_col:
        image_url = row.get("image_url")
        lat, lon = row.get("latitude"), row.get("longitude")
        if pd.notna(image_url) and str(image_url).strip():
            st.image(
                str(image_url),
                caption=str(row.get("stadium_name") or ""),
                width=280,
            )
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
        if pd.notna(row.get("timezone")):
            meta.append(f"**Timezone:** {row['timezone']}")
        if pd.notna(row.get("altitude_m")):
            meta.append(f"**Altitude:** {int(row['altitude_m'])} m")
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

    st.divider()
    _render_stadium_location_map(lat, lon)
    st.divider()
    _render_stadium_name_history(row)


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
    season_opts = ([t("all_seasons")] + _seasons) if all_seasons else (_seasons or [t("no_seasons_paren")])
    with sc2:
        _season_sel = st.selectbox(
            t("season"), season_opts,
            key=f"{key_prefix}_season",
            disabled=not _seasons,
        )
    _season = None if (_season_sel in (t("all_seasons"), t("no_seasons_paren")) or not _seasons) else _season_sel
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
            t("season"), seasons or [t("no_seasons_in_db")], key="ex_season",
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
                st.info(t("no_shot_data_pipeline"))
            else:
                st.dataframe(df, width='stretch')
                st.caption(t("caption_player_stats"))

        # ── Shots by source ───────────────────────────────
        with t_shots:
            df = explore.get_shots_by_source(season, team, competition)
            if df.empty:
                _empty_info()
            else:
                st.dataframe(df, width='stretch')
                st.bar_chart(df.set_index("data_source")["shots"])
                st.caption(t("caption_shots_by_source"))

        # ── Events ────────────────────────────────────────
        with t_events:
            df = explore.get_events_summary(season, team, competition)
            if df.empty:
                st.info(t("no_event_data_selection"))
            else:
                st.dataframe(df, width='stretch')
                st.caption(t("caption_events_summary"))

        # ── Standings (formerly Teams tab) ────────────────
        with t_standings:
            df = explore.get_team_standings(season, team, competition)
            if df.empty:
                st.info(t("no_match_data_pipeline"))
            else:
                # A selected team yields a single standings row whose "played"
                # already equals its match count; only halve for the full table
                # (each match appears twice, once per team).
                _p_sum = int(df["p"].sum())
                total_matches = _p_sum if len(df) <= 1 else _p_sum // 2
                total_goals = int(df["gf"].sum())
                avg_goals = round(total_goals / total_matches, 2) if total_matches else 0
                avg_xg = round(float(df["xg_for"].sum()) / total_matches, 2) if total_matches else 0

                sm1, sm2, sm3, sm4 = st.columns(4)
                sm1.metric(t("tab_teams"), len(df))
                sm2.metric(t("total_goals"), _fmt(total_goals))
                sm3.metric(t("avg_goals_match"), f"{avg_goals:.2f}")
                sm4.metric(t("avg_xg_match"), f"{avg_xg:.2f}")

                display_df = df.rename(columns={
                    "p": t("col_played"), "w": t("col_won"), "d": t("col_drawn"), "l": t("col_lost"),
                    "gf": t("col_gf"), "ga": t("col_ga"), "gd": t("col_gd"),
                    "xg_for": t("col_xg_for"), "xg_against": t("col_xg_against"),
                    "shots_for": t("col_shots_for"), "shots_against": t("col_shots_against"),
                })
                st.dataframe(display_df, width='stretch')
                st.caption(t("caption_standings"))


# ════════════════════════════════════════════════════════════════════
# TAB 2 — PLAYERS  (Discipline · Goalkeepers · Player Detail · Injuries)
# ════════════════════════════════════════════════════════════════════
with tab_players:
    st.header(t("tab_players"))

    t_discipline, t_cards, t_gk, t_detail, t_injuries = st.tabs(
        [t("tab_players"), t("cards_fouls_section"), t("tab_goalkeepers"),
         t("tab_player_detail"), t("tab_injuries")]
    )

    # ── Discipline ──────────────────────────────────────────
    with t_discipline:
        _pl_comp, _pl_season, _pl_team = _tab_selectors("players", all_seasons=True)

        df = explore.get_player_discipline(_pl_season, _pl_team, _pl_comp)
        if df.empty:
            st.info(t("no_player_data"))
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
                        t("sort_order"), [t("descending"), t("ascending")],
                        horizontal=True, key="pl_top_sort",
                    ) == t("ascending")
                    top10 = top10.sort_values("goals", ascending=_pl_sort_asc)
                    fig_pl, ax_pl = plt.subplots(figsize=(10, max(3, len(top10) * 0.45)))
                    fig_pl.patch.set_facecolor("#0e1117")
                    ax_pl.set_facecolor("#0e1117")
                    ax_pl.barh(top10["player"], top10["goals"], color="#3498db")
                    ax_pl.set_xlabel(t("goals"), color="white")
                    ax_pl.tick_params(colors="white")
                    for spine in ax_pl.spines.values():
                        spine.set_color("#444")
                    ax_pl.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_pl)
                    plt.close(fig_pl)

            st.caption(t("caption_discipline_rows"))

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
                st.info(t("no_gk_data"))
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
                    "goalkeeper": t("col_goalkeeper"),
                    "team": t("team"),
                    "matches_played": t("col_matches"),
                    "goals_allowed": t("col_goals_allowed"),
                    "shots_faced": t("col_shots_faced"),
                    "saves": t("col_saves"),
                    "save_pct": t("col_save_pct_formula"),
                    "xg_conceded": t("col_xg_conceded"),
                    "goals_saved_above_expected": t("col_gsae"),
                    "clean_sheets": t("clean_sheets"),
                })
                st.dataframe(display_df, width='stretch')
                st.caption(t("caption_gk_stats"))

    # ── Player Detail ───────────────────────────────────────
    with t_detail:
        try:
            from mplsoccer import Pitch as _PlayerPitch
        except ImportError:
            st.error(t("install_mplsoccer"))
            st.stop()

        _pd_all = player_detail.get_all_players()
        _pd_search = st.text_input(t("search_player"), key="pd_search", placeholder=t("search_player_ph"))
        _pd_filtered = _pd_all[
            _pd_all["canonical_name"].str.contains(_pd_search, case=False, na=False)
        ] if _pd_search else _pd_all
        _pd_names = _pd_filtered["canonical_name"].tolist()

        _pd_selected_name = st.selectbox(
            t("select_player"), options=_pd_names if _pd_names else [t("no_player_match")],
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
            _PD_ALL = "All"
            _sm_seasons_all = [_PD_ALL] + player_detail.get_player_shot_seasons(_pd_cid)
            _pd_season_sel = st.selectbox(
                t("season"), _sm_seasons_all, key="pd_season_global",
                format_func=lambda s: t("filter_all") if s == _PD_ALL else s,
            )
            _pd_season_val = None if _pd_season_sel == _PD_ALL else _pd_season_sel

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
                    _ft_items.append(
                        f"**{t('born')}:** {_bd.strftime('%d/%m/%Y')} ({_age} {t('years_old')})"
                    )
                if _pd["nationality"]:
                    _ft_items.append(f"**{t('nationality')}:** {_pd['nationality']}")
                if _pd["position"]:
                    _ft_items.append(f"**{t('position')}:** {_pd['position']}")
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
            _season_label = (
                _pd_season_sel if _pd_season_sel != _PD_ALL else t("all_seasons")
            )
            st.markdown(
                f'<h3 style="margin-bottom:0.3em">{t("statistics_title").format(season=_season_label)}</h3>',
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
            _stat_card(t("shots"),      _summary["shots"],   _r1c2)
            _stat_card(t("col_xg"),     f'{_summary["xg"]:.2f}', _r1c3)
            _stat_card(t("matches"), _summary["matches"], _r1c4)

            # Row 2: Penalties · Penalty Goals · Yellow · Red
            _r2c1, _r2c2, _r2c3, _r2c4 = st.columns(4)
            _stat_card(t("penalties"),      _summary["penalties"],     _r2c1)
            _stat_card(t("penalty_goals"),  _summary["penalty_goals"], _r2c2)
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
            _stat_card(t("conversion_pct"), f"{_conv}%", _r3c1)
            _stat_card(t("goals_minus_xg"),   f"{_g_minus_xg_total:+.2f}", _r3c2)
            _stat_card(t("goals_per_match"),   f"{_gpm:.2f}", _r3c3)
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
                _CMP_LEAGUE, _CMP_PLAYER = "__league__", "__player__"
                with _cmp_col1:
                    _cmp_mode = st.radio(
                        t("compare_with"),
                        [_CMP_LEAGUE, _CMP_PLAYER],
                        format_func=lambda m: (
                            t("compare_mode_league").format(comp=_comp_name)
                            if m == _CMP_LEAGUE else t("compare_mode_player")
                        ),
                        key="pd_cmp_mode",
                        horizontal=True,
                    )

                _cmp_label = f"{_comp_name} avg"
                _cmp_vals = None

                if _cmp_mode == _CMP_PLAYER:
                    with _cmp_col2:
                        _cmp_search = st.text_input(
                            t("search_rival"), key="pd_cmp_search", placeholder=t("search_player_ph")
                        )
                        _cmp_candidates = _pd_all[
                            (_pd_all["canonical_name"].str.contains(_cmp_search, case=False, na=False))
                            & (_pd_all["canonical_id"] != _pd_cid)
                        ] if _cmp_search else _pd_all[_pd_all["canonical_id"] != _pd_cid]
                        _cmp_names = _cmp_candidates["canonical_name"].tolist()
                        _cmp_selected = st.selectbox(
                            t("select_player_compare"),
                            _cmp_names if _cmp_names else [t("no_player_match")],
                            key="pd_cmp_select",
                            disabled=not _cmp_names,
                        )
                    if _cmp_names and _cmp_selected != t("no_player_match"):
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
                            t("col_metric"): _labels,
                            _pd["canonical_name"]: [
                                _fmt_val(_p_vals[i], i) for i in range(_n)
                            ],
                            _cmp_label: [
                                _fmt_val(_cmp_vals[i], i) for i in range(_n)
                            ],
                        })
                        st.dataframe(_rv_df, width="stretch", hide_index=True)
                        _caption = (
                            t("caption_radar_player")
                            if _cmp_mode == _CMP_PLAYER
                            else t("caption_radar_league")
                        )
                        st.caption(_caption)
                else:
                    if _cmp_mode == _CMP_PLAYER:
                        st.info(t("no_shot_cmp_player"))
                    else:
                        st.info(t("not_enough_league"))

                st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 4 — SHOT MAP  (kept from before)
            # ═══════════════════════════════════════════════════════
            st.subheader(t("shot_map"))
            _sm_sources = [_PD_ALL] + player_detail.get_player_shot_sources(_pd_cid)
            _smc1, _smc2 = st.columns(2)
            with _smc1:
                _sm_source = st.selectbox(
                    t("source"), _sm_sources, key="pd_sm_source",
                    format_func=lambda s: t("filter_all") if s == _PD_ALL else s,
                )
            _sm_matches_df = player_detail.get_player_shot_matches(
                _pd_cid, _pd_season_sel, _sm_source
            )
            _sm_match_options = {_PD_ALL: None}
            for _match in _sm_matches_df.itertuples(index=False):
                _date = (
                    _match.match_date.strftime("%Y-%m-%d")
                    if pd.notna(_match.match_date) else t("unknown_date")
                )
                _home = _match.home_team or t("home_label")
                _away = _match.away_team or t("away_label")
                _score = (
                    f" {_match.home_score}-{_match.away_score}"
                    if pd.notna(_match.home_score) and pd.notna(_match.away_score) else ""
                )
                _comp = f" | {_match.competition}" if _match.competition else ""
                _shots_label = f" | {_match.shots} {t('shots').lower()}"
                _label = f"{_date} | {_home}{_score} {_away}{_comp}{_shots_label}"
                _sm_match_options[_label] = int(_match.match_id)
            with _smc2:
                _sm_match_label = st.selectbox(
                    t("match"),
                    list(_sm_match_options.keys()),
                    key="pd_sm_match",
                    disabled=_sm_matches_df.empty,
                    format_func=lambda k: t("all_matches_filter") if k == _PD_ALL else k,
                )
            _sm_match_id = _sm_match_options.get(_sm_match_label)

            _shots_df = player_detail.get_player_shots(
                _pd_cid, _pd_season_sel, _sm_source, _sm_match_id
            )
            if _shots_df.empty:
                st.info(t("no_shot_data_selection"))
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
                    ax=_ax_sm, zorder=3, label=t("shot_map_no_goal"))
                _pitch.scatter(_x[_is_goal], _y[_is_goal], s=_sizes[_is_goal],
                    color="red", edgecolors="white", linewidths=0.5, alpha=0.9,
                    ax=_ax_sm, zorder=4, label=t("shot_map_goal"))
                _ax_sm.legend(facecolor="#1a472a", labelcolor="white", loc="upper right", fontsize=8)
                st.pyplot(_fig_sm)
                plt.close(_fig_sm)
                _st1, _st2, _st3, _st4 = st.columns(4)
                _total_shots = len(_shots_df)
                _total_goals = int(_is_goal.sum())
                _total_xg = round(float(_xg_arr.sum()), 2)
                _g_minus_xg = round(_total_goals - _total_xg, 2)
                _st1.metric(t("shots"), _total_shots)
                _st2.metric(t("goals"), _total_goals)
                _st3.metric(t("col_xg"), _total_xg)
                _st4.metric(t("goals_minus_xg"), f"{_g_minus_xg:+.2f}")
            st.divider()

            # ═══════════════════════════════════════════════════════
            # SECTION 4b — ACTION HEATMAP (WhoScored events)
            # ═══════════════════════════════════════════════════════
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
            st.subheader(t("seasonal_stats"))
            _ss_df = player_detail.get_player_seasonal_stats(_pd_cid)
            if _ss_df.empty:
                st.info(t("no_shot_data_player"))
            else:
                _ss_df.columns = [t("col_season_short"), t("col_competition"), t("col_shots"), t("goals"), t("col_xg")]
                st.dataframe(_ss_df, width="stretch", hide_index=True)
            st.divider()

            st.subheader(t("injury_history"))
            _inj_df = player_detail.get_player_injuries(_pd_cid)
            if _inj_df.empty:
                st.info(t("no_injury_records"))
            else:
                _inj_df.columns = [t("col_season_short"), t("injury_type"), t("date_from"), t("date_until"),
                                    t("days_absent"), t("matches_missed")]
                st.dataframe(_inj_df, width="stretch", hide_index=True)
            st.divider()

            with st.expander(t("mdm_expander")):
                _mdm_df = player_detail.get_player_mdm(_pd_cid)
                if _mdm_df.empty:
                    st.info(t("no_source_aliases"))
                else:
                    _mdm_df.columns = [t("mdm_source"), t("mdm_name_used"), t("mdm_source_id"), t("mdm_score"), t("mdm_resolved")]
                    st.dataframe(_mdm_df, width="stretch", hide_index=True)

    # ── Injuries (aggregate) ────────────────────────────────
    with t_injuries:
        _inj_comp, _inj_season, _inj_team = _tab_selectors("injuries", all_seasons=True)

        df = explore.get_injuries_standalone(_inj_season, _inj_team)
        if df.empty:
            st.info(t("no_injury_data"))
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
            df_render["date_until"] = df_render["date_until"].fillna(t("ongoing")).astype(str)
            st.dataframe(df_render, width='stretch')

            breakdown = explore.get_injury_type_breakdown(_inj_season, _inj_team)
            if not breakdown.empty:
                st.subheader(t("top_injury_types"))
                _inj_sort_asc = st.radio(
                    t("sort_order"), [t("descending"), t("ascending")],
                    horizontal=True, key="inj_type_sort",
                ) == t("ascending")
                breakdown = breakdown.sort_values("count", ascending=_inj_sort_asc).head(10)
                fig_inj, ax_inj = plt.subplots(figsize=(10, max(3, len(breakdown) * 0.45)))
                fig_inj.patch.set_facecolor("#0e1117")
                ax_inj.set_facecolor("#0e1117")
                ax_inj.barh(breakdown["injury_type"], breakdown["count"], color="#e67e22")
                ax_inj.set_xlabel(t("count"), color="white")
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

            st.caption(t("caption_injuries"))


# ════════════════════════════════════════════════════════════════════
# TAB 3 — SHOT INTELLIGENCE
# ════════════════════════════════════════════════════════════════════
with tab_shot:
    st.header(t("shot_intelligence"))
    st.caption(t("si_caption_coords"))

    # ── mplsoccer availability guard ─────────────────────────────
    try:
        from mplsoccer import Pitch as _Pitch
    except ImportError:
        st.error(t("install_mplsoccer"))
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
            t("competition"),
            _si_competitions or [t("none_option")],
            key="si_competition",
            disabled=not _si_competitions,
        )
    _si_seasons = explore.get_seasons_for_competition(si_competition) if _si_competitions else []
    with sf2:
        si_season = st.selectbox(
            t("season"),
            _si_seasons or [t("no_seasons_in_db")],
            key="si_season",
            disabled=not _si_seasons,
        )
    _si_teams = explore.get_teams_for_season(si_season, si_competition) if _si_seasons else []
    with sf3:
        si_team_choice = st.selectbox(
            t("team"), [t("all_teams")] + _si_teams,
            key="si_team",
            disabled=not _si_teams,
        )
    _si_team_name = None if si_team_choice == t("all_teams") else si_team_choice
    _si_team_id = analytics._resolve_team_id(_si_team_name)
    _si_competition_val = si_competition if _si_competitions else None

    with sf4:
        metric_choice = st.radio(
            t("metric_label"),
            [t("si_metric_avg_xg"), t("si_metric_conversion")],
            key="si_metric",
        )
    metric_col = "avg_xg" if metric_choice == t("si_metric_avg_xg") else "conversion_rate"
    metric_label = t("si_avg_xg_label") if metric_col == "avg_xg" else t("si_conversion_label")

    if not _si_seasons:
        st.info(t("no_seasons"))
    else:
        # ── Section 1 — Pitch Danger Heatmap ─────────────────────
        st.subheader(t("pitch_danger_heatmap"))

        hm_df = analytics.get_heatmap_data(si_season, _si_team_id, _si_competition_val)

        if hm_df.empty:
            st.info(t("no_shots_coords"))
        else:
            scope = si_team_choice
            hm_title = t("hm_title").format(metric=metric_label, season=si_season, scope=scope)

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

            with st.expander(t("zone_data_expander")):
                st.dataframe(
                    hm_df[["x_band", "y_band", "shots", "goals", "avg_xg", "conversion_rate"]],
                    width='stretch',
                )

        st.divider()

        # ── Section 2 — Player Finishing Quality ──────────────────
        st.subheader(t("player_finishing"))
        st.caption(t("si_finishing_caption"))

        pf_df = analytics.get_player_finishing(si_season, _si_team_id, _si_competition_val)

        if pf_df.empty:
            st.info(t("no_players_20_shots"))
        else:
            _pf_sort_asc = st.radio(
                t("sort_order"), [t("descending"), t("ascending")],
                horizontal=True, key="si_finishing_sort",
            ) == t("ascending")
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
            ax2.set_xlabel(t("goals_minus_xg"), color="white")
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
            st.info(t("no_setpiece_data"))
        else:
            display_sp = sp_df.rename(columns={
                "player":        t("col_player"),
                "team":          t("team"),
                "penalty_goals": t("col_penalty_goals"),
                "freekick_goals": t("col_freekick_goals"),
                "openplay_goals":       t("col_openplay_goals"),
                "setpiece_other_goals": t("col_setpiece_other"),
                "total_goals":          t("col_total_goals"),
            }).sort_values(t("col_penalty_goals"), ascending=False)
            st.dataframe(display_sp, width='stretch')

            _sp_players = explore.get_players_for_season(si_season, _si_team_id, _si_competition_val)
            _sp_labels = [t("all_players")] + [name for name, _ in _sp_players]
            _sp_id_map = {name: pid for name, pid in _sp_players}

            si_player_name = st.selectbox(
                t("player_drilldown"), _sp_labels, key="si_player"
            )
            si_player_id = _sp_id_map.get(si_player_name)

            if si_player_id is not None:
                bucket_df = analytics.get_setpiece_goals(
                    si_season, _si_team_id, player_id=si_player_id,
                    competition=_si_competition_val,
                )
                if not bucket_df.empty:
                    st.bar_chart(bucket_df.set_index("situation_bucket")["goals"])

            st.caption(t("si_setpiece_caption"))

        st.divider()


# ════════════════════════════════════════════════════════════════════
# TAB 5 — MATCH CONTEXT (Weather · Attendance · Referees · Managers)
# ════════════════════════════════════════════════════════════════════
with tab_match_ctx:
    st.header(t("tab_match_context"))
    _mc_comp, _mc_season, _mc_team = _tab_selectors("match_ctx")

    t_match, t_weather, t_attendance, t_referees, t_managers, t_chalk = st.tabs(
        [t("match_detail_section"), t("weather_section"), t("attendance_section"),
         t("referees_section"), t("managers_section"), t("chalkboard_section")]
    )

    # ── Match (per-match context card) ───────────────────────
    with t_match:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            _df_matches = explore.get_matches_for_context(
                _mc_season, _mc_team, _mc_comp
            )
            if _df_matches.empty:
                st.info(t("no_matches_found"))
            else:
                def _mc_match_label(_r) -> str:
                    _d = _r["match_date"]
                    _ds = _d.strftime("%Y-%m-%d") if hasattr(_d, "strftime") else str(_d)
                    _sc = ""
                    if pd.notna(_r["home_score"]) and pd.notna(_r["away_score"]):
                        _sc = f" {int(_r['home_score'])}-{int(_r['away_score'])}"
                    return f"{_ds} · {_r['home_team']}{_sc} {_r['away_team']}"

                _mc_labels = [_mc_match_label(_r) for _, _r in _df_matches.iterrows()]
                _mc_idx = st.selectbox(
                    t("match_select"), range(len(_mc_labels)),
                    format_func=lambda i: _mc_labels[i], key="mc_match_pick",
                )
                _mid = int(_df_matches.iloc[_mc_idx]["match_id"])
                _ctx = explore.get_match_context(_mid)

                if not _ctx:
                    st.info(t("no_data"))
                else:
                    _hs, _as = _ctx.get("home_score"), _ctx.get("away_score")
                    _score = (
                        f"{int(_hs)} – {int(_as)}"
                        if pd.notna(_hs) and pd.notna(_as) else "—"
                    )
                    _sb1, _sb2, _sb3 = st.columns([3, 1, 3])
                    _sb1.markdown(f"### {_ctx.get('home_team') or '—'}")
                    _sb2.markdown(
                        f"<h2 style='text-align:center;margin:0'>{_score}</h2>",
                        unsafe_allow_html=True,
                    )
                    _sb3.markdown(
                        f"<h3 style='text-align:right;margin:0'>"
                        f"{_ctx.get('away_team') or '—'}</h3>",
                        unsafe_allow_html=True,
                    )
                    _md = _ctx.get("match_date")
                    _md_s = _md.strftime("%Y-%m-%d") if hasattr(_md, "strftime") else str(_md or "")
                    _meta = [
                        x for x in (
                            _md_s, _mc_comp,
                            str(_ctx.get("stadium") or _ctx.get("venue_name") or ""),
                        ) if x
                    ]
                    st.caption(" · ".join(_meta))
                    st.divider()

                    _left, _right = st.columns(2)

                    with _left:
                        st.subheader(t("weather_section"))
                        _temp = _ctx.get("temperature_c")
                        if _temp is None or pd.isna(_temp):
                            st.caption(t("no_weather_data"))
                        else:
                            def _wfmt(v, unit, dec=0):
                                if v is None or pd.isna(v):
                                    return "—"
                                return f"{float(v):.{dec}f}{unit}"
                            _w1, _w2 = st.columns(2)
                            _w1.metric(t("temperature"), _wfmt(_temp, " °C", 1))
                            _w2.metric(t("humidity"), _wfmt(_ctx.get("humidity_pct"), "%"))
                            _w3, _w4 = st.columns(2)
                            _w3.metric(t("precipitation"), _wfmt(_ctx.get("precipitation_mm"), " mm", 1))
                            _w4.metric(t("wind"), _wfmt(_ctx.get("wind_speed_kmh"), " km/h"))

                        st.subheader(t("match_officials"))
                        st.metric(t("referee"), str(_ctx.get("referee") or "—"))
                        _cards = explore.get_match_cards(_mid)
                        if not _cards.empty:
                            _cy = int(pd.to_numeric(_cards["yellow_cards"], errors="coerce").fillna(0).sum())
                            _cr = int(pd.to_numeric(_cards["red_cards"], errors="coerce").fillna(0).sum())
                            _cc1, _cc2 = st.columns(2)
                            _cc1.metric(t("yellow_cards"), _cy)
                            _cc2.metric(t("red_cards"), _cr)
                            st.dataframe(
                                _cards.rename(columns={
                                    "team": t("team"),
                                    "yellow_cards": t("yellow_cards"),
                                    "red_cards": t("red_cards"),
                                }),
                                width="stretch", hide_index=True,
                            )

                        st.subheader(t("managers_section"))
                        _mg1, _mg2 = st.columns(2)
                        _mg1.metric(t("home_label"), str(_ctx.get("manager_home") or "—"))
                        _mg2.metric(t("away_label"), str(_ctx.get("manager_away") or "—"))

                    with _right:
                        st.subheader(t("attendance_section"))
                        _att = _ctx.get("attendance")
                        _cap = _ctx.get("capacity")
                        _fill = _ctx.get("fill_pct")
                        _has_att = _att is not None and not pd.isna(_att)
                        _has_cap = _cap not in (None, 0) and not pd.isna(_cap)
                        if _has_att and _has_cap and _fill is not None:
                            stadium_fill_svg.render_stadium_fill_svg(
                                float(_fill),
                                attendance=int(_att),
                                capacity=int(_cap),
                                title=str(_ctx.get("stadium") or ""),
                                subtitle=f"{_ctx.get('home_team')} vs {_ctx.get('away_team')}",
                                height=430,
                            )
                            _am1, _am2 = st.columns(2)
                            _am1.metric(t("attendance"), _fmt(int(_att)))
                            _am2.metric(t("total_capacity"), _fmt(int(_cap)))
                        elif _has_att:
                            st.metric(t("attendance"), _fmt(int(_att)))
                            st.caption(t("stadium_fill_no_cap"))
                        else:
                            st.caption(t("no_attendance_data"))

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

                # ── Temperature over the season (line chart) ──────
                df_w = explore.get_weather_by_match(_mc_season, _mc_team, _mc_comp)
                if not df_w.empty and "match_date" in df_w.columns:
                    st.subheader(t("temp_over_season"))
                    df_w["match_date"] = pd.to_datetime(df_w["match_date"])
                    df_w["temperature_c"] = pd.to_numeric(
                        df_w["temperature_c"], errors="coerce"
                    )
                    chart_df = (
                        df_w.groupby("match_date")["temperature_c"]
                        .mean()
                        .sort_index()
                        .rename("°C")
                        .to_frame()
                    )
                    st.line_chart(chart_df)

                # ── Temperature by stadium / venue ────────────────
                df_venue = explore.get_weather_by_venue(
                    _mc_season, _mc_team, _mc_comp,
                )
                if not df_venue.empty:
                    st.subheader(t("temperature") + " — " + t("stadiums"))
                    _venue_sort = st.radio(
                        t("sort_order"),
                        [t("descending"), t("ascending")],
                        horizontal=True, key="weather_venue_sort",
                    )
                    _venue_asc = _venue_sort == t("ascending")
                    df_venue_sorted = df_venue.sort_values(
                        "avg_temp", ascending=_venue_asc
                    ).head(20)

                    fig_wv, ax_wv = plt.subplots(
                        figsize=(10, max(3, len(df_venue_sorted) * 0.5))
                    )
                    fig_wv.patch.set_facecolor("#0e1117")
                    ax_wv.set_facecolor("#0e1117")

                    _venue_labels = [
                        (
                            f"{r.venue} ({r.home_team})"
                            if pd.notna(r.home_team) and r.home_team
                            else str(r.venue)
                        )
                        for r in df_venue_sorted.itertuples()
                    ]
                    _avg_temps = df_venue_sorted["avg_temp"].values.astype(float)
                    _min_temps = df_venue_sorted["min_temp"].values.astype(float)
                    _max_temps = df_venue_sorted["max_temp"].values.astype(float)

                    # Error bars: min to max range
                    _err_low = _avg_temps - _min_temps
                    _err_high = _max_temps - _avg_temps

                    _bar_colors = [
                        "#e74c3c" if t > 25 else "#3498db" if t < 10 else "#f39c12"
                        for t in _avg_temps
                    ]
                    ax_wv.barh(
                        _venue_labels, _avg_temps,
                        xerr=[_err_low, _err_high],
                        color=_bar_colors,
                        error_kw={"ecolor": "#aaa", "capsize": 3, "linewidth": 0.8},
                    )
                    ax_wv.set_xlabel(t("temp_axis_label"), color="white")
                    ax_wv.tick_params(colors="white")
                    for spine in ax_wv.spines.values():
                        spine.set_color("#444")
                    ax_wv.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_wv)
                    plt.close(fig_wv)

                    # Venue data table
                    _wv_display = df_venue.rename(columns={
                        "venue": t("col_stadium"), "home_team": t("col_home_team"),
                        "matches": t("col_matches"), "avg_temp": t("col_avg_temp"),
                        "min_temp": t("col_min_temp_c"), "max_temp": t("col_max_temp_c"),
                        "avg_humidity": t("col_avg_humidity"), "rainy": t("col_rainy_matches"),
                    })
                    st.dataframe(_wv_display, width="stretch")

                    st.caption(t("venue_weather_caption"))

                # ── Cross-season temperature for a specific stadium/team ──
                st.subheader("📊 " + t("temperature") + " — " + t("season_trend"))
                _wcs1, _wcs2 = st.columns(2)
                with _wcs1:
                    _venue_list = explore.get_venues_list(_mc_team)
                    _venue_sel = st.selectbox(
                        t("stadium_venue_select"),
                        [t("all_teams")] + _venue_list,
                        key="weather_venue_sel",
                    )
                    _venue_pick = None if _venue_sel == t("all_teams") else _venue_sel
                with _wcs2:
                    _team_list_w = explore.get_teams_for_season(
                        _mc_season, _mc_comp
                    ) if _mc_season else []
                    _team_sel_w = st.selectbox(
                        t("team"),
                        [t("all_teams")] + _team_list_w,
                        key="weather_team_cross",
                    )
                    _team_pick_w = None if _team_sel_w == t("all_teams") else _team_sel_w

                if _venue_pick or _team_pick_w:
                    df_cross = explore.get_weather_venue_across_seasons(
                        _venue_pick, _team_pick_w,
                    )
                    if df_cross.empty:
                        st.info(t("no_weather_data"))
                    else:
                        # ── Metrics ──
                        _wcm1, _wcm2, _wcm3 = st.columns(3)
                        _all_avg = df_cross["avg_temp"].astype(float).mean()
                        _all_min = df_cross["min_temp"].astype(float).min()
                        _all_max = df_cross["max_temp"].astype(float).max()
                        _wcm1.metric(t("avg_temp"), f"{_all_avg:.1f} °C")
                        _wcm2.metric(t("min_temp"), f"{_all_min:.0f} °C")
                        _wcm3.metric(t("max_temp"), f"{_all_max:.0f} °C")

                        # ── Bar chart: avg temp per season with error bars ──
                        fig_wcs, ax_wcs = plt.subplots(figsize=(10, 5))
                        fig_wcs.patch.set_facecolor("#0e1117")
                        ax_wcs.set_facecolor("#0e1117")

                        _seasons = df_cross["season"].values
                        _avgs = df_cross["avg_temp"].values.astype(float)
                        _mins = df_cross["min_temp"].values.astype(float)
                        _maxs = df_cross["max_temp"].values.astype(float)
                        _err_l = _avgs - _mins
                        _err_h = _maxs - _avgs

                        _cs_colors = [
                            "#e74c3c" if v > 25 else "#3498db" if v < 10 else "#f39c12"
                            for v in _avgs
                        ]
                        ax_wcs.bar(
                            _seasons, _avgs,
                            yerr=[_err_l, _err_h],
                            color=_cs_colors,
                            error_kw={"ecolor": "#aaa", "capsize": 4, "linewidth": 0.8},
                        )
                        # Value labels on bars
                        for i, (s, v) in enumerate(zip(_seasons, _avgs)):
                            ax_wcs.text(
                                i, v + _err_h[i] + 0.5,
                                f"{v:.1f}°",
                                ha="center", va="bottom",
                                fontsize=9, color="white", fontweight="bold",
                            )
                        ax_wcs.set_ylabel("°C", color="white")
                        ax_wcs.set_xlabel(t("season"), color="white")
                        ax_wcs.tick_params(colors="white", axis="both")
                        for spine in ax_wcs.spines.values():
                            spine.set_color("#444")
                        plt.xticks(rotation=45, ha="right")
                        plt.tight_layout()
                        st.pyplot(fig_wcs)
                        plt.close(fig_wcs)

                        # ── Match-level scatter over time ──
                        df_matches_w = explore.get_weather_matches_for_venue(
                            _venue_pick, _team_pick_w,
                        )
                        if not df_matches_w.empty:
                            df_matches_w["match_date"] = pd.to_datetime(
                                df_matches_w["match_date"]
                            )
                            df_matches_w["temperature_c"] = pd.to_numeric(
                                df_matches_w["temperature_c"], errors="coerce"
                            )
                            _scatter_df = (
                                df_matches_w.set_index("match_date")[["temperature_c"]]
                                .rename(columns={"temperature_c": "°C"})
                                .sort_index()
                            )
                            st.line_chart(_scatter_df)

                        # ── Data table ──
                        _wcs_display = df_cross.rename(columns={
                            "season": t("season"), "matches": t("matches"),
                            "avg_temp": t("col_avg_temp"), "min_temp": t("col_min_temp_c"),
                            "max_temp": t("col_max_temp_c"), "avg_humidity": t("col_avg_humidity"),
                            "rainy": t("rainy_matches"),
                        })
                        st.dataframe(_wcs_display, width="stretch")

                        st.caption(t("weather_trend_color_caption"))
                else:
                    st.info(t("select_stadium_team"))

                with st.expander(t("results")):
                    if not df_w.empty:
                        st.dataframe(df_w, width="stretch")
                    else:
                        st.info(t("no_weather_data"))

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
                _has_fill_match = "fill_pct" in df_att.columns and df_att["fill_pct"].notna().any()
                if _has_fill_match:
                    am1, am2, am3, am4, am5 = st.columns(5)
                else:
                    am1, am2, am3, am4 = st.columns(4)
                am1.metric(t("matches"), _fmt(len(df_att)))
                am2.metric(t("avg_attendance"), _fmt(att_vals.mean()))
                am3.metric(t("max_attendance"), _fmt(att_vals.max()))
                am4.metric("Min", _fmt(att_vals.min()))
                if _has_fill_match:
                    avg_fill = df_att["fill_pct"].dropna().mean()
                    am5.metric("Avg Fill %", f"{avg_fill:.1f}%")

                # ── Stadium fill illustration ────────────────────
                if _has_fill_match:
                    df_fill = df_att[df_att["fill_pct"].notna()].copy()
                    if not df_fill.empty:
                        st.subheader(t("stadium_fill_viz"))
                        _fill_options: list[str] = [t("stadium_fill_avg")]
                        _fill_rows: list[pd.Series | None] = [None]

                        df_fill_sorted = df_fill.sort_values("match_date")
                        for _, _fr in df_fill_sorted.iterrows():
                            _md = _fr["match_date"]
                            _md_str = (
                                _md.strftime("%Y-%m-%d")
                                if hasattr(_md, "strftime") else str(_md)
                            )
                            _fill_options.append(
                                f"{_md_str} · {_fr['home_team']} vs {_fr['away_team']}"
                                f" ({_fr['fill_pct']:.0f}%)"
                            )
                            _fill_rows.append(_fr)

                        _fill_pick = st.selectbox(
                            t("stadium_fill_select"),
                            range(len(_fill_options)),
                            format_func=lambda i: _fill_options[i],
                            key="att_stadium_fill_sel",
                        )

                        if _fill_pick == 0:
                            _viz_att = int(att_vals.mean())
                            _viz_fill = float(df_fill["fill_pct"].mean())
                            _cap_vals = pd.to_numeric(
                                df_fill["capacity"], errors="coerce"
                            ).dropna()
                            _viz_cap = (
                                int(_cap_vals.mean())
                                if not _cap_vals.empty
                                else int(_viz_att / (_viz_fill / 100))
                                if _viz_fill > 0 else 0
                            )
                            _viz_title = (
                                _mc_team or _mc_comp or _mc_season or ""
                            )
                            _viz_sub = t("stadium_fill_avg")
                        else:
                            _row = _fill_rows[_fill_pick]
                            _viz_att = int(_row["attendance"])
                            _viz_cap = int(_row["capacity"])
                            _viz_fill = float(_row["fill_pct"])
                            _viz_title = str(_row.get("stadium") or _row.get("venue_name") or "")
                            _viz_sub = (
                                f"{_row['home_team']} vs {_row['away_team']}"
                            )

                        _viz_col, _viz_info = st.columns([2, 1])
                        with _viz_col:
                            # Semi-transparent glass-dome stadium (SVG). The old
                            # matplotlib render is still available as
                            # stadium_fill.render_stadium_fill if you prefer it.
                            stadium_fill_svg.render_stadium_fill_svg(
                                _viz_fill,
                                attendance=_viz_att,
                                capacity=_viz_cap,
                                title=_viz_title,
                                subtitle=_viz_sub,
                                height=470,
                            )
                        with _viz_info:
                            st.metric(t("attendance"), _fmt(_viz_att))
                            st.metric(t("total_capacity"), _fmt(_viz_cap))
                            st.metric(t("fill_pct"), f"{_viz_fill:.1f}%")
                            _empty = max(_viz_cap - _viz_att, 0)
                            st.metric(t("empty_seats"), _fmt(_empty))
                        st.caption(t("stadium_fill_caption"))
                    else:
                        st.info(t("stadium_fill_no_cap"))

                # ── Attendance trend over the season ─────────────
                df_trend = explore.get_attendance_trend(
                    _mc_season, _mc_team, _mc_comp,
                )
                if not df_trend.empty and "match_date" in df_trend.columns:
                    st.subheader(t("attendance") + " — " + t("season_trend"))
                    df_trend["match_date"] = pd.to_datetime(df_trend["match_date"])
                    df_trend["attendance"] = pd.to_numeric(
                        df_trend["attendance"], errors="coerce"
                    )
                    trend_chart = (
                        df_trend.groupby("match_date")["attendance"]
                        .mean()
                        .sort_index()
                        .rename(t("attendance"))
                        .to_frame()
                    )
                    st.line_chart(trend_chart)

                # ── Attendance by team (with capacity utilization) ─
                if _mc_team is None:
                    df_att_team = explore.get_attendance_by_team(_mc_season, _mc_comp)
                    if not df_att_team.empty:
                        st.subheader(t("attendance_by_team"))
                        top_att = df_att_team.head(20)

                        has_fill = "fill_pct" in top_att.columns and top_att["fill_pct"].notna().any()

                        fig_att, ax_att = plt.subplots(
                            figsize=(10, max(3, len(top_att) * 0.5))
                        )
                        fig_att.patch.set_facecolor("#0e1117")
                        ax_att.set_facecolor("#0e1117")

                        bars = ax_att.barh(
                            top_att["team"],
                            top_att["avg_attendance"],
                            color="#1abc9c",
                        )
                        # Annotate fill % on bars when available
                        if has_fill:
                            for bar, fp in zip(bars, top_att["fill_pct"]):
                                if pd.notna(fp):
                                    ax_att.text(
                                        bar.get_width() + 50,
                                        bar.get_y() + bar.get_height() / 2,
                                        f"{fp:.0f}%",
                                        va="center", fontsize=8,
                                        color="#f39c12", fontweight="bold",
                                    )

                        ax_att.set_xlabel(t("avg_attendance"), color="white")
                        ax_att.tick_params(colors="white")
                        for spine in ax_att.spines.values():
                            spine.set_color("#444")
                        ax_att.invert_yaxis()
                        plt.tight_layout()
                        st.pyplot(fig_att)
                        plt.close(fig_att)

                        if has_fill:
                            st.caption(t("attendance_fill_caption"))

                        # ── Table with full data ───────────────────
                        _att_display = top_att.rename(columns={
                            "team": t("team"), "home_matches": t("col_home_matches_short"),
                            "avg_attendance": t("col_avg_short"), "max_attendance": t("col_max_short"),
                            "min_attendance": t("col_min_short"), "total_attendance": t("col_total_short"),
                            "capacity": t("col_stadium_capacity"), "fill_pct": t("fill_pct"),
                        })
                        st.dataframe(_att_display, width="stretch")

                with st.expander(t("results")):
                    _att_cols_rename = {
                        "match_date": t("col_date"), "home_team": t("home_label"),
                        "away_team": t("away_label"), "home_score": t("col_hg"),
                        "away_score": t("col_ag"), "attendance": t("attendance"),
                        "stadium": t("col_stadium"), "venue_name": t("col_raw_venue"),
                        "capacity": t("col_capacity"), "fill_pct": t("fill_pct"),
                    }
                    _att_display = df_att.rename(
                        columns={k: v for k, v in _att_cols_rename.items()
                                 if k in df_att.columns}
                    )
                    st.dataframe(_att_display, width="stretch")

    # ── Referees ─────────────────────────────────────────────
    with t_referees:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            df_ref = explore.get_referee_stats(_mc_season, _mc_comp, _mc_team)
            if df_ref.empty:
                st.info(t("no_referee_data"))
            else:
                _ref_avg_cpm = round(
                    float(df_ref["cards_per_match"].dropna().mean()), 2
                ) if not df_ref["cards_per_match"].dropna().empty else 0

                rm1, rm2, rm3, rm4 = st.columns(4)
                rm1.metric(t("referees_section"), len(df_ref))
                rm2.metric(t("yellow_cards"),
                           _fmt(df_ref["yellow_cards"].sum()))
                rm3.metric(t("red_cards"),
                           _fmt(df_ref["red_cards"].sum()))
                rm4.metric(t("avg_cards_match"), f"{_ref_avg_cpm:.2f}")

                _ref_scope_label = (
                    f" ({_mc_team})" if _mc_team else ""
                )
                display_ref = df_ref.rename(columns={
                    "referee": t("col_referee"),
                    "matches_officiated": t("col_matches"),
                    "yellow_cards": t("col_yellows_scope").format(scope=_ref_scope_label),
                    "red_cards": t("col_reds_scope").format(scope=_ref_scope_label),
                    "total_cards": t("col_total_cards_scope").format(scope=_ref_scope_label),
                    "cards_per_match": t("col_cards_match_scope").format(scope=_ref_scope_label),
                })
                st.dataframe(display_ref, width="stretch")

                # Chart: top referees by cards per match (min 3 matches)
                _min_ref_matches = 3 if _mc_team else 5
                strict_ref = df_ref[
                    df_ref["matches_officiated"] >= _min_ref_matches
                ].copy()
                if not strict_ref.empty:
                    _chart_title = (
                        t("cards_match_vs_team").format(team=_mc_team)
                        if _mc_team
                        else t("cards_match_all")
                    )
                    st.subheader(_chart_title)
                    top_strict = strict_ref.sort_values(
                        "cards_per_match", ascending=False
                    ).head(15)
                    fig_ref, ax_ref = plt.subplots(
                        figsize=(10, max(3, len(top_strict) * 0.45))
                    )
                    fig_ref.patch.set_facecolor("#0e1117")
                    ax_ref.set_facecolor("#0e1117")
                    # Stacked yellow + red bar
                    _ref_y = top_strict["yellow_cards"].values / top_strict["matches_officiated"].values
                    _ref_r = top_strict["red_cards"].values / top_strict["matches_officiated"].values
                    ax_ref.barh(
                        top_strict["referee"], _ref_y,
                        color="#f1c40f", label=t("yellows_per_match"),
                    )
                    ax_ref.barh(
                        top_strict["referee"], _ref_r,
                        left=_ref_y,
                        color="#e74c3c", label=t("reds_per_match"),
                    )
                    ax_ref.set_xlabel(t("cards_per_match_label"), color="white")
                    ax_ref.tick_params(colors="white")
                    ax_ref.legend(
                        facecolor="#1a1a2e", edgecolor="#444",
                        labelcolor="white", fontsize=9,
                    )
                    for spine in ax_ref.spines.values():
                        spine.set_color("#444")
                    ax_ref.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_ref)
                    plt.close(fig_ref)

                _ref_caption = (
                    t("ref_scope_team").format(team=_mc_team)
                    if _mc_team else ""
                )
                st.caption(
                    t("referee_caption").format(scope=_ref_caption, min_m=_min_ref_matches)
                )

    # ── Managers ──────────────────────────────────────────────
    with t_managers:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            df_mgr = explore.get_manager_stats(_mc_season, _mc_comp, _mc_team)
            if df_mgr.empty:
                st.info(t("no_manager_data"))
            else:
                _mgr_total_matches = int(df_mgr["matches"].sum())
                _mgr_total_wins = int(df_mgr["wins"].sum())
                _mgr_avg_ppct = round(
                    float(df_mgr["points_pct"].mean()), 1
                ) if not df_mgr["points_pct"].dropna().empty else 0

                mm1, mm2, mm3, mm4 = st.columns(4)
                mm1.metric(t("managers_section"), len(df_mgr))
                mm2.metric(t("matches"), _fmt(_mgr_total_matches))
                mm3.metric(t("wins"), _fmt(_mgr_total_wins))
                mm4.metric(t("points_pct"), f"{_mgr_avg_ppct:.1f}%")

                display_mgr = df_mgr.rename(columns={
                    "manager": t("col_manager"),
                    "team": t("team"),
                    "matches": t("col_matches"),
                    "wins": t("col_wins_short"), "draws": t("col_draws_short"),
                    "losses": t("col_losses_short"),
                    "goals_for": t("col_gf"), "goals_against": t("col_ga"),
                    "avg_gf": t("col_gf"), "avg_ga": t("col_ga"),
                    "points_pct": t("points_pct"),
                })
                st.dataframe(display_mgr, width="stretch")

                # Top 15 managers by points %  (min 3 matches for chart)
                chart_mgr = df_mgr[df_mgr["matches"] >= 3].copy()
                top_mgr = chart_mgr.sort_values("points_pct", ascending=False).head(15)
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
                    # Stacked W/D/L bar
                    bar_w = top_mgr["wins"].values
                    bar_d = top_mgr["draws"].values
                    bar_l = top_mgr["losses"].values
                    ax_mgr.barh(labels_mgr, bar_w, color="#2ecc71", label=t("col_wins_short"))
                    ax_mgr.barh(labels_mgr, bar_d, left=bar_w, color="#f39c12", label=t("col_draws_short"))
                    ax_mgr.barh(labels_mgr, bar_l, left=bar_w + bar_d, color="#e74c3c", label=t("col_losses_short"))
                    ax_mgr.set_xlabel(t("matches"), color="white")
                    ax_mgr.tick_params(colors="white")
                    ax_mgr.legend(
                        facecolor="#1a1a2e", edgecolor="#444",
                        labelcolor="white", fontsize=9,
                    )
                    for spine in ax_mgr.spines.values():
                        spine.set_color("#444")
                    ax_mgr.invert_yaxis()
                    plt.tight_layout()
                    st.pyplot(fig_mgr)
                    plt.close(fig_mgr)

                st.caption(t("manager_caption"))

    # ── Chalkboard (per-match passes / tackles / shots) ──────
    with t_chalk:
        if _mc_season is None:
            st.info(t("select_season"))
        else:
            _cb_matches = explore.get_matches_for_context(
                _mc_season, _mc_team, _mc_comp
            )
            if _cb_matches.empty:
                st.info(t("no_matches_found"))
            else:
                def _cb_label(_r) -> str:
                    _d = _r["match_date"]
                    _ds = _d.strftime("%Y-%m-%d") if hasattr(_d, "strftime") else str(_d)
                    _sc = ""
                    if pd.notna(_r["home_score"]) and pd.notna(_r["away_score"]):
                        _sc = f" {int(_r['home_score'])}-{int(_r['away_score'])}"
                    return f"{_ds} · {_r['home_team']}{_sc} {_r['away_team']}"

                _cb_labels = [_cb_label(_r) for _, _r in _cb_matches.iterrows()]
                _cb_idx = st.selectbox(
                    t("match"), range(len(_cb_labels)),
                    format_func=lambda i: _cb_labels[i], key="cb_match_pick",
                )
                _cb_mid = int(_cb_matches.iloc[_cb_idx]["match_id"])
                _cb_ev = explore.get_match_events_xy(_cb_mid)

                if _cb_ev.empty:
                    st.info(t("no_event_data"))
                else:
                    try:
                        from mplsoccer import Pitch as _CbPitch
                    except ImportError:
                        st.error(t("install_mplsoccer"))
                        _CbPitch = None

                    def _cb_cat(_et):
                        _e = str(_et).lower()
                        if "pass" in _e:
                            return "pass"
                        if "tackle" in _e:
                            return "tackle"
                        if any(k in _e for k in ("shot", "goal", "miss", "saved", "post")):
                            return "shot"
                        return "other"

                    _cb_ev = _cb_ev.copy()
                    _cb_ev["cat"] = _cb_ev["event_type"].map(_cb_cat)
                    for _c in ("x", "y", "end_x", "end_y"):
                        _cb_ev[_c] = pd.to_numeric(_cb_ev[_c], errors="coerce")

                    _f1, _f2, _f3 = st.columns([1.2, 1.2, 1.6])
                    with _f1:
                        _teams = list(_cb_ev["team"].dropna().unique())
                        _team_pick = st.selectbox("Equipo", ["Ambos"] + _teams, key="cb_team")
                    _ev_t = _cb_ev if _team_pick == "Ambos" else _cb_ev[_cb_ev["team"] == _team_pick]
                    with _f2:
                        _players = sorted(_ev_t["player"].dropna().unique().tolist())
                        _player_pick = st.selectbox("Jugador", ["Todos"] + _players, key="cb_player")
                    with _f3:
                        _acts = st.multiselect(
                            t("action_type"),
                            [t("passes"), t("tackles"), t("shots")],
                            default=[t("passes"), t("tackles"), t("shots")],
                            key="cb_acts",
                        )
                    _sel = _ev_t if _player_pick == "Todos" else _ev_t[_ev_t["player"] == _player_pick]
                    _cat_map = {t("passes"): "pass", t("tackles"): "tackle", t("shots"): "shot"}
                    _wanted = {_cat_map[a] for a in _acts}
                    _sel = _sel[_sel["cat"].isin(_wanted)]

                    if _CbPitch is not None:
                        _pitch_cb = _CbPitch(
                            pitch_type="custom", pitch_length=105, pitch_width=68,
                            pitch_color="#1a472a", line_color="white", line_zorder=2,
                        )
                        _fig_cb, _ax_cb = _pitch_cb.draw(figsize=(9, 6))
                        _fig_cb.patch.set_facecolor("#1a472a")

                        _p = _sel[(_sel["cat"] == "pass") & _sel["end_x"].notna() & _sel["end_y"].notna()]
                        _p_ok = _p[_p["outcome"].astype(str).str.lower() == "successful"]
                        _p_no = _p[_p["outcome"].astype(str).str.lower() != "successful"]
                        for _grp, _col in ((_p_ok, "#2ecc71"), (_p_no, "#e74c3c")):
                            if not _grp.empty:
                                _pitch_cb.arrows(
                                    _grp["x"] * 105, _grp["y"] * 68,
                                    _grp["end_x"] * 105, _grp["end_y"] * 68,
                                    ax=_ax_cb, color=_col, width=1.2,
                                    headwidth=4, headlength=4, alpha=0.55, zorder=2,
                                )
                        _tk = _sel[(_sel["cat"] == "tackle") & _sel["x"].notna() & _sel["y"].notna()]
                        if not _tk.empty:
                            _pitch_cb.scatter(
                                _tk["x"] * 105, _tk["y"] * 68, ax=_ax_cb, s=90,
                                marker="s", color="#3498db", edgecolors="white",
                                linewidths=0.5, zorder=3,
                            )
                        _sh = _sel[(_sel["cat"] == "shot") & _sel["x"].notna() & _sel["y"].notna()]
                        if not _sh.empty:
                            _pitch_cb.scatter(
                                _sh["x"] * 105, _sh["y"] * 68, ax=_ax_cb, s=150,
                                marker="*", color="#f1c40f", edgecolors="black",
                                linewidths=0.4, zorder=4,
                            )
                        st.pyplot(_fig_cb)
                        plt.close(_fig_cb)

                        _m1, _m2, _m3 = st.columns(3)
                        _m1.metric(t("passes"), _fmt(int((_sel["cat"] == "pass").sum())))
                        _m2.metric(t("tackles"), _fmt(int((_sel["cat"] == "tackle").sum())))
                        _m3.metric(t("shots"), _fmt(int((_sel["cat"] == "shot").sum())))
                        st.caption(t("chalkboard_caption"))


# ════════════════════════════════════════════════════════════════════
# TAB 4 — PASS NETWORK  (fact_events · WhoScored)
# ════════════════════════════════════════════════════════════════════
with tab_passnet:
    st.header(t("pass_network"))
    st.caption(t("pn_caption_header"))

    try:
        from mplsoccer import Pitch as _PnPitch
    except ImportError:
        st.error(t("install_mplsoccer"))
        st.stop()

    _pn_comps = explore.get_competitions()
    pnf1, pnf2, pnf3 = st.columns([1, 1, 2])
    with pnf1:
        pn_comp = st.selectbox(
            t("competition"), _pn_comps or [t("none_option")],
            key="pn_comp", disabled=not _pn_comps,
        )
    _pn_seasons = explore.get_seasons_for_competition(pn_comp) if _pn_comps else []
    with pnf2:
        pn_season = st.selectbox(
            t("season"), _pn_seasons or [t("no_seasons_paren")],
            key="pn_season", disabled=not _pn_seasons,
        )

    _pn_matches = (
        pass_network.get_matches_with_passes(pn_comp, pn_season)
        if _pn_seasons else pd.DataFrame()
    )
    with pnf3:
        pn_match_label = st.selectbox(
            t("match"),
            _pn_matches["label"].tolist() if not _pn_matches.empty else [t("no_matches_paren")],
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

        st.caption(t("pn_caption"))


# ════════════════════════════════════════════════════════════════════
# TAB 6 — STADIUMS  (dim_stadium · Transfermarkt · SCD2)
# ════════════════════════════════════════════════════════════════════
with tab_stadiums:
    st.header(t("stadiums"))
    st.caption(t("stadium_caption"))

    if not explore._stadium_table_exists():
        st.warning(t("stadium_table_missing"))
    else:
        # ── Filtros ──────────────────────────────────────────────
        st_seasons   = explore.get_stadium_seasons()
        st_comps     = explore.get_competitions()
        st_countries = explore.get_stadium_countries()

        f1, f2, f3, f4 = st.columns([1, 1, 1, 2])
        with f1:
            st_season = st.selectbox(
                t("season"),
                [t("all_seasons")] + st_seasons,
                key="st_season",
                disabled=not st_seasons,
            )
        with f2:
            st_comp = st.selectbox(
                t("competition"),
                [t("all_competitions")] + st_comps,
                key="st_comp",
                disabled=not st_comps,
            )
        with f3:
            st_country = st.selectbox(
                t("country"),
                [t("all_countries")] + st_countries,
                key="st_country",
                disabled=not st_countries,
            )
        with f4:
            st_search = st.text_input(
                t("search_stadium"),
                value="", key="st_search",
            ).strip() or None

        st_include_venues = st.checkbox(
            t("stadium_include_venues"),
            value=False,
            key="st_include_match_venues",
        )

        season_q  = None if st_season  == t("all_seasons")      else st_season
        comp_q    = None if st_comp    == t("all_competitions") else st_comp
        country_q = None if st_country == t("all_countries")    else st_country

        # ── Tarjetas resumen ─────────────────────────────────────
        summary = explore.get_stadium_summary(
            season=season_q, competition=comp_q, country=country_q,
            include_match_venues=st_include_venues,
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
            include_match_venues=st_include_venues,
        )
        if df_st.empty:
            st.info(t("stadium_no_results"))
        else:
            st.caption(t("stadium_select_hint"))
            display_df = df_st.copy()
            display_df.columns = [
                "stadium_id", t("team"), t("season"), t("stadiums"), t("col_capacity"),
                t("col_seats"), t("col_built"), t("col_owner"), t("col_city"), t("country"), t("col_surface"),
                t("col_architect"), t("col_lat"), t("col_lon"), t("col_altitude"), t("col_timezone"),
                t("source"), t("col_tm_url"), "master_stadium_id",
                "image_url", "wikipedia_url", "wikidata_qid",
            ]
            table_df = display_df.drop(
                columns=[
                    "stadium_id", "master_stadium_id",
                    "image_url", "wikipedia_url", "wikidata_qid",
                ],
            )
            selection = st.dataframe(
                table_df,
                width="stretch",
                on_select="rerun",
                selection_mode="single-row",
                key="stadium_picker",
                column_config={
                    t("col_tm_url"): st.column_config.LinkColumn(
                        t("tm_link_label"), display_text=t("tm_open")
                    ),
                    t("col_capacity"): st.column_config.NumberColumn(format="%d"),
                    t("col_lat"): st.column_config.NumberColumn(format="%.4f"),
                    t("col_lon"): st.column_config.NumberColumn(format="%.4f"),
                },
            )
            selected_rows = (
                selection.selection.rows
                if selection is not None and hasattr(selection, "selection")
                else []
            )
            stadium_labels = [
                f"{r.stadium_name} — {r.team}" for r in df_st.itertuples()
            ]
            default_idx = selected_rows[0] if selected_rows else 0
            detail_idx = st.selectbox(
                t("stadium_view_select"),
                range(len(stadium_labels)),
                index=default_idx,
                format_func=lambda i: stadium_labels[i],
                key="stadium_detail_sel",
            )
            st.divider()
            _render_stadium_detail(df_st.iloc[detail_idx])

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
                ax_st.set_xlabel(t("col_capacity"), color="white")
                ax_st.tick_params(colors="white")
                for spine in ax_st.spines.values():
                    spine.set_color("#444")
                ax_st.invert_yaxis()
                plt.tight_layout()
                st.pyplot(fig_st)
                plt.close(fig_st)

        st.caption(t("stadium_footer_caption"))


# ════════════════════════════════════════════════════════════════════
# TAB 6 — PIPELINE MONITORING
# ════════════════════════════════════════════════════════════════════
with tab_monitor:
    st.header(t("pipeline_monitoring"))

    # ── Section 1 — DB metric cards ───────────────────────
    p1, p2, p3, p4 = st.columns(4)
    p1.metric(t("tab_players"),    _fmt(_DB_SUMMARY['players']))
    p2.metric(t("matches"),       _fmt(_DB_SUMMARY['matches']))
    p3.metric(t("shots_xg_metric"),       _fmt(_DB_SUMMARY['shots']))
    p4.metric(t("tab_injuries"),  _fmt(_DB_SUMMARY['injuries']))

    st.divider()

    # ── Section 2 — Season scanner ────────────────────────
    st.subheader(t("season_scanner"))
    if st.button(t("scan_all_sources"), type="primary", key="scan_btn"):
        with st.spinner(t("scanning_spinner")):
            st.session_state["scan_results"] = scanner.scan_all()

    scan_results = st.session_state.get("scan_results")
    if scan_results is not None:
        errors = scan_results.get("_errors") or {}
        if errors:
            st.warning(f"{t('scanner_errors')}: {sorted(errors.keys())}")

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
            st.info(t("load_missing_cli"))
        else:
            st.success(t("all_sources_up_to_date"))

    st.divider()

    # ── Section 3 — Coverage ──────────────────────────────
    st.subheader(t("coverage_by_source"))
    cov_competitions = explore.get_competitions()
    cov_seasons = explore.get_seasons_for_competition(cov_competitions[0]) \
        if cov_competitions else []
    cc1, cc2 = st.columns(2)
    with cc1:
        cov_comp = st.selectbox(t("competition"), cov_competitions, key="cov_comp")
    with cc2:
        cov_season = st.selectbox(
            t("season"), cov_seasons or [t("no_seasons_paren")], key="cov_season",
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
                st.caption(t("sofascore_incident_caption"))
        if total_total > 0:
            st.write(f"**{t('overall')}**")
            st.progress(min(total_loaded / total_total, 1.0))

    st.divider()

    # ── Section 3b — Event diagnostics (WhoScored) ────────
    st.subheader(t("event_diagnostics"))
    _ev_cov = explore.get_whoscored_event_coverage()
    if _ev_cov.empty:
        st.info(t("no_event_data"))
    else:
        _ec1, _ec2 = st.columns([1, 1])
        with _ec1:
            st.caption(t("whoscored_events_season"))
            st.dataframe(
                _ev_cov.rename(columns={
                    "season": t("season"), "matches": t("matches"), "events": "Eventos",
                }),
                width="stretch", hide_index=True,
            )
        with _ec2:
            st.caption(t("event_types_xy"))
            st.dataframe(
                explore.get_whoscored_event_types().rename(columns={
                    "event_type": "event_type", "events": "Eventos", "with_xy": "Con x/y",
                }),
                width="stretch", hide_index=True,
            )
        st.caption(t("event_diag_caption"))

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
        st.info(t("no_unresolved_review"))
    else:
        st.dataframe(pr_df, width='stretch')
    st.info(t("resolve_player_cli"))

    st.divider()

    # ── Section 5 — Recent matches ────────────────────────
    st.subheader(t("recent_matches"))
    rm_df = db.get_recent_matches(20)
    if rm_df.empty:
        st.info(t("no_matches_dim"))
    else:
        st.dataframe(rm_df, width='stretch')


# ════════════════════════════════════════════════════════════════════
# TAB 7 — WIZARD (writes to the database — read-only exception)
# ════════════════════════════════════════════════════════════════════
with tab_wizard:
    wizard_view.render()
=======
if _st_runtime.exists():
    _nav = st.navigation(
        [
            st.Page(_mod.render, title=t(_key), url_path=_path)
            for _mod, _key, _path in _PAGES
        ],
        position="top",
    )
    _nav.run()
else:
    # Bare mode (python dashboard/app.py): render every page so the smoke
    # test still exercises all of them.
    for _mod, *_ in _PAGES:
        _mod.render()
>>>>>>> origin/main
