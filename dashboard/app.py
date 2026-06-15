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

from dashboard import db
from dashboard.i18n import t, LANGUAGES
from dashboard.views import (
    exploration, players, shot_intelligence, pass_network,
    match_context, stadiums, pipeline, wizard,
)

st.set_page_config(
    page_title="Football Scraping Dashboard",
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
