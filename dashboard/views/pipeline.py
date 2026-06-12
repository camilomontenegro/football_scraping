"""
dashboard/views/pipeline.py
===========================
Pipeline — DB metrics, scanner, coverage, player review, recent matches.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboard import db, explore, scanner
from dashboard.i18n import t
from dashboard.views.shared import _fmt


def render() -> None:
    # The DB guard in app.py already ran get_db_summary() this rerun;
    # reuse its result instead of re-counting the fact tables.
    _DB_SUMMARY = st.session_state.get("_db_summary") or db.get_db_summary()

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
