"""
dashboard/views/stadiums.py
===========================
Stadiums — dim_stadium browser (Transfermarkt, SCD2).

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import streamlit as st

from dashboard import explore
from dashboard.i18n import t
from dashboard.views.shared import _fmt, _render_stadium_detail


def _is_international_competition(competition: str | None) -> bool:
    if not competition:
        return False
    c = competition.lower()
    keywords = (
        "champions",
        "europa",
        "conference",
        "international",
        "world",
        "mundial",
        "intercontinental",
        "super cup",
        "supercopa",
        "nations league",
        "copa america",
        "euro cup",
    )
    return any(k in c for k in keywords)


def render() -> None:
    st.header(t("stadiums"))
    st.caption(t("stadium_caption"))

    if not explore._stadium_table_exists():
        st.warning(t("stadium_table_missing"))
    else:
        include_match_venues = st.checkbox(
            t("stadium_include_venues"),
            value=False,
            key="st_include_match_venues",
        )
        # ── Filtros ──────────────────────────────────────────────
        st_seasons = explore.get_stadium_seasons()
        st_comps = explore.get_competitions()
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
        comp_q_preview = None if st_comp == t("all_competitions") else st_comp
        show_country_filter = _is_international_competition(comp_q_preview)
        with f3:
            if show_country_filter:
                st_country = st.selectbox(
                    t("country"),
                    [t("all_countries")] + st_countries,
                    key="st_country",
                    disabled=not st_countries,
                )
            else:
                st_country = t("all_countries")
                st.caption("")
        with f4:
            st_search = st.text_input(
                t("search_stadium"),
                value="", key="st_search",
            ).strip() or None

        season_q  = None if st_season  == t("all_seasons")      else st_season
        comp_q    = None if st_comp    == t("all_competitions") else st_comp
        country_q = None if st_country == t("all_countries")    else st_country

        # ── Tarjetas resumen ─────────────────────────────────────
        summary = explore.get_stadium_summary(
            season=season_q, competition=comp_q, country=country_q,
            include_match_venues=include_match_venues,
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
            include_match_venues=include_match_venues,
        )
        if df_st.empty:
            st.info(t("stadium_no_results"))
        else:
            st.caption(t("stadium_select_hint"))
            display_df = df_st.copy()
            display_df.columns = [
                "stadium_id", t("team"), t("season"), t("stadiums"), t("total_capacity"),
                t("col_seats"), t("col_built"), t("col_owner"), t("col_city"), t("country"),
                t("col_surface"), t("col_architect"), t("col_lat"), t("col_lon"), t("col_altitude"),
                t("col_timezone"), t("source"), t("col_tm_url"), "master_stadium_id",
                "image_url", "wikipedia_url", "wikidata_qid",
            ]
            table_df = display_df.drop(
                columns=[
                    "stadium_id", "master_stadium_id", "image_url",
                    "wikipedia_url", "wikidata_qid", t("source"),
                    t("col_seats"), t("col_altitude"), t("col_timezone"),
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
                    t("total_capacity"): st.column_config.NumberColumn(format="%d"),
                    t("col_lat"): st.column_config.NumberColumn(format="%.4f"),
                    t("col_lon"): st.column_config.NumberColumn(format="%.4f"),
                },
            )
            selected_rows = (
                selection.selection.rows
                if selection is not None and hasattr(selection, "selection")
                else []
            )
            selected_idx: int | None = selected_rows[0] if selected_rows else None

            # Fallback selector: keeps the old "pick one stadium and view detail"
            # behavior even when dataframe row selection is not available.
            st.divider()
            options = list(range(len(df_st)))
            default_idx = selected_idx if selected_idx is not None else 0
            selected_option = st.selectbox(
                t("stadium_view_select"),
                options,
                index=default_idx,
                format_func=lambda i: (
                    f"{df_st.iloc[i]['stadium_name']} ({df_st.iloc[i]['team']})"
                ),
                key="stadium_detail_selectbox",
            )
            _render_stadium_detail(df_st.iloc[int(selected_option)])

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

        st.caption(t("stadium_footer_caption"))
