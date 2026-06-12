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


def render() -> None:
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
