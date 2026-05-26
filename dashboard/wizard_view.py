"""
dashboard/wizard_view.py
========================
Streamlit Wizard tab. UI counterpart of `wizard/wizard.py`'s interactive flow.

Stepwise selectors → summary panel → Run button → live log stream → optional
team-CSV download. The Run button invokes `wizard.pipeline_runner.run_pipeline`
with the same kwargs the CLI uses.

This is the single exception to the dashboard's read-only contract: every other
tab only reads from the database. The Wizard tab writes to it via the scraping
pipeline.
"""
from __future__ import annotations

import datetime as _dt
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

from wizard.competitions import (
    COMPETITIONS,
    WORKING_COMPETITIONS,
    WORKING_COMPETITION_NAMES,
    get_competition,
    get_competition_slug_transfermarkt,
    get_season_start_year,
)
from wizard.pipeline_runner import (
    available_sources_for_competition,
    get_available_seasons,
    get_current_season,
    run_pipeline,
)
from wizard.wizard import export_matches_for_team, is_international_competition
from dashboard.i18n import t


_LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "logs" / "wizard_latest_log.txt"
_LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"


# ─────────────────────────────────────────────────────────────────────
# Logging — stream wizard output into st.session_state
# ─────────────────────────────────────────────────────────────────────
class WizardStreamHandler(logging.Handler):
    """Appends each formatted record to st.session_state['wiz_log']."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        buf = st.session_state.setdefault("wiz_log", [])
        buf.append(msg)


def _install_log_handlers() -> tuple[logging.Handler, logging.Handler]:
    """Attach a stream handler (session_state) and a file handler (latest log)."""
    stream_handler = WizardStreamHandler()
    stream_handler.setFormatter(logging.Formatter(_LOG_FMT))
    stream_handler.setLevel(logging.INFO)

    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(_LOG_PATH, mode="w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_LOG_FMT))
    file_handler.setLevel(logging.INFO)

    root = logging.getLogger()
    wiz_logger = logging.getLogger("wizard.pipeline_runner")
    root.addHandler(stream_handler)
    root.addHandler(file_handler)
    wiz_logger.addHandler(stream_handler)
    # Ensure INFO records propagate even if a prior basicConfig set a higher level.
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    return stream_handler, file_handler


def _remove_log_handlers(stream_handler: logging.Handler, file_handler: logging.Handler) -> None:
    root = logging.getLogger()
    wiz_logger = logging.getLogger("wizard.pipeline_runner")
    for h in (stream_handler, file_handler):
        try:
            root.removeHandler(h)
        except Exception:
            pass
        try:
            wiz_logger.removeHandler(h)
        except Exception:
            pass
        try:
            h.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────
# Competition selector helpers
# ─────────────────────────────────────────────────────────────────────
def _competition_options() -> List[tuple[str, str]]:
    """[(label, name)] flat list, category-prefixed labels, working comps only."""
    out: List[tuple[str, str]] = []
    for category, names in WORKING_COMPETITIONS.items():
        for name in names:
            if name in COMPETITIONS:
                out.append((f"[{category}] {name}", name))
    return out


def _season_options() -> List[str]:
    current = get_current_season()
    seasons = get_available_seasons(start_year=2020, end_year=get_season_start_year(current))
    if current not in seasons:
        seasons.append(current)
    return seasons


def _cached_team_slugs(competition: str, season: str) -> List[str]:
    """Fetch team slugs from Transfermarkt; cache in session_state per (comp, season)."""
    key = f"wiz_teams_{competition}_{season}"
    if key in st.session_state:
        return st.session_state[key]

    comp_conf = get_competition(competition) or {}
    league_code = comp_conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
    if not league_code:
        st.session_state[key] = []
        return []

    try:
        from scrapers.transfermarkt_scraper import get_league_teams
        tm_slug = get_competition_slug_transfermarkt(competition) or "laliga"
        teams = get_league_teams(get_season_start_year(season), tm_slug, league_code)
        slugs = [t["team_slug"] for t in teams]
    except Exception as exc:
        st.warning(f"No se pudo consultar Transfermarkt para los equipos: {exc}")
        slugs = []

    st.session_state[key] = slugs
    return slugs


# ─────────────────────────────────────────────────────────────────────
# Pipeline invocation
# ─────────────────────────────────────────────────────────────────────
def _execute_pipeline(kwargs: Dict[str, Any]) -> Optional[Exception]:
    """Run pipeline with handlers attached. Returns the exception (if any)."""
    st.session_state["wiz_log"] = []
    st.session_state["wizard_running"] = True
    stream_handler, file_handler = _install_log_handlers()
    exc: Optional[Exception] = None
    try:
        run_pipeline(**kwargs)
    except SystemExit as e:
        exc = e
    except Exception as e:  # noqa: BLE001
        exc = e
        logging.getLogger("wizard.pipeline_runner").exception(
            "Pipeline raised an unexpected exception"
        )
    finally:
        _remove_log_handlers(stream_handler, file_handler)
        st.session_state["wizard_running"] = False
    return exc


# ─────────────────────────────────────────────────────────────────────
# Public entrypoint
# ─────────────────────────────────────────────────────────────────────
def render() -> None:
    """Render the Wizard tab."""
    st.header(t("wizard"))
    st.caption(t("wizard_warning"))

    # ── Step 1: Operation ─────────────────────────────────────────
    op_download = t("download_full_season")
    op_update = t("update_new_games")
    op_stadiums = t("download_stadiums")
    operation = st.radio(
        t("what_to_do"),
        [op_download, op_update, op_stadiums],
        key="wiz_operation",
    )
    stadiums_only = operation == op_stadiums
    full_scrape = operation == op_download

    # En modo estadios: SOLO se elige la temporada. La descarga itera todas
    # las ligas de WORKING_COMPETITIONS automaticamente.
    if stadiums_only:
        season_opts = _season_options()
        default_season = get_current_season()
        default_idx = (
            season_opts.index(default_season)
            if default_season in season_opts else len(season_opts) - 1
        )
        season = st.selectbox(
            t("season"), season_opts, index=default_idx, key="wiz_season"
        )
        competition = "Todas (wizard)"
        comp_conf: Dict[str, Any] = {}
        source = "transfermarkt (stadiums)"
        team_slug: Optional[str] = None
        from_date: Optional[str] = None
        st.info(
            "Modo **estadios**: se procesarán **todas** las ligas de "
            "`WORKING_COMPETITIONS` del wizard (ligas nacionales + "
            "competiciones UEFA de clubes). Las selecciones nacionales "
            "se omiten automáticamente. Fuente: Transfermarkt. "
            "Modelo en BD: SCD2 (una fila por estado del estadio)."
        )

        st.markdown(f"**{t('operation_summary')}**")
        st.markdown(
            f"- **Acción:** Descarga de estadios (Transfermarkt → dim_stadium SCD2)\n"
            f"- **Competiciones:** todas las de WORKING_COMPETITIONS\n"
            f"- **Temporada:** {season}"
        )

        running = st.session_state.get("wizard_running", False)
        run_clicked = st.button(
            "Run stadium pipeline",
            type="primary",
            disabled=running,
            key="wiz_run_stadiums",
        )
        if run_clicked:
            from wizard.wizard import run_all_stadiums_flow
            st.session_state["wiz_log"] = []
            st.session_state["wizard_running"] = True
            stream_handler, file_handler = _install_log_handlers()
            exc: Optional[Exception] = None
            try:
                with st.spinner("Descargando estadios — no cierres esta pestaña…"):
                    run_all_stadiums_flow(season)
            except SystemExit as e:
                exc = e
            except Exception as e:  # noqa: BLE001
                exc = e
                logging.getLogger("wizard.stadiums").exception(
                    "Stadium pipeline raised an unexpected exception"
                )
            finally:
                _remove_log_handlers(stream_handler, file_handler)
                st.session_state["wizard_running"] = False
            if exc is None:
                st.success("Descarga de estadios completada.")
            else:
                st.error(f"Stadium pipeline failed: {exc}")

        # Log persistente
        log_buf = st.session_state.get("wiz_log") or []
        if log_buf:
            st.markdown(f"**{t('pipeline_log')}**")
            st.code("\n".join(log_buf), language="text")
            if _LOG_PATH.exists():
                st.caption(f"Full log file: `{_LOG_PATH}`")
        return

    # ── Step 2: Competition (grouped by category) ─────────────────
    comp_options = _competition_options()
    labels = [label for label, _ in comp_options]
    label_to_name = {label: name for label, name in comp_options}
    chosen_label = st.selectbox(t("competition"), labels, key="wiz_competition_label")
    competition = label_to_name[chosen_label]
    st.session_state["wiz_competition"] = competition
    comp_conf = get_competition(competition) or {}

    # ── Step 3: Season ────────────────────────────────────────────
    season_opts = _season_options()
    default_season = get_current_season()
    default_idx = season_opts.index(default_season) if default_season in season_opts else len(season_opts) - 1
    season = st.selectbox(t("season"), season_opts, index=default_idx, key="wiz_season")

    # ── Step 4: Source (auto-filtered) ────────────────────────────
    available = available_sources_for_competition(comp_conf, competition, season)
    if not available:
        st.warning(
            "La competición seleccionada no tiene fuentes configuradas en "
            "`competitions.py` o en el reference CSV."
        )
        st.stop()

    if "understat" not in available and comp_conf.get("sources", {}).get("understat", {}).get("league"):
        # Reference CSV may filter understat too; only show the i18n note when
        # we know understat is "structurally" available but suppressed.
        if is_international_competition(comp_conf):
            st.info(
                "Understat sólo cubre ligas domésticas — se ha eliminado de la "
                "lista de fuentes para esta competición."
            )
        else:
            st.info(
                "Understat no tiene datos para esta competición — se ha "
                "eliminado de la lista de fuentes."
            )

    source_options = ["all"] + available
    source = st.selectbox(
        t("data_sources"),
        source_options,
        index=0,
        key="wiz_source",
    )

    # ── Step 5: Match filter ──────────────────────────────────────
    filter_all = t("all_matches")
    filter_team = t("team_only")
    filter_date = t("from_date")
    match_filter_choice = st.radio(
        t("match_filter"),
        [filter_all, filter_team, filter_date],
        key="wiz_match_filter",
    )

    team_slug: Optional[str] = None
    from_date: Optional[str] = None

    if match_filter_choice == filter_team:
        league_code = comp_conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
        if not league_code:
            st.warning(
                "Esta competición no tiene Transfermarkt configurado; no se puede "
                "filtrar por equipo. Se descargarán todos los partidos."
            )
        else:
            slugs = _cached_team_slugs(competition, season)
            if not slugs:
                st.warning(
                    "No se obtuvieron equipos desde Transfermarkt. Se descargarán "
                    "todos los partidos."
                )
            else:
                team_slug = st.selectbox(t("team"), slugs, key="wiz_team_slug")
    elif match_filter_choice == filter_date:
        picked = st.date_input(
            t("start_date"),
            value=_dt.date.today(),
            key="wiz_from_date",
        )
        if isinstance(picked, _dt.date):
            from_date = picked.isoformat()

    # ── Summary panel ─────────────────────────────────────────────
    accion = t("full_download") if full_scrape else t("incremental_update")
    if team_slug:
        filtro = f"{t('team_only')}: '{team_slug}'"
    elif from_date:
        filtro = f"{t('from_date')}: {from_date}"
    else:
        filtro = t("all_matches")

    st.markdown(f"**{t('operation_summary')}**")
    st.markdown(
        f"- **{t('action')}:** {accion}\n"
        f"- **{t('competition')}:** {competition}\n"
        f"- **{t('season')}:** {season}\n"
        f"- **{t('data_sources')}:** {source}\n"
        f"- **{t('filter')}:** {filtro}"
    )

    # ── Run button (with concurrent-run guard) ────────────────────
    running = st.session_state.get("wizard_running", False)
    run_clicked = st.button(
        t("run_pipeline"),
        type="primary",
        disabled=running,
        key="wiz_run_button",
    )

    if run_clicked:
        kwargs = {
            "scrape": full_scrape,
            "competition": competition,
            "source": source,
            "season": season,
            "from_date": from_date,
            "update": not full_scrape,
        }
        with st.spinner(t("run_pipeline") + "..."):
            exc = _execute_pipeline(kwargs)
        if exc is None:
            st.success(t("pipeline_completed"))
            if team_slug:
                _render_team_export(team_slug, competition, season)
        else:
            st.error(f"{t('pipeline_failed')}: {exc}")

    # ── Persisted log buffer ──────────────────────────────────────
    log_buf = st.session_state.get("wiz_log") or []
    if log_buf:
        st.markdown(f"**{t('pipeline_log')}**")
        st.code("\n".join(log_buf), language="text")
        if _LOG_PATH.exists():
            st.caption(f"Full log file: `{_LOG_PATH}`")


def _render_team_export(team_slug: str, competition: str, season: str) -> None:
    """Run the team-CSV export and surface a download_button or info."""
    csv_path = export_matches_for_team(
        team_slug, competition.replace(" ", "_"), season
    )
    if csv_path is None or not Path(csv_path).exists():
        st.info("No hay partidos para ese equipo en la temporada seleccionada.")
        return

    csv_bytes = Path(csv_path).read_bytes()
    st.download_button(
        label=f"Download {team_slug} matches ({season})",
        data=csv_bytes,
        file_name=Path(csv_path).name,
        mime="text/csv",
        key="wiz_team_download",
    )
