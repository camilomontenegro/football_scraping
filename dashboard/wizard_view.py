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
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

from wizard.competitions import (
    COMPETITIONS,
    get_competition,
    get_competition_slug_transfermarkt,
    get_season_start_year,
)
from wizard.pipeline_runner import (
    PipelineCancelled,
    available_sources_for_competition,
    get_available_seasons,
    get_current_season,
    grouped_db_competitions,
    run_pipeline,
)
from wizard.wizard import export_matches_for_team, is_international_competition


_LOG_PATH = Path(__file__).resolve().parents[1] / "data" / "logs" / "wizard_latest_log.txt"
_LOG_FMT = "%(asctime)s - %(levelname)s - %(message)s"
_MAX_DISPLAY_LINES = 1000
_POLL_INTERVAL_S = 0.5


# ─────────────────────────────────────────────────────────────────────
# Logging — push every line (logging + stdout + stderr) into a Queue so
# the Streamlit main thread can render it live while the pipeline runs
# in a background worker thread.
# ─────────────────────────────────────────────────────────────────────
class _QueueLogHandler(logging.Handler):
    """Logging handler that pushes formatted records to a thread-safe queue."""

    def __init__(self, q: "queue.Queue[str]") -> None:
        super().__init__()
        self._queue = q

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        try:
            self._queue.put(msg)
        except Exception:
            pass


class _StreamTee:
    """File-like object that tees writes into the original stream AND a queue.

    Carriage returns are normalised to newlines so tqdm-style progress bars
    don't get swallowed — each refresh becomes its own line in the log view.
    """

    def __init__(self, original, q: "queue.Queue[str]") -> None:
        self._original = original
        self._queue = q
        self._buf = ""

    def write(self, data) -> int:
        try:
            self._original.write(data)
            self._original.flush()
        except Exception:
            pass
        if not isinstance(data, str):
            try:
                data = data.decode("utf-8", errors="replace")
            except Exception:
                data = str(data)
        data = data.replace("\r\n", "\n").replace("\r", "\n")
        self._buf += data
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            try:
                self._queue.put(line)
            except Exception:
                pass
        return len(data)

    def flush(self) -> None:
        if self._buf:
            try:
                self._queue.put(self._buf)
            except Exception:
                pass
            self._buf = ""
        try:
            self._original.flush()
        except Exception:
            pass

    def isatty(self) -> bool:
        return False


def _install_handlers(
    q: "queue.Queue[str]",
) -> tuple[_QueueLogHandler, logging.FileHandler]:
    """Attach (1) queue handler for live UI, (2) file handler for the on-disk log."""
    queue_handler = _QueueLogHandler(q)
    queue_handler.setFormatter(logging.Formatter(_LOG_FMT))
    queue_handler.setLevel(logging.INFO)

    _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(_LOG_PATH, mode="w", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_LOG_FMT))
    file_handler.setLevel(logging.INFO)

    # Solo se conecta al root logger; los loggers hijos (p.ej.
    # `wizard.pipeline_runner`) propagan sus records hacia root por defecto,
    # así evitamos que cada línea se duplique en el panel del wizard.
    root = logging.getLogger()
    root.addHandler(queue_handler)
    root.addHandler(file_handler)
    if root.level > logging.INFO:
        root.setLevel(logging.INFO)
    return queue_handler, file_handler


def _remove_handlers(
    queue_handler: _QueueLogHandler, file_handler: logging.FileHandler
) -> None:
    root = logging.getLogger()
    for h in (queue_handler, file_handler):
        try:
            root.removeHandler(h)
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
    """[(label, name)] de las competiciones registradas en `dim_competition`.

    La selección es dinámica: lo que aparece aquí depende de las filas que
    haya en la tabla `dim_competition` de la base de datos. Añade una fila
    nueva y aparecerá en el wizard sin tocar código.
    """
    out: List[tuple[str, str]] = []
    for category, names in grouped_db_competitions():
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
# Pipeline invocation — pattern: non-blocking start + st.rerun() polling.
#
# The worker thread runs `run_pipeline(...)` in the background while the
# Streamlit main thread re-renders the page every ~0.5s via `st.rerun()`.
# That way the UI stays responsive — the Stop button can be clicked at
# any time, which sets `cancel_event` and triggers cooperative
# cancellation inside `run_pipeline` between phases / sources.
# ─────────────────────────────────────────────────────────────────────
def _drain_queue(q: "queue.Queue[str]", buf: list[str]) -> bool:
    """Pull every available line from `q` into `buf`. Returns True if drained."""
    drained = False
    while True:
        try:
            line = q.get_nowait()
        except queue.Empty:
            break
        buf.append(line)
        drained = True
    return drained


def _start_pipeline(
    kwargs: Dict[str, Any],
    team_slug: Optional[str],
    competition: str,
    season: str,
) -> None:
    """Spawn the worker thread and stash all the state needed for live polling.

    Does NOT block — returns immediately. The caller is expected to
    `st.rerun()` so the script re-enters in the "running" branch.
    """
    log_queue: "queue.Queue[str]" = queue.Queue()
    queue_handler, file_handler = _install_handlers(log_queue)
    cancel_event = threading.Event()

    holder: dict[str, Optional[BaseException]] = {"exc": None}

    def _worker() -> None:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = _StreamTee(old_stdout, log_queue)
        sys.stderr = _StreamTee(old_stderr, log_queue)
        try:
            run_pipeline(cancel_event=cancel_event, **kwargs)
        except BaseException as e:  # noqa: BLE001
            holder["exc"] = e
            if not isinstance(e, PipelineCancelled):
                logging.getLogger("wizard.pipeline_runner").exception(
                    "Pipeline raised an unexpected exception"
                )
        finally:
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    thread = threading.Thread(target=_worker, name="wizard-pipeline", daemon=True)

    st.session_state["wiz_running"] = True
    st.session_state["wiz_cancel_requested"] = False
    st.session_state["wiz_cancel_event"] = cancel_event
    st.session_state["wiz_log_queue"] = log_queue
    st.session_state["wiz_log_buf"] = []
    st.session_state["wiz_thread"] = thread
    st.session_state["wiz_handlers"] = (queue_handler, file_handler)
    st.session_state["wiz_exc_holder"] = holder
    st.session_state["wiz_run_team_slug"] = team_slug
    st.session_state["wiz_run_competition"] = competition
    st.session_state["wiz_run_season"] = season
    st.session_state["wiz_run_source"] = kwargs.get("source", "all")
    st.session_state["wiz_run_full_scrape"] = bool(kwargs.get("scrape"))
    st.session_state["wiz_final_exc"] = None

    thread.start()


def _start_stadiums_pipeline(season: str) -> None:
    """
    Lanza en background el sub-pipeline de estadios: itera todas las
    competiciones de `dim_competition` con `id_transfermarkt`, scrapea
    cada una desde Transfermarkt y carga en `dim_stadium`. Reusa la misma
    infraestructura de log y holder que `_start_pipeline`, pero NO admite
    cancelación granular.
    """
    log_queue: "queue.Queue[str]" = queue.Queue()
    queue_handler, file_handler = _install_handlers(log_queue)
    cancel_event = threading.Event()
    holder: dict[str, Optional[BaseException]] = {"exc": None}

    def _worker() -> None:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = _StreamTee(old_stdout, log_queue)
        sys.stderr = _StreamTee(old_stderr, log_queue)
        try:
            # Import perezoso para evitar coste si nunca se usa el flujo
            from wizard.wizard import run_all_stadiums_flow
            run_all_stadiums_flow(season)
        except BaseException as e:  # noqa: BLE001
            holder["exc"] = e
            logging.getLogger("wizard.stadiums").exception(
                "Stadium pipeline raised an unexpected exception"
            )
        finally:
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    thread = threading.Thread(target=_worker, name="wizard-stadiums", daemon=True)

    st.session_state["wiz_running"] = True
    st.session_state["wiz_cancel_requested"] = False
    st.session_state["wiz_cancel_event"] = cancel_event
    st.session_state["wiz_log_queue"] = log_queue
    st.session_state["wiz_log_buf"] = []
    st.session_state["wiz_thread"] = thread
    st.session_state["wiz_handlers"] = (queue_handler, file_handler)
    st.session_state["wiz_exc_holder"] = holder
    st.session_state["wiz_run_team_slug"] = None
    st.session_state["wiz_run_competition"] = "Todas (dim_competition)"
    st.session_state["wiz_run_season"] = season
    st.session_state["wiz_run_source"] = "transfermarkt (stadiums)"
    st.session_state["wiz_run_full_scrape"] = True
    st.session_state["wiz_final_exc"] = None

    thread.start()


def _finalize_pipeline() -> None:
    """Called once the worker thread has died: drain queue, remove handlers,
    convert any captured exception into a user-facing exception object."""
    log_queue = st.session_state.get("wiz_log_queue")
    buf = st.session_state.setdefault("wiz_log_buf", [])
    if log_queue is not None:
        _drain_queue(log_queue, buf)

    handlers = st.session_state.get("wiz_handlers")
    if handlers is not None:
        _remove_handlers(*handlers)

    raw_exc = (st.session_state.get("wiz_exc_holder") or {}).get("exc")
    final_exc: Optional[Exception]
    if raw_exc is None:
        final_exc = None
    elif isinstance(raw_exc, PipelineCancelled):
        # Cancellation is its own signal; surfaced via wiz_cancel_requested.
        final_exc = None
    elif isinstance(raw_exc, SystemExit):
        code = raw_exc.code
        final_exc = (
            None
            if code in (None, 0)
            else RuntimeError(f"Pipeline exited with code {code}")
        )
    elif isinstance(raw_exc, Exception):
        final_exc = raw_exc
    else:
        final_exc = RuntimeError(f"{type(raw_exc).__name__}: {raw_exc}")

    st.session_state["wiz_final_exc"] = final_exc
    st.session_state["wiz_running"] = False
    # Drop the now-stale references but keep the log buf + final state visible
    for k in ("wiz_log_queue", "wiz_handlers", "wiz_exc_holder", "wiz_thread"):
        st.session_state.pop(k, None)


def _render_running_panel() -> None:
    """Render Run+Stop controls, status and live log while the worker runs."""
    cancel_requested = st.session_state.get("wiz_cancel_requested", False)
    cancel_event = st.session_state.get("wiz_cancel_event")

    # ── Buttons (Stop is the actionable one) ──────────────────────
    col_run, col_stop = st.columns(2)
    col_run.button(
        "Run pipeline",
        type="primary",
        disabled=True,
        key="wiz_run_button_busy",
    )
    stop_clicked = col_stop.button(
        "Stop pipeline",
        type="secondary",
        disabled=cancel_requested,
        help=(
            "Marca el pipeline para cancelar entre fases. La fuente que se "
            "esté descargando ahora mismo termina su request actual; las "
            "siguientes fuentes y la fase de carga no se ejecutan."
        ),
        key="wiz_stop_button",
    )

    if stop_clicked and cancel_event is not None and not cancel_requested:
        cancel_event.set()
        st.session_state["wiz_cancel_requested"] = True
        cancel_requested = True

    # ── Status ────────────────────────────────────────────────────
    if cancel_requested:
        st.warning(
            "Cancelando… esperando a que el pipeline llegue al siguiente "
            "checkpoint y termine limpiamente."
        )
    else:
        st.info("Pipeline en ejecución — no cierres esta pestaña.")

    # ── Live log ─────────────────────────────────────────────────
    log_queue = st.session_state.get("wiz_log_queue")
    buf = st.session_state.setdefault("wiz_log_buf", [])
    if log_queue is not None:
        _drain_queue(log_queue, buf)

    st.markdown("**Pipeline log (en vivo)**")
    tail = buf[-_MAX_DISPLAY_LINES:]
    st.code("\n".join(tail) if tail else "(esperando salida…)", language="text")
    if _LOG_PATH.exists():
        st.caption(f"Full log file: `{_LOG_PATH}`")

    # ── Polling: thread dead → finalize; thread alive → rerun ────
    thread = st.session_state.get("wiz_thread")
    if thread is None or not thread.is_alive():
        _finalize_pipeline()
        st.rerun()

    time.sleep(_POLL_INTERVAL_S)
    st.rerun()


# ─────────────────────────────────────────────────────────────────────
# Public entrypoint
# ─────────────────────────────────────────────────────────────────────
def render() -> None:
    """Render the Wizard tab."""
    st.header("Wizard")
    st.caption(
        "⚠️ This tab writes to the database via the scraping pipeline. "
        "Every other tab is read-only."
    )

    # If a pipeline is currently running we render a dedicated panel and
    # auto-refresh via st.rerun() so the UI stays interactive (Stop button
    # works mid-run). This branch returns early — selectors are hidden.
    if st.session_state.get("wiz_running"):
        _render_run_summary()
        _render_running_panel()
        return

    # ── Step 1: Operation ─────────────────────────────────────────
    operation = st.radio(
        "¿Qué quieres hacer?",
        [
            "Descargar temporada completa",
            "Actualizar datos con juegos nuevos",
            "Descargar estadios por temporada",
        ],
        key="wiz_operation",
    )
    op_lower      = operation.lower()
    stadiums_only = "estadio" in op_lower
    full_scrape   = (not stadiums_only) and op_lower.startswith("descargar")

    season_opts = _season_options()
    default_season = get_current_season()
    default_idx = season_opts.index(default_season) if default_season in season_opts else len(season_opts) - 1

    team_slug: Optional[str] = None
    from_date: Optional[str] = None
    source = "transfermarkt"  # default usado en modo estadios

    if stadiums_only:
        # En modo estadios sólo se pide la temporada; el pipeline procesa
        # todas las competiciones registradas en `dim_competition` con
        # `id_transfermarkt`.
        season = st.selectbox(
            "Temporada", season_opts, index=default_idx, key="wiz_season"
        )
        competition = "Todas (dim_competition)"
        st.info(
            "Modo **estadios**: se procesarán **todas** las competiciones de "
            "`dim_competition` con `id_transfermarkt`. La fuente es "
            "Transfermarkt y los datos se cargan en `dim_stadium`."
        )
    else:
        # ── Step 2: Competition (grouped by category, dynamic from dim_competition) ─
        comp_options = _competition_options()
        if not comp_options:
            st.warning(
                "No hay competiciones registradas en la tabla `dim_competition` "
                "de la base de datos. Inserta las competiciones que quieras "
                "scrapear en `dim_competition` y recarga el dashboard."
            )
            st.stop()
        labels = [label for label, _ in comp_options]
        label_to_name = {label: name for label, name in comp_options}
        chosen_label = st.selectbox("Competición", labels, key="wiz_competition_label")
        competition = label_to_name[chosen_label]
        st.session_state["wiz_competition"] = competition
        comp_conf = get_competition(competition) or {}

        # ── Step 3: Season ────────────────────────────────────────────
        season = st.selectbox(
            "Temporada", season_opts, index=default_idx, key="wiz_season"
        )

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
            "Fuente(s) de datos",
            source_options,
            index=0,
            key="wiz_source",
        )

        # ── Step 5: Match filter ──────────────────────────────────────
        match_filter_choice = st.radio(
            "¿Cómo filtrar los partidos descargados?",
            ["Todos los partidos", "Sólo de un equipo", "Desde una fecha"],
            key="wiz_match_filter",
        )

        if match_filter_choice == "Sólo de un equipo":
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
                    team_slug = st.selectbox("Equipo", slugs, key="wiz_team_slug")
        elif match_filter_choice == "Desde una fecha":
            picked = st.date_input(
                "Fecha de inicio",
                value=_dt.date.today(),
                key="wiz_from_date",
            )
            if isinstance(picked, _dt.date):
                from_date = picked.isoformat()

    # ── Summary panel ─────────────────────────────────────────────
    if stadiums_only:
        accion = "Descarga de estadios (Transfermarkt → dim_stadium)"
        filtro = "n/a"
    else:
        accion = "Descarga completa" if full_scrape else "Actualización incremental"
        if team_slug:
            filtro = f"sólo equipo '{team_slug}'"
        elif from_date:
            filtro = f"partidos desde {from_date}"
        else:
            filtro = "todos los partidos"

    st.markdown("**Resumen de la operación**")
    st.markdown(
        f"- **Acción:** {accion}\n"
        f"- **Competición:** {competition}\n"
        f"- **Temporada:** {season}\n"
        f"- **Fuente(s):** {source}\n"
        f"- **Filtro:** {filtro}"
    )

    # ── Run + Stop buttons ────────────────────────────────────────
    col_run, col_stop = st.columns(2)
    run_clicked = col_run.button(
        "Run pipeline",
        type="primary",
        key="wiz_run_button",
    )
    col_stop.button(
        "Stop pipeline",
        type="secondary",
        disabled=True,
        help="Solo activo mientras hay un pipeline en ejecución.",
        key="wiz_stop_button_idle",
    )

    if run_clicked:
        if stadiums_only:
            _start_stadiums_pipeline(season)
        else:
            kwargs = {
                "scrape": full_scrape,
                "competition": competition,
                "source": source,
                "season": season,
                "from_date": from_date,
                "update": not full_scrape,
            }
            _start_pipeline(kwargs, team_slug, competition, season)
        st.rerun()

    # ── Post-run summary (success / error / cancel) ───────────────
    final_exc = st.session_state.get("wiz_final_exc")
    cancel_requested = st.session_state.get("wiz_cancel_requested", False)
    log_buf = st.session_state.get("wiz_log_buf") or []

    if log_buf or final_exc is not None or cancel_requested:
        if cancel_requested:
            st.warning("Pipeline cancelado por el usuario.")
        elif final_exc is None and log_buf:
            st.success("Pipeline completed successfully.")
            run_team_slug = st.session_state.get("wiz_run_team_slug")
            if run_team_slug:
                _render_team_export(
                    run_team_slug,
                    st.session_state.get("wiz_run_competition", competition),
                    st.session_state.get("wiz_run_season", season),
                )
        elif final_exc is not None:
            st.error(f"Pipeline failed: {final_exc}")

        if log_buf:
            st.markdown("**Pipeline log (última ejecución)**")
            st.code("\n".join(log_buf[-_MAX_DISPLAY_LINES:]), language="text")
            if _LOG_PATH.exists():
                st.caption(f"Full log file: `{_LOG_PATH}`")


def _render_run_summary() -> None:
    """Compact summary of what's running (shown above the live log)."""
    comp = st.session_state.get("wiz_run_competition", "?")
    seas = st.session_state.get("wiz_run_season", "?")
    team = st.session_state.get("wiz_run_team_slug")
    src  = st.session_state.get("wiz_run_source", "?")
    stadiums_mode = "stadiums" in (src or "")
    if stadiums_mode:
        accion = "Descarga de estadios (Transfermarkt → dim_stadium)"
        filtro = "n/a"
    else:
        accion = (
            "Descarga completa"
            if st.session_state.get("wiz_run_full_scrape")
            else "Actualización incremental"
        )
        if team:
            filtro = f"sólo equipo '{team}'"
        else:
            filtro = "todos los partidos (o filtro de fecha)"
    st.markdown("**Resumen de la operación en curso**")
    st.markdown(
        f"- **Acción:** {accion}\n"
        f"- **Competición:** {comp}\n"
        f"- **Temporada:** {seas}\n"
        f"- **Fuente(s):** {src}\n"
        f"- **Filtro:** {filtro}"
    )


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
