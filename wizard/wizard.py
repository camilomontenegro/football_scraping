#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
wizard.py
=========
Asistente interactivo del pipeline de scraping de fútbol.

Mejoras de UX (versión unificada):
  • Pulsando "0" en cualquier menú vuelves al paso anterior.
  • Pulsando "Q" sales del wizard limpiamente.
  • Las fuentes de datos se filtran automáticamente según la competición.
  • Understat queda excluido para competiciones internacionales (Champions,
    Europa, Conference) ya que sólo cubre ligas domésticas.
  • Resumen + confirmación antes de lanzar el pipeline.
  • Las competiciones se agrupan por categoría (Nacionales / Continentales).

Uso:
    Interactivo:
        $ python -m scripts.wizard

    CLI (no se hacen preguntas):
        $ python -m scripts.wizard --competition "La Liga" --season 2024/2025 --scrape
        $ python -m scripts.wizard --competition "Champions League" --update
        $ python -m scripts.wizard --competition "La Liga" --season 2024/2025 --scrape --team "real-madrid"
"""

# --------------------------------------------------------------------------- #
# Imports
# --------------------------------------------------------------------------- #
import argparse
import csv
import datetime
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from wizard.pipeline_runner import (
    run_pipeline,
    list_available_competitions,
    get_available_seasons,
    get_last_match_date,
    get_current_season,
    db_competition_names,
    grouped_db_competitions,
)
from wizard.competitions import (
    COMPETITIONS,
    get_competition,
    get_season_start_year,
)
from scrapers.transfermarkt_scraper import get_league_teams
from loaders.common import engine
from sqlalchemy import text


# --------------------------------------------------------------------------- #
# Sentinels para navegación entre pasos
# --------------------------------------------------------------------------- #
BACK = "__BACK__"
QUIT = "__QUIT__"


class WizardQuit(Exception):
    """Lanzada cuando el usuario pulsa Q para salir del wizard."""


LOG_PATH = Path(__file__).resolve().parent.parent / "data" / "logs" / "wizard_latest_log.txt"


class _Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
            stream.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()


def run_with_log_capture(fn) -> None:
    """Guarda stdout/stderr/logging del wizard en un TXT nuevo por ejecucion."""
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as log_file:
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = _Tee(old_stdout, log_file)
        sys.stderr = _Tee(old_stderr, log_file)
        file_handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        try:
            print(f"[LOG] Esta ejecucion se guarda en: {LOG_PATH}")
            fn()
        finally:
            root_logger.removeHandler(file_handler)
            file_handler.close()
            sys.stdout = old_stdout
            sys.stderr = old_stderr


# --------------------------------------------------------------------------- #
# Helpers — prompts interactivos
# --------------------------------------------------------------------------- #
def prompt_choice(
    prompt: str,
    options: List[str],
    default: Optional[str] = None,
    allow_back: bool = True,
    back_label: str = "volver al paso anterior",
) -> str:
    """Menú numerado con soporte de back/quit."""
    while True:
        print()
        if default:
            print(f"{prompt} (default: {default})")
        else:
            print(prompt)
        for i, opt in enumerate(options, 1):
            print(f"  {i}) {opt}")
        if allow_back:
            print(f"  0) {back_label}")
        print("  q) salir del wizard")

        choice = input("Selecciona una opción: ").strip()

        if choice.lower() == "q":
            raise WizardQuit()
        if allow_back and choice == "0":
            return BACK
        if not choice and default is not None:
            return default
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]

        print("  [!] Entrada no válida. Introduce el número de una opción "
              "(o 0 para volver, q para salir).")


def prompt_date(prompt: str, default: Optional[str] = None,
                allow_back: bool = True) -> Optional[str]:
    """Pide una fecha YYYY-MM-DD. ENTER vacío = None."""
    while True:
        print()
        if default:
            print(f"{prompt} (default: {default})")
        else:
            print(prompt)
        msg = "Fecha (YYYY-MM-DD), ENTER para omitir"
        if allow_back:
            msg += ", 0 para volver, q para salir"
        d = input(f"{msg}: ").strip()

        if d.lower() == "q":
            raise WizardQuit()
        if allow_back and d == "0":
            return BACK
        if not d:
            return None
        try:
            datetime.datetime.strptime(d, "%Y-%m-%d")
            return d
        except ValueError:
            print("  [!] Formato inválido. Ejemplo válido: 2025-03-01")


def prompt_yes_no(prompt: str, default: bool = True) -> bool:
    """Pregunta sí/no. ENTER usa el default."""
    suffix = "[S/n]" if default else "[s/N]"
    while True:
        ans = input(f"{prompt} {suffix} ").strip().lower()
        if ans == "q":
            raise WizardQuit()
        if not ans:
            return default
        if ans in ("s", "si", "sí", "y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("  [!] Responde s o n (o q para salir).")


# --------------------------------------------------------------------------- #
# Categorización de competiciones
# --------------------------------------------------------------------------- #
_CONTINENTAL_COUNTRIES = {"Europe", "Europa", "EU"}
_INTERNATIONAL_COUNTRIES = {"International", "Internacional", "World", "WW"}


def _category(comp_conf: Dict[str, Any]) -> str:
    """Devuelve 'nacional', 'continental' o 'internacional'."""
    country = (comp_conf.get("country") or "").strip()
    code = (comp_conf.get("country_code") or "").strip().upper()
    if country in _INTERNATIONAL_COUNTRIES or code == "WW":
        return "internacional"
    if country in _CONTINENTAL_COUNTRIES or code == "EU":
        return "continental"
    return "nacional"


def _is_international(comp_conf: Dict[str, Any]) -> bool:
    """True si la competición NO es de una liga doméstica de un país.

    Devuelve True tanto para continentales (Champions, Europa) como para
    selecciones / internacionales (Mundial, Copa America, EURO, …).
    Las fuentes que sólo cubren ligas (Understat) la usan como filtro.
    """
    return _category(comp_conf) != "nacional"


def is_international_competition(comp_conf: Dict[str, Any]) -> bool:
    """Public wrapper used by the Streamlit dashboard."""
    return _is_international(comp_conf)


def _grouped_competitions() -> List[tuple]:
    """Sólo competiciones que estén registradas en `dim_competition`.

    La fuente de verdad es la tabla `dim_competition` de la base de datos —
    si una competición no aparece allí, queda oculta del wizard hasta que
    se inserte. Esto hace que el menú sea dinámico sin tocar código.
    """
    return grouped_db_competitions()


def _grouped_competitions_old() -> List[tuple]:
    """Agrupa las claves de COMPETITIONS por categoría."""
    nacionales: List[str] = []
    continentales: List[str] = []
    internacionales: List[str] = []
    for name, conf in COMPETITIONS.items():
        cat = _category(conf)
        if cat == "nacional":
            nacionales.append(name)
        elif cat == "continental":
            continentales.append(name)
        else:
            internacionales.append(name)

    groups: List[tuple] = []
    if nacionales:
        groups.append(("Ligas nacionales", nacionales))
    if continentales:
        groups.append(("Competiciones continentales", continentales))
    if internacionales:
        groups.append(("Selecciones / Internacional", internacionales))
    return groups


def _flatten_grouped(groups: List[tuple]) -> List[str]:
    """Devuelve la lista plana respetando el orden de los grupos."""
    flat: List[str] = []
    for _, items in groups:
        flat.extend(items)
    return flat


# --------------------------------------------------------------------------- #
# Selectores
# --------------------------------------------------------------------------- #
def choose_operation() -> str:
    return prompt_choice(
        "¿Qué quieres hacer?",
        [
            "Descargar temporada completa",
            "Actualizar datos con juegos nuevos",
            "Descargar estadios por temporada",
        ],
        default="Descargar temporada completa",
        allow_back=True,
        back_label="cancelar y salir",
    )


def choose_competition() -> str:
    """Selecciona competición agrupada por categoría.

    Sólo aparecen las competiciones registradas en `dim_competition`. Si la
    tabla está vacía (o la conexión a la BD falla) se aborta el wizard con
    un mensaje explícito.
    """
    groups = _grouped_competitions()
    flat = _flatten_grouped(groups)

    if not flat:
        print(
            "\n  [!] No hay competiciones registradas en `dim_competition`. "
            "Inserta las que quieras scrapear en esa tabla y vuelve a "
            "lanzar el wizard."
        )
        raise WizardQuit()

    while True:
        print()
        print("Selecciona la competición:")
        idx = 1
        for header, items in groups:
            print(f"\n  -- {header} --")
            for name in items:
                print(f"  {idx}) {name}")
                idx += 1
        print("\n  0) volver")
        print("  q) salir")

        raw = input("Selecciona una opción: ").strip()
        if raw.lower() == "q":
            raise WizardQuit()
        if raw == "0":
            return BACK
        if raw.isdigit() and 1 <= int(raw) <= len(flat):
            return flat[int(raw) - 1]
        print("  [!] Entrada no válida.")


def choose_season() -> str:
    """Devuelve la temporada elegida.  Default = temporada actual."""
    current = get_current_season()
    seasons = get_available_seasons(start_year=2020, end_year=get_season_start_year(current))
    if current not in seasons:
        seasons.append(current)
    return prompt_choice(
        "Selecciona la temporada a procesar:",
        seasons,
        default=current,
    )


def _reference_has_source(competition: str, season: str, source: str) -> bool:
    ref_path = Path(__file__).resolve().parent.parent / "data" / "reference" / "source_reference_ids.csv"
    if not ref_path.exists():
        return True
    with ref_path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (
                row.get("competition") == competition
                and row.get("season") == season
                and row.get("source") == source
            ):
                if source in {"sofascore", "statsbomb", "whoscored"}:
                    return bool(row.get("season_id"))
                return True
    return False


def _available_sources_for(comp_conf: Dict[str, Any], competition: str, season: str) -> List[str]:
    """Devuelve la lista de fuentes con datos para esta competición."""
    sources_map = comp_conf.get("sources", {})
    available: List[str] = []

    tm = sources_map.get("transfermarkt", {})
    if tm.get("league_code") and _reference_has_source(competition, season, "transfermarkt"):
        available.append("transfermarkt")

    sf = sources_map.get("sofascore", {})
    if sf.get("tournament_id") is not None and _reference_has_source(competition, season, "sofascore"):
        available.append("sofascore")

    # Understat — sólo para ligas nacionales.  No tiene datos fiables
    # para Champions / Europa / Conference aunque el dict tenga 'league'.
    us = sources_map.get("understat", {})
    if us.get("league") and not _is_international(comp_conf) and _reference_has_source(competition, season, "understat"):
        available.append("understat")

    sb = sources_map.get("statsbomb", {})
    if sb.get("competition_id") is not None and _reference_has_source(competition, season, "statsbomb"):
        available.append("statsbomb")

    ws = sources_map.get("whoscored", {})
    if (
        ws.get("tournament_id") is not None
        and _reference_has_source(competition, season, "whoscored")
    ):
        from scrapers.whoscored_scraper import whoscored_season_available
        if whoscored_season_available(competition, season):
            available.append("whoscored")

    return available


def choose_source(comp_conf: Dict[str, Any], competition: str, season: str) -> str:
    """Sólo muestra fuentes con datos reales para la competición elegida."""
    available = _available_sources_for(comp_conf, competition, season)
    if not available:
        print("\n[!] La competición seleccionada no tiene fuentes válidas en "
              "competitions.py. Revisa la configuración.")
        return BACK

    options = ["all"] + available

    if "understat" not in available:
        if _is_international(comp_conf):
            print("\n[i] Understat sólo cubre ligas domésticas — "
                  "se ha eliminado de la lista de fuentes para esta competición.")
        else:
            print("\n[i] Understat no tiene datos para esta competición — "
                  "se ha eliminado de la lista de fuentes.")

    return prompt_choice(
        "Selecciona la fuente(s) de datos a usar:",
        options,
        default="all",
    )


def choose_match_filter(comp_conf: Dict[str, Any], season_start: int) -> Dict[str, Optional[str]]:
    """All / Team / Date.  Devuelve dict con la elección o sentinel."""
    raw = prompt_choice(
        "¿Cómo filtrar los partidos descargados?",
        ["Todos los partidos", "Sólo de un equipo", "Desde una fecha"],
        default="Todos los partidos",
    )
    if raw in (BACK, QUIT):
        return {"_sentinel": raw}

    mapping = {
        "Todos los partidos": "all",
        "Sólo de un equipo":  "team",
        "Desde una fecha":    "date",
    }
    match_type = mapping[raw]

    result: Dict[str, Optional[str]] = {
        "match_type": match_type,
        "team_slug": None,
        "from_date": None,
    }

    if match_type == "team":
        league_code = comp_conf["sources"].get("transfermarkt", {}).get("league_code")
        if not league_code:
            print("  [!] Esta competición no tiene Transfermarkt configurado; "
                  "no se puede filtrar por equipo. Volviendo a 'Todos'.")
            result["match_type"] = "all"
            return result
        try:
            from scripts.competitions import get_competition_slug_transfermarkt
            tm_slug = get_competition_slug_transfermarkt(
                next((name for name, conf in COMPETITIONS.items() if conf is comp_conf), "")
            ) or "laliga"
            teams_list = get_league_teams(season_start, tm_slug, league_code)
            teams_dict = {t["team_slug"]: t["team_id"] for t in teams_list}
        except Exception as e:
            print(f"  [!] No se pudo consultar Transfermarkt: {e}.")
            result["match_type"] = "all"
            return result
        if not teams_dict:
            print("  [!] No se obtuvieron equipos. Filtro reducido a 'Todos'.")
            result["match_type"] = "all"
            return result
        team_slugs = list(teams_dict.keys())
        team_slug = prompt_choice("Selecciona el equipo:", team_slugs)
        if team_slug == BACK:
            return {"_sentinel": BACK}
        result["team_slug"] = team_slug

    elif match_type == "date":
        d = prompt_date("Introduce la fecha de inicio (YYYY-MM-DD)")
        if d == BACK:
            return {"_sentinel": BACK}
        result["from_date"] = d

    return result


# --------------------------------------------------------------------------- #
# Resumen + confirmación final
# --------------------------------------------------------------------------- #
def _print_summary(state: Dict[str, Any]) -> None:
    if state.get("stadiums_only"):
        op = "Descarga de estadios (Transfermarkt → dim_stadium)"
    elif state["full_scrape"]:
        op = "Descarga completa"
    else:
        op = "Actualización incremental"

    print("\n" + "=" * 60)
    print("  RESUMEN DE LA OPERACION")
    print("=" * 60)
    print(f"  Accion      : {op}")
    if state.get("stadiums_only"):
        print(f"  Competicion : Todas las ligas del wizard (WORKING_COMPETITIONS)")
    else:
        print(f"  Competicion : {state['competition']}")
    print(f"  Temporada   : {state['season']}")

    # En el sub-flujo de estadios no aplican fuente ni filtro de partidos
    if not state.get("stadiums_only"):
        print(f"  Fuente(s)   : {state['source']}")
        if state.get("match_filter", {}).get("match_type") == "team":
            print(f"  Filtro      : sólo equipo '{state['match_filter']['team_slug']}'")
        elif state.get("match_filter", {}).get("from_date"):
            print(f"  Filtro      : partidos desde {state['match_filter']['from_date']}")
        else:
            print( "  Filtro      : todos los partidos")
    else:
        print(f"  Fuente      : Transfermarkt (única para estadios)")
        print(f"  Destino     : dim_stadium")
    print("=" * 60)


def confirm_summary(state: Dict[str, Any]) -> str:
    """Muestra el resumen y pide confirmación final.

    Devuelve 'go' | BACK | (lanza WizardQuit).
    """
    _print_summary(state)
    print("\n  S = ejecutar     0 = volver atrás     Q = salir")
    while True:
        ans = input("¿Lanzar el pipeline? [S/0/q]: ").strip().lower()
        if ans in ("", "s", "si", "sí", "y", "yes"):
            return "go"
        if ans == "0":
            return BACK
        if ans == "q":
            raise WizardQuit()
        print("  [!] Responde S, 0 o Q.")


# --------------------------------------------------------------------------- #
# Export helper (team-specific)
# --------------------------------------------------------------------------- #
def export_matches_for_team(team_slug: str, competition: str, season: str) -> Optional[Path]:
    """Genera CSV con los partidos del equipo seleccionado para la temporada."""
    print(f"\n[EXPORT] Generando CSV con los partidos de {team_slug}...")

    with engine.connect() as conn:
        like_pattern = f"%{team_slug.replace('-', ' ')}%"
        row = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) LIKE :like LIMIT 1"),
            {"like": like_pattern.lower()},
        ).fetchone()
        if not row:
            print("  [ERROR] No se encontró el equipo en dim_team.")
            return None
        team_cid = row[0]

        matches = conn.execute(
            text(
                """
                SELECT
                    m.match_id,
                    m.match_date,
                    m.competition,
                    m.season,
                    m.home_team_id,
                    m.away_team_id,
                    m.home_score,
                    m.away_score,
                    m.data_source,
                    m.id_sofascore,
                    m.id_understat,
                    m.id_statsbomb,
                    m.id_whoscored
                FROM dim_match m
                WHERE (m.home_team_id = :tid OR m.away_team_id = :tid)
                  AND m.season = :season
                ORDER BY m.match_date
                """
            ),
            {"tid": team_cid, "season": season},
        ).fetchall()

        if not matches:
            print("  [INFO] No hay partidos para ese equipo en la temporada seleccionada.")
            return None

        export_dir = Path("data") / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        out_path = export_dir / f"{competition}_{season}_team_{team_slug}.csv"

        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "match_id", "match_date", "competition", "season",
                    "home_team_id", "away_team_id", "home_score", "away_score",
                    "data_source", "id_sofascore", "id_understat",
                    "id_statsbomb", "id_whoscored",
                ]
            )
            for r in matches:
                writer.writerow(r)

        print(f"  [OK] CSV creado en {out_path}")
        return out_path


# --------------------------------------------------------------------------- #
# Sub-flujo: descarga de estadios (Transfermarkt)
# --------------------------------------------------------------------------- #
def run_stadiums_flow(competition: str, season: str, full_refresh: bool = False) -> None:
    """
    Descarga estadios desde Transfermarkt y los carga en `dim_stadium`.

    Args:
        competition: nombre canónico (ej. "La Liga") tal como está en
                     `dim_competition`.
        season:      etiqueta tipo "2024/2025".
        full_refresh: ignora la caché de 30 días del scraper.
    """
    from scrapers.transfermarkt_stadiums_scraper import (
        scrape_transfermarkt_stadiums,
        resolve_competition_from_db,
    )
    from loaders.stadium_loader import load_stadiums

    print("\n=== DESCARGA DE ESTADIOS (Transfermarkt) ===")
    print(f"  Competición: {competition}")
    print(f"  Temporada  : {season}")

    # 1) Resolver league_code: primero contra dim_competition (verdad),
    #    fallback al dict COMPETITIONS para no bloquear si falta la fila.
    comp_db = resolve_competition_from_db(competition)
    if comp_db:
        league_code = comp_db["id_transfermarkt"]
    else:
        comp_conf = get_competition(competition)
        league_code = (
            (comp_conf or {}).get("sources", {})
                              .get("transfermarkt", {})
                              .get("league_code")
        )
    if not league_code:
        print(f"  [ERROR] No hay league_code de Transfermarkt para '{competition}'.")
        return

    season_start  = get_season_start_year(season)
    season_label  = season.replace("/", "_")

    # 2) Scrape
    print("\n[1/2] Scrapeando estadios desde Transfermarkt...")
    try:
        scrape_transfermarkt_stadiums(
            competition_name=competition,
            league_code=league_code,
            season=season_start,
            season_label=season_label,
            full_refresh=full_refresh,
        )
    except Exception as e:
        print(f"  [ERROR] Falló el scraper de estadios: {e}")
        return

    # 3) Load → dim_stadium
    print("\n[2/2] Cargando dim_stadium...")
    try:
        with engine.begin() as conn:
            # El loader normaliza competition (acepta nombre humano o slug)
            # y season (acepta '2024/2025' o '2024_2025').
            n = load_stadiums(
                conn,
                competition=competition,
                season=season_label,
            )
        print(f"\n  [OK] {n} estadio(s) upserteados en dim_stadium.")
    except Exception as e:
        print(f"  [ERROR] Falló el loader: {e}")


def run_all_stadiums_flow(season: str, full_refresh: bool = False) -> None:
    """
    Itera las competiciones de clubes del wizard (WORKING_COMPETITIONS) y
    ejecuta run_stadiums_flow para cada una. Las selecciones nacionales se
    omiten automaticamente porque /stadion/verein/<team_id> no aplica a
    federaciones.

    Equivale al modo --all-db del scraper CLI, pero invocable desde el
    wizard interactivo (CLI o Streamlit). En esta version el usuario NO
    elige liga: se itera la lista canonica del wizard.
    """
    from wizard.competitions import WORKING_COMPETITIONS
    from scripts.competitions import (
        COMPETITIONS,
        get_competition_slug_transfermarkt,
    )

    # Misma lista negra que el scraper --all-db: selecciones nacionales sin
    # estadio de club no tienen sentido aqui.
    NATIONAL_TEAM_COMPS = {
        "FIFA World Cup",
        "European Championship",
        "Copa America",
        "UEFA Women's EURO",
        "FIFA Women's World Cup",
        "UEFA Nations League A",
        "UEFA Nations League B",
        "UEFA Nations League C",
        "UEFA Nations League D",
        "World Cup Qualification UEFA",
        "World Cup Qualification CONMEBOL",
        "Int. Friendly",
        "Africa Cup of Nations",
        "Asian Cup",
    }

    # Mantener el orden del wizard (Ligas nacionales → continentales → ...)
    seen, ordered = set(), []
    for _bucket, names in WORKING_COMPETITIONS.items():
        for n in names:
            if n in seen:
                continue
            seen.add(n)
            ordered.append(n)

    to_process, skipped = [], []
    for name in ordered:
        cfg = COMPETITIONS.get(name, {})
        league_code = (cfg.get("sources", {})
                          .get("transfermarkt", {})
                          .get("league_code"))
        reasons = []
        if not league_code:
            reasons.append("sin league_code TM")
        if not get_competition_slug_transfermarkt(name):
            reasons.append("sin slug TM")
        if name in NATIONAL_TEAM_COMPS:
            reasons.append("seleccion nacional")
        if reasons:
            skipped.append((name, "; ".join(reasons)))
        else:
            to_process.append(name)

    print("\n=== DESCARGA MASIVA DE ESTADIOS (Transfermarkt) ===")
    print(f"  Temporada     : {season}")
    print(f"  Procesando    : {len(to_process)} competiciones")
    print(f"  Omitidas      : {len(skipped)}")
    if skipped:
        print("  Detalle omitidas:")
        for name, why in skipped:
            print(f"    - {name}: {why}")

    ok: list[str] = []
    failed: list[tuple[str, str]] = []
    for i, name in enumerate(to_process, start=1):
        print(f"\n----- [{i}/{len(to_process)}] {name} -----")
        try:
            run_stadiums_flow(name, season, full_refresh=full_refresh)
            ok.append(name)
        except Exception as e:  # noqa: BLE001
            print(f"  [ERROR] Fallo en '{name}': {e}")
            failed.append((name, str(e)))

    print("\n=== RESUMEN DESCARGA MASIVA DE ESTADIOS ===")
    print(f"  OK    : {len(ok)}/{len(to_process)}")
    if failed:
        print(f"  Fallos: {len(failed)}")
        for name, err in failed:
            print(f"    - {name}: {err}")


# --------------------------------------------------------------------------- #
# Flujo interactivo con navegación entre pasos
# --------------------------------------------------------------------------- #
PHASES = ["operation", "competition", "season", "source", "filter", "confirm"]


def interactive_flow() -> None:
    """Orquesta el wizard permitiendo retroceder entre pasos."""
    print("\n=== FOOTBALL DATA PIPELINE — WIZARD ===")
    state: Dict[str, Any] = {}

    idx = 0
    try:
        while idx < len(PHASES):
            phase = PHASES[idx]

            if phase == "operation":
                op = choose_operation()
                if op == BACK:
                    print("\n  Saliendo del wizard. Hasta luego!")
                    return
                state["operation"] = op
                state["full_scrape"] = op.lower().startswith("descargar temporada")
                state["stadiums_only"] = "estadios" in op.lower()
                # En el flujo de estadios el usuario NO elige liga: se
                # iteran todas las ligas de WORKING_COMPETITIONS.
                if state["stadiums_only"]:
                    state["competition"] = "ALL"
                idx += 1

            elif phase == "competition":
                # En modo estadios saltamos la eleccion de liga.
                if state.get("stadiums_only"):
                    idx += 1
                    continue
                comp = choose_competition()
                if comp == BACK:
                    idx -= 1
                    continue
                state["competition"] = comp
                state["comp_conf"] = get_competition(comp)
                if not state["comp_conf"]:
                    print(f"  [ERROR] Competicion '{comp}' no encontrada.")
                    continue
                idx += 1

            elif phase == "season":
                season = choose_season()
                if season == BACK:
                    idx -= 1
                    continue
                state["season"] = season
                state["season_start"] = get_season_start_year(season)
                # Si el usuario eligió "Descargar estadios", saltamos
                # source/filter y vamos directos a confirmar.
                if state.get("stadiums_only"):
                    idx = PHASES.index("confirm")
                else:
                    idx += 1
                continue

            elif phase == "source":
                src = choose_source(state["comp_conf"], state["competition"], state["season"])
                if src == BACK:
                    idx -= 1
                    continue
                state["source"] = src
                idx += 1

            elif phase == "filter":
                f = choose_match_filter(state["comp_conf"], state["season_start"])
                if f.get("_sentinel") == BACK:
                    idx -= 1
                    continue
                state["match_filter"] = f
                idx += 1

            elif phase == "confirm":
                ans = confirm_summary(state)
                if ans == BACK:
                    idx -= 1
                    continue
                idx += 1

            else:
                idx += 1

    except WizardQuit:
        print("\n  Saliendo del wizard. Hasta luego!")
        return

    # -- Ejecucion ------------------------------------------------
    # Caso especial: solo estadios -> iteramos TODAS las ligas del wizard.
    if state.get("stadiums_only"):
        print("\n=== INICIANDO DESCARGA MASIVA DE ESTADIOS ===")
        run_all_stadiums_flow(state["season"])
        print("\n=== PROCESO FINALIZADO EXITOSAMENTE ===")
        return

    match_filter = state.get("match_filter", {})
    kwargs = {
        "scrape": state["full_scrape"],
        "load": False,
        "competition": state["competition"],
        "source": state["source"],
        "season": state["season"],
        "from_date": match_filter.get("from_date"),
        "update": not state["full_scrape"],
    }

    print("\n=== INICIANDO EL PROCESO ===")
    run_pipeline(**kwargs)

    if match_filter.get("team_slug"):
        export_matches_for_team(
            match_filter["team_slug"],
            state["competition"].replace(" ", "_"),
            state["season"],
        )

    print("\n=== PROCESO FINALIZADO EXITOSAMENTE ===")


# --------------------------------------------------------------------------- #
# CLI (no interactivo)
# --------------------------------------------------------------------------- #
def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Football data wizard — interactive or CLI",
        epilog="Sin argumentos arranca en modo interactivo.",
    )
    parser.add_argument("--competition", help="Nombre de la competición (ej. 'La Liga')")
    parser.add_argument("--season", help="Temporada (ej. 2024/2025)")
    parser.add_argument("--source",
                        choices=["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"],
                        default="all",
                        help="Fuente(s) de datos (default: all)")
    parser.add_argument("--scrape", action="store_true", help="Forzar scrape completo")
    parser.add_argument("--update", action="store_true", help="Update incremental")
    parser.add_argument("--from-date", help="Fecha mínima YYYY-MM-DD")
    parser.add_argument("--team", help="Slug del equipo para exportar")
    return parser.parse_args()


def main() -> None:
    args = parse_cli_args()

    if not any([args.competition, args.season, args.source != "all",
                args.scrape, args.update, args.from_date, args.team]):
        interactive_flow()
        return

    competition = args.competition
    if not competition:
        print("ERROR: debes especificar --competition")
        sys.exit(1)
    comp_conf = get_competition(competition)
    if not comp_conf:
        print(f"ERROR: la competición '{competition}' no existe en competitions.py.")
        sys.exit(1)
    if competition not in db_competition_names():
        print(
            f"ERROR: la competición '{competition}' no está registrada en "
            f"`dim_competition`. Inserta una fila para esa competición antes "
            f"de lanzar el pipeline."
        )
        sys.exit(1)

    season = args.season or get_current_season()

    kwargs = {
        "scrape": args.scrape or not args.update,
        "load": False,
        "competition": competition,
        "source": args.source,
        "season": season,
        "from_date": args.from_date,
        "update": args.update,
    }

    print("\n=== INICIANDO EL PROCESO (CLI) ===")
    run_pipeline(**kwargs)

    if args.team:
        export_matches_for_team(args.team, competition.replace(" ", "_"), season)

    print("\n=== PROCESO FINALIZADO EXITOSAMENTE ===")


if __name__ == "__main__":
    run_with_log_capture(main)
