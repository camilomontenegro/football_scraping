"""
pipeline_runner.py
==================
Orquestador principal del pipeline ETL de fútbol (versión unificada).

Combina:
  • Los scrapers de la rama "noeli" (incluyendo los genéricos
    sofascore_generico.py y understat_generico.py).
  • La API de orquestación de la rama "last" que el wizard.py necesita
    (run_pipeline rico, list_available_competitions, get_current_season,
    get_last_match_date, …).

Fases:
    1. SCRAPING  — cada scraper extrae y guarda datos en data/raw/<fuente>/
    2. LOAD DIM  — loaders cargan dimensiones en la DB
    3. LOAD FACT — loaders cargan hechos en la DB
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import threading

# Permitir loops asíncronos anidados (necesario para Streamlit y ejecuciones
# anidadas). Es opcional: si no está instalado, seguimos funcionando en CLI.
try:
    import nest_asyncio  # type: ignore[import-not-found]
    nest_asyncio.apply()
except ImportError:
    pass  # CLI normal no lo necesita; instálalo si embebes el pipeline en otro loop

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Re-exportamos helpers de competitions.py para que el wizard los pueda
# importar desde scripts.pipeline_runner (compatibilidad con last).
from wizard.competitions import (  # noqa: F401
    COMPETITIONS,
    WORKING_COMPETITIONS,
    get_competition,
    get_source_ids,
    get_source_config,
    get_season_start_year,
    get_available_seasons,
    list_competitions,
)

# ── Loaders cargados de forma lazy ────────────────────────────────────
_loaders_loaded = False
_engine = None
_load_teams = None
_load_players = None
_load_matches = None
_load_shots = None
_load_events = None
_load_injuries = None


def _ensure_loaders() -> None:
    global _loaders_loaded, _engine, _load_teams, _load_players, _load_matches
    global _load_shots, _load_events, _load_injuries
    if _loaders_loaded:
        return
    from loaders.common import engine
    from loaders.team_loader_generico import load_teams
    from loaders.player_loader_generico import load_players
    from loaders.match_loader_generico import load_matches
    from loaders.fact_loader_generico import load_shots, load_events, load_injuries
    _engine = engine
    _load_teams = load_teams
    _load_players = load_players
    _load_matches = load_matches
    _load_shots = load_shots
    _load_events = load_events
    _load_injuries = load_injuries
    _loaders_loaded = True


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ─────────────────────────────────────────────────────────────────────
# Cancelación cooperativa
# ─────────────────────────────────────────────────────────────────────
class PipelineCancelled(Exception):
    """Lanzada cuando el usuario detiene el pipeline desde la UI."""


def _raise_if_cancelled(cancel_event: Optional[threading.Event]) -> None:
    """Comprueba el `cancel_event` y aborta con `PipelineCancelled` si está set.

    Los puntos de chequeo están entre fases (no podemos interrumpir un
    request HTTP en curso, pero sí podemos parar antes del siguiente
    scraper o antes de la fase de carga).
    """
    if cancel_event is not None and cancel_event.is_set():
        raise PipelineCancelled("Pipeline detenido por el usuario.")


# ─────────────────────────────────────────────────────────────────────
# Helpers de consulta / temporada
# ─────────────────────────────────────────────────────────────────────
def get_current_season() -> str:
    """Temporada actual (agosto-julio)."""
    from datetime import date
    today = date.today()
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start}/{start + 1}"


def _season_variants(competition: str, season: str) -> list[str]:
    """Devuelve los posibles formatos de `season` que pueden estar en BD.

    Tras normalizar con utils.season_utils.normalize_season() los nuevos
    cargas guardan SIEMPRE 'YYYY/YYYY'. Esta función mantiene compat
    con bases viejas donde aún pueda quedar 'YY/YY', 'LaLiga 25/26', etc.
    """
    from utils.season_utils import normalize_season as _norm
    comp_config = get_competition(competition)
    comp_db_name = comp_config["name"] if comp_config else competition

    canonical = _norm(season) or season
    variants: list[str] = [canonical, season]

    parts = canonical.split("/") if "/" in canonical else []
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        a_full, b_full = parts[0], parts[1]
        a_short, b_short = a_full[-2:], b_full[-2:]
        variants += [
            f"{a_short}/{b_short}",                  # "25/26"
            f"{a_full}/{b_short}",                   # "2025/26"
            f"{a_full}",                             # "2025"
            f"{comp_db_name} {a_short}/{b_short}",   # "Bundesliga 25/26"
            f"{comp_db_name} {a_full}/{b_full}",     # "Bundesliga 2025/2026"
        ]
    elif canonical.isdigit():
        variants += [canonical]
    return list(dict.fromkeys(v for v in variants if v))


def get_last_match_date(competition: str, season: str) -> Optional[str]:
    """Última fecha de partido cargada en BD para esa competición/temporada.

    Tolerante a las variantes de formato de `season` que escribe cada scraper.
    """
    from sqlalchemy import text, bindparam
    from loaders.common import engine

    comp_config = get_competition(competition)
    comp_db_name = (comp_config["name"] if comp_config else competition).lower()
    variants = _season_variants(competition, season)

    sql = text("""
        SELECT MAX(match_date) FROM dim_match
        WHERE LOWER(competition) LIKE :comp_like
          AND season IN :variants
    """).bindparams(bindparam("variants", expanding=True))

    try:
        with engine.connect() as conn:
            row = conn.execute(sql, {
                "comp_like": f"%{comp_db_name}%",
                "variants": variants,
            }).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception as e:
        logger.error("Error consultando última fecha en BD: %s", e)
    return None


def check_existing_data(competition: str, season: str, source: Optional[str] = None) -> dict:
    """Verifica qué datos existen en la BD para (competition, season).

    Tolerante a las variantes de formato de `season` que escribe cada scraper.
    """
    from sqlalchemy import text, bindparam
    from loaders.common import engine

    season_start = get_season_start_year(season)
    result = {
        "competition": competition,
        "season": season,
        "season_start_year": season_start,
        "has_data": False,
    }

    comp_config = get_competition(competition)
    comp_db_name = (comp_config["name"] if comp_config else competition).lower()
    variants = _season_variants(competition, season)
    parts = season.split("/")
    season_short = f"{parts[0][-2:]}/{parts[1][-2:]}" if len(parts) == 2 else season

    where_match = """
        WHERE LOWER(dim_match.competition) LIKE :comp_like
          AND dim_match.season IN :variants
    """
    params = {
        "comp_like": f"%{comp_db_name}%",
        "variants":  variants,
    }

    try:
        with engine.connect() as conn:
            sql_match = text(
                "SELECT MAX(match_date), COUNT(*) FROM dim_match " + where_match
            ).bindparams(bindparam("variants", expanding=True))
            row = conn.execute(sql_match, params).fetchone()
            result["last_match_date"] = str(row[0]) if row and row[0] else None
            result["match_count"]     = row[1] if row else 0

            sql_shots = text(
                "SELECT COUNT(*) FROM fact_shots f "
                "JOIN dim_match ON f.match_id = dim_match.match_id " + where_match
            ).bindparams(bindparam("variants", expanding=True))
            result["shot_count"] = conn.execute(sql_shots, params).fetchone()[0] or 0

            sql_events = text(
                "SELECT COUNT(*) FROM fact_events e "
                "JOIN dim_match ON e.match_id = dim_match.match_id " + where_match
            ).bindparams(bindparam("variants", expanding=True))
            result["event_count"] = conn.execute(sql_events, params).fetchone()[0] or 0

            # fact_injuries.season es independiente y suele venir como "25/26"
            row = conn.execute(
                text("SELECT COUNT(*), MAX(date_from) FROM fact_injuries WHERE season = :s"),
                {"s": season_short},
            ).fetchone()
            result["injury_count"] = row[0] if row else 0

            result["has_data"] = (
                result["match_count"] > 0
                or result["shot_count"] > 0
                or result["event_count"] > 0
            )
    except Exception as e:
        result["error"] = str(e)

    return result


def print_data_check(check_result: dict) -> None:
    print("\n" + "=" * 60)
    print("VERIFICACION DE DATOS EN BASE DE DATOS")
    print(f"   Competicion: {check_result['competition']}")
    print(f"   Temporada:   {check_result['season']}")
    print("=" * 60)
    if check_result.get("error"):
        print(f"\n[ERROR] {check_result['error']}")
        print("\n" + "=" * 60)
        return
    if check_result.get("has_data"):
        print(f"\n  Ultimo partido: {check_result.get('last_match_date', 'N/A')}")
        print(f"  Partidos: {check_result.get('match_count', 0):,}")
        print(f"  Shots:    {check_result.get('shot_count', 0):,}")
        print(f"  Events:   {check_result.get('event_count', 0):,}")
        print(f"  Injuries: {check_result.get('injury_count', 0):,}")
    else:
        print("\n  Sin datos para esta competición/temporada")
    print("\n" + "=" * 60)


def list_available_competitions() -> None:
    print("\n" + "=" * 60)
    print("COMPETICIONES DISPONIBLES")
    print("=" * 60)
    for comp in list_competitions():
        sources = []
        if comp.get("has_transfermarkt"):
            sources.append("TM")
        if comp.get("has_sofascore"):
            sources.append("SF")
        if comp.get("has_understat"):
            sources.append("US")
        if comp.get("has_statsbomb"):
            sources.append("SB")
        print(f"\n  {comp['name']} ({comp['country']})")
        print(f"    Fuentes: {', '.join(sources) if sources else 'Ninguna'}")
    print("\n" + "=" * 60)


# ─────────────────────────────────────────────────────────────────────
# Source availability helper (shared by CLI and Streamlit dashboard)
# ─────────────────────────────────────────────────────────────────────
_CONTINENTAL_COUNTRIES = {"Europe", "Europa", "EU"}
_INTERNATIONAL_COUNTRIES = {"International", "Internacional", "World", "WW"}


def _is_international(comp_conf: Dict[str, Any]) -> bool:
    """Return True for continental/international competitions."""
    country = (comp_conf.get("country") or "").strip()
    code = (comp_conf.get("country_code") or "").strip().upper()
    return (
        country in _INTERNATIONAL_COUNTRIES
        or country in _CONTINENTAL_COUNTRIES
        or code in {"WW", "EU"}
    )


def _reference_has_source(competition: str, season: str, source: str) -> bool:
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
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


def _sofascore_season_available(competition: str, season: str) -> bool:
    from scrapers.sofascore_seasons import sofascore_season_available
    return sofascore_season_available(competition, season)


# ─────────────────────────────────────────────────────────────────────
# Competiciones registradas en la base de datos (dim_competition)
# ─────────────────────────────────────────────────────────────────────
def db_competition_names() -> set[str]:
    """Devuelve el conjunto de `canonical_name` presentes en `dim_competition`.

    Se consulta en vivo cada vez que el wizard arranca, por lo que añadir
    o quitar filas en la tabla `dim_competition` actualiza automáticamente
    las opciones que aparecen al elegir competición — sin tener que tocar
    `WORKING_COMPETITIONS` ni ningún otro código.
    """
    try:
        from loaders.common import engine
        from sqlalchemy import text

        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT canonical_name FROM dim_competition")
            ).fetchall()
        return {r[0] for r in rows if r[0]}
    except Exception as exc:
        logger.warning("No se pudo leer dim_competition: %s", exc)
        return set()


def _competition_category(comp_conf: Dict[str, Any]) -> str:
    """Devuelve 'nacional', 'continental' o 'internacional'."""
    country = (comp_conf.get("country") or "").strip()
    code = (comp_conf.get("country_code") or "").strip().upper()
    if country in _INTERNATIONAL_COUNTRIES or code == "WW":
        return "internacional"
    if country in _CONTINENTAL_COUNTRIES or code == "EU":
        return "continental"
    return "nacional"


_CATEGORY_LABELS = {
    "nacional": "Ligas nacionales",
    "continental": "Torneos continentales",
    "internacional": "Torneos intercontinentales",
}


def grouped_db_competitions() -> List[tuple]:
    """Lista de competiciones agrupadas por categoría, filtradas por `dim_competition`.

    Sólo aparecen las competiciones que cumplen TODAS las condiciones:
        • Existen en `dim_competition.canonical_name` (la BD manda).
        • Tienen configuración en `COMPETITIONS` (necesaria para scrapear).

    El orden dentro de cada grupo respeta el de `WORKING_COMPETITIONS`
    cuando es posible y deja al final las competiciones de BD que no estén
    listadas allí (ordenadas alfabéticamente).
    """
    valid_db = db_competition_names()
    available = [name for name in valid_db if name in COMPETITIONS]

    # Orden preferido según WORKING_COMPETITIONS
    ordered: list[str] = []
    for names in WORKING_COMPETITIONS.values():
        for name in names:
            if name in available and name not in ordered:
                ordered.append(name)
    for name in sorted(available):
        if name not in ordered:
            ordered.append(name)

    grouped: Dict[str, list[str]] = {}
    for name in ordered:
        cat = _competition_category(COMPETITIONS[name])
        label = _CATEGORY_LABELS[cat]
        grouped.setdefault(label, []).append(name)

    return [
        (label, grouped[label])
        for label in _CATEGORY_LABELS.values()
        if label in grouped
    ]


def available_sources_for_competition(
    comp_conf: Dict[str, Any],
    competition: str,
    season: str,
) -> List[str]:
    """Return configured sources with reference data for competition/season."""
    sources_map = comp_conf.get("sources", {})
    available: List[str] = []

    tm = sources_map.get("transfermarkt", {})
    if tm.get("league_code") and _reference_has_source(competition, season, "transfermarkt"):
        available.append("transfermarkt")

    sf = sources_map.get("sofascore", {})
    if sf.get("tournament_id") is not None and (
        _reference_has_source(competition, season, "sofascore")
        or _sofascore_season_available(competition, season)
    ):
        available.append("sofascore")

    us = sources_map.get("understat", {})
    if (
        us.get("league")
        and not _is_international(comp_conf)
        and _reference_has_source(competition, season, "understat")
    ):
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


# ─────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────
def _comp_slug(competition: str) -> str:
    return competition.lower().replace(" ", "_")


def _sofascore_season_label(competition: str, season: str) -> str:
    comp_config = get_competition(competition) or {}
    comp_db_name = comp_config.get("name", competition)
    parts = season.split("/")
    season_short = f"{parts[0][-2:]}/{parts[1][-2:]}" if len(parts) == 2 else season
    return f"{comp_db_name} {season_short}"


def _statsbomb_season_id_from_reference(competition: str, season: str) -> Optional[int]:
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
    if not ref_path.exists():
        return None
    with ref_path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (
                row.get("competition") == competition
                and row.get("season") == season
                and row.get("source") == "statsbomb"
                and row.get("season_id")
            ):
                return int(row["season_id"])
    return None


# ─────────────────────────────────────────────────────────────────────
# FASE DE SCRAPING
# ─────────────────────────────────────────────────────────────────────
def run_scraping(
    competition: Optional[str] = None,
    source: str = "all",
    season: str = "2024/2025",
    match_ids: Optional[list] = None,
    from_date: Optional[str] = None,
    full_refresh: bool = False,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    """Ejecuta el scraper de la fuente indicada."""
    comp_config = get_competition(competition) if competition else None
    if competition and not comp_config:
        logger.error("Competición '%s' no encontrada en competitions.py", competition)
        return

    season_start = get_season_start_year(season)

    if from_date:
        logger.info("[INFO] Mantenimiento desde fecha %s (partidos >= fecha).", from_date)
    if full_refresh:
        logger.info("[INFO] full_refresh=True — se ignorará la caché local.")

    # ── Understat (Osen) ──
    _raise_if_cancelled(cancel_event)
    if source in ("all", "understat"):
        logger.info("[START] Scraping Understat...")
        if comp_config and comp_config["sources"].get("understat", {}).get("league"):
            from scrapers.understat_scraper import scrape_understat
            try:
                # Ejecución segura del scraper asíncrono
                asyncio.run(scrape_understat(
                    competition_name=competition,
                    seasons=[season_start],
                    update=bool(from_date),
                    from_date=from_date,
                    delay=1.5,
                ))
            except Exception as e:
                logger.warning("Understat fallÃ³ para %s %s: %s", competition, season, e)
        else:
            logger.info("Understat: '%s' no es liga doméstica con cobertura. Omitiendo.",
                        competition)

    # ── SofaScore (Osen) ──
    _raise_if_cancelled(cancel_event)
    if source in ("all", "sofascore"):
        logger.info("[START] Scraping SofaScore...")
        from scrapers.sofascore_scraper import scrape_sofascore
        if not comp_config:
            logger.warning("SofaScore requiere una competición configurada. Omitiendo.")
        else:
            tournament_id = comp_config["sources"].get("sofascore", {}).get("tournament_id")
            if tournament_id is None:
                logger.warning("'%s' no tiene tournament_id en SofaScore. Omitiendo.", competition)
            else:
                # IMPORTANTE: el scraper de Osen busca la temporada por
                # substring contra los nombres que devuelve la API de
                # SofaScore (p.ej. "22/23"). Si le pasamos "Premier League
                # 22/23" no matchea. Le pasamos directamente la temporada
                # canónica "YYYY/YYYY"; el scraper deduce "YY/YY" solo.
                try:
                    scrape_sofascore(
                        tournament_id=tournament_id,
                        season_name=season,           # "2022/2023"
                        competition_name=competition, # para log y carpetas
                        from_date=from_date,
                        full_refresh=full_refresh,
                    )
                except TypeError:
                    # Fallback por si la firma es distinta en otra rama
                    scrape_sofascore(
                        tournament_id=tournament_id,
                        season_name=season,
                    )
                except Exception as e:
                    logger.warning("SofaScore fallÃ³ para %s %s: %s", competition, season, e)

    # ── Transfermarkt (Osen) ──
    _raise_if_cancelled(cancel_event)
    if source in ("all", "transfermarkt"):
        logger.info("[START] Scraping Transfermarkt...")
        from scrapers.transfermarkt_scraper import scrape_transfermarkt, LEAGUE_CODE
        league_code = LEAGUE_CODE
        if comp_config:
            league_code = comp_config["sources"].get("transfermarkt", {}).get("league_code", LEAGUE_CODE)
        try:
            scrape_transfermarkt(
                competition_name=competition,
                league_code=league_code,
                season=season_start,
                from_date=from_date,
                season_label=season,
                full_refresh=full_refresh,
            )
        except TypeError:
            scrape_transfermarkt(league_code=league_code, season=season_start)
        except Exception as e:
            logger.warning("Transfermarkt fallÃ³ para %s %s: %s", competition, season, e)

    # ── StatsBomb (Osen) ──
    _raise_if_cancelled(cancel_event)
    if source in ("all", "statsbomb"):
        logger.info("[START] Scraping StatsBomb...")
        from scrapers.statsbomb_scraper import scrape_statsbomb, COMPETITION_ID
        competition_id = COMPETITION_ID
        if comp_config:
            competition_id = comp_config["sources"].get("statsbomb", {}).get("competition_id") or COMPETITION_ID
        statsbomb_season_id = _statsbomb_season_id_from_reference(competition, season)
        if statsbomb_season_id is None:
            logger.info(
                "StatsBomb: sin season_id Open Data para %s %s. Omitiendo.",
                competition, season,
            )
        else:
            try:
                scrape_statsbomb(
                    competition_name=competition,
                    competition_id=competition_id,
                    season_id=statsbomb_season_id,
                    from_date=from_date,
                )
            except TypeError:
                scrape_statsbomb(competition_id=competition_id, season_id=statsbomb_season_id)
            except Exception as e:
                logger.warning("StatsBomb fallÃ³ para %s %s: %s", competition, season, e)

    # ── WhoScored ──
    _raise_if_cancelled(cancel_event)
    if source in ("all", "whoscored"):
        logger.info("[START] Scraping WhoScored...")
        from scrapers.whoscored_scraper import scrape_whoscored
        try:
            # Pasamos competición y temporada; el scraper construye la URL
            # desde competitions.py + WHOSCORED_STAGES y normaliza el formato
            # de temporada internamente.
            scrape_whoscored(
                season=season,
                competition=competition or "La Liga",
                from_date=from_date,
                full_refresh=full_refresh,
            )
        except Exception as e:
            logger.warning("WhoScored falló: %s", e)


# ─────────────────────────────────────────────────────────────────────
# FASE DE CARGA
# ─────────────────────────────────────────────────────────────────────
def run_load(
    competition: Optional[str] = None,
    season: Optional[str] = None,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    """Carga la BD para la (competition, season) indicada.

    Si `competition` es None, itera por WORKING_COMPETITION_NAMES. `season`
    se normaliza al formato YYYY_YYYY que usan los paths data/clean.
    """
    _ensure_loaders()

    from sqlalchemy import text
    from utils.data_paths import clean_dir, normalize_season

    season_label = normalize_season(season) or season or "2025_2026"
    targets = [competition] if competition else sorted(WORKING_COMPETITION_NAMES)

    def _comp_id(conn, name: str) -> Optional[int]:
        conf = get_competition(name)
        if not conf:
            return None
        code = conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
        if not code:
            return None
        return conn.execute(
            text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"),
            {"c": code},
        ).scalar()

    for comp_name in targets:
        _raise_if_cancelled(cancel_event)
        paths = {
            "ss": clean_dir(comp_name, season_label, "sofascore"),
            "tm": clean_dir(comp_name, season_label, "transfermarkt"),
            "ws": clean_dir(comp_name, season_label, "whoscored"),
            "us": clean_dir(comp_name, season_label, "understat"),
            "sb": clean_dir(comp_name, season_label, "statsbomb"),
        }

        with _engine.begin() as conn:
            comp_id = _comp_id(conn, comp_name)
        if not comp_id:
            logger.warning(
                "%s: sin competition_id en dim_competition (ejecuta load_competitions). Skip.",
                comp_name,
            )
            continue

        logger.info("── CARGANDO DIMENSIONES — %s ────────────", comp_name)
        for name, fn, kwargs in [
            ("teams",   _load_teams,   dict(ss_path=paths["ss"], tm_path=paths["tm"], ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"])),
            ("players", _load_players, dict(tm_path=paths["tm"], ss_path=paths["ss"], ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"])),
            ("matches", _load_matches, dict(ss_path=paths["ss"], competition_id=comp_id, ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"])),
        ]:
            _raise_if_cancelled(cancel_event)
            try:
                with _engine.begin() as conn:
                    fn(conn, **kwargs)
            except Exception as e:
                logger.error("Error loading %s en %s: %s", name, comp_name, e, exc_info=True)

        logger.info("── CARGANDO HECHOS — %s ─────────────────", comp_name)
        for name, fn, kwargs in [
            ("shots",    _load_shots,    dict(ss_path=paths["ss"], competition_id=comp_id, us_path=paths["us"])),
            ("events",   _load_events,   dict(ss_path=paths["ss"], sb_path=paths["sb"], ws_path=paths["ws"])),
            ("injuries", _load_injuries, dict(tm_path=paths["tm"])),
        ]:
            _raise_if_cancelled(cancel_event)
            try:
                with _engine.begin() as conn:
                    fn(conn, **kwargs)
            except Exception as e:
                logger.error("Error loading %s en %s: %s", name, comp_name, e, exc_info=True)


# ─────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────
def run_pipeline(
    scrape: bool = False,
    load: bool = False,
    competition: Optional[str] = None,
    source: str = "all",
    season: str = "2024/2025",
    match_ids: Optional[list] = None,
    check_only: bool = False,
    from_date: Optional[str] = None,
    update: bool = False,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    logger.info("=================================================================")
    logger.info("   FOOTBALL DATA PIPELINE")
    logger.info("=================================================================")
    if competition:
        logger.info("   Competición: %s", competition)
    logger.info("   Temporada: %s", season)
    logger.info("   Fuente:    %s", source)
    logger.info("   Modo:      %s", " + ".join(
        f for f, v in [("scrape", scrape), ("load", load), ("update", update)] if v
    ) or "solo load")

    try:
        _raise_if_cancelled(cancel_event)

        if check_only:
            logger.info("── FASE 0: VERIFICACIÓN ─────────────────────────────")
            check_result = check_existing_data(
                competition or "La Liga",
                season,
                source if source != "all" else None,
            )
            print_data_check(check_result)
            return

        if update:
            logger.info("── MODO MANTENIMIENTO: última fecha en BD ───────────")
            last_date = get_last_match_date(competition or "La Liga", season)
            if last_date:
                # Mantener fecha manual del wizard si es más reciente que la BD.
                if from_date and str(from_date) > str(last_date):
                    logger.info(
                        "   Fecha manual %s (más reciente que BD %s) → se usa la manual",
                        from_date, last_date,
                    )
                else:
                    from_date = last_date
                    logger.info(
                        "   Último partido en BD: %s → scrapers desde esa fecha",
                        from_date,
                    )
                current_season = get_current_season()
                if current_season != season:
                    logger.info(
                        "   Temporada actual detectada: %s (era %s) → scraping de la nueva temporada",
                        current_season, season,
                    )
                    season = current_season
            else:
                logger.warning(
                    "No se encontraron partidos en BD para %s %s. "
                    "Se descargará la temporada completa.",
                    competition, season,
                )
            scrape = True

        if from_date:
            logger.info("   Desde fecha: %s", from_date)

        if scrape:
            _raise_if_cancelled(cancel_event)
            logger.info("── FASE 1: SCRAPING ─────────────────────────────────")
            try:
                run_scraping(
                    competition=competition,
                    source=source,
                    season=season,
                    match_ids=match_ids,
                    from_date=from_date,
                    full_refresh=scrape and not update,
                    cancel_event=cancel_event,
                )
            except PipelineCancelled:
                raise
            except Exception as e:
                logger.error("Error fatal en fase de scraping: %s", e, exc_info=True)
                raise SystemExit(1)

        if load:
            _raise_if_cancelled(cancel_event)
            logger.info("── FASE 2/3: CARGA EN DB ────────────────────────────")
            try:
                run_load(competition=competition, season=season, cancel_event=cancel_event)
            except PipelineCancelled:
                raise
            except Exception as e:
                logger.error("Error fatal en fase de carga: %s", e, exc_info=True)
                raise SystemExit(1)
        elif scrape:
            logger.info("")
            logger.info("   Los datos se han descargado a data/raw/ y data/clean/.")
            logger.info("   Para cargarlos en la BD ejecuta:")
            logger.info("     python wizard/pipeline_runner.py --load")
            logger.info("")

        logger.info("=================================================================")
        logger.info("   PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=================================================================")
    except PipelineCancelled as e:
        logger.warning("=================================================================")
        logger.warning("   PIPELINE CANCELADO POR EL USUARIO")
        logger.warning("=================================================================")
        raise
    except SystemExit:
        raise
    except Exception as e:
        logger.error("Error inesperado en pipeline: %s", e, exc_info=True)
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Football Data Pipeline (unified)")
    parser.add_argument("--scrape", action="store_true",
                        help="Descargar datos (scraping). No carga en BD automáticamente.")
    parser.add_argument("--load", action="store_true",
                        help="Cargar CSVs existentes a BD.")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--competition", "-c", type=str, default=None)
    parser.add_argument("--source", "-s", default="all",
                        choices=["all", "understat", "sofascore", "transfermarkt",
                                 "statsbomb", "whoscored"])
    current_year = datetime.now().year
    current_month = datetime.now().month
    default_season_year = current_year if current_month >= 7 else current_year - 1
    default_season = f"{default_season_year}/{default_season_year + 1}"
    parser.add_argument("--season", "-t", type=str, default=default_season)
    parser.add_argument("--match-ids", nargs="+", type=int)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--from-date", type=str, default=None)
    args = parser.parse_args()

    if args.list:
        list_available_competitions()
    else:
        # Si no se pide ni --scrape ni --load ni --update ni --check,
        # mostrar ayuda para evitar ejecuciones vacías.
        if not any([args.scrape, args.load, args.update, args.check]):
            parser.print_help()
            raise SystemExit(0)
        run_pipeline(
            scrape=args.scrape,
            load=args.load,
            competition=args.competition,
            source=args.source,
            season=args.season,
            match_ids=args.match_ids,
            check_only=args.check,
            from_date=args.from_date,
            update=args.update,
        )
