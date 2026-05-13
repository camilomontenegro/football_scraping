"""
pipeline_runner.py
==================
Orquestador principal del pipeline ETL de fútbol (versión unificada).

Combina:
  • Los scrapers existentes (incluyendo los genéricos sofascore_generico.py
    y understat_generico.py).
  • La API de orquestación que el wizard.py necesita (run_pipeline rico,
    list_available_competitions, get_current_season, get_last_match_date, …).

Fases:
    1. SCRAPING  — cada scraper extrae y guarda datos en data/raw/<fuente>/
    2. LOAD DIM  — loaders cargan dimensiones en la DB
    3. LOAD FACT — loaders cargan hechos en la DB
"""

from __future__ import annotations

import argparse
import asyncio
import csv as _csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Re-exportamos helpers de competitions.py para que el wizard los pueda
# importar desde wizard.pipeline_runner.
from wizard.competitions import (  # noqa: F401
    COMPETITIONS,
    get_competition,
    get_source_ids,
    get_source_config,
    get_season_start_year,
    get_available_seasons,
    list_competitions,
)

__all__ = [
    "run_pipeline",
    "run_scraping",
    "run_load",
    "check_existing_data",
    "print_data_check",
    "list_available_competitions",
    "get_current_season",
    "get_last_match_date",
    "get_available_seasons",
    "available_sources_for_competition",
    "COMPETITIONS",
    "get_competition",
    "get_source_ids",
    "get_source_config",
    "get_season_start_year",
    "list_competitions",
]

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
    from loaders.team_loader import load_teams
    from loaders.player_loader import load_players
    from loaders.match_loader import load_matches
    from loaders.fact_loader import load_shots, load_events, load_injuries
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
# Helpers de consulta / temporada
# ─────────────────────────────────────────────────────────────────────
def get_current_season() -> str:
    """Temporada actual (agosto-julio)."""
    from datetime import date
    today = date.today()
    start = today.year if today.month >= 7 else today.year - 1
    return f"{start}/{start + 1}"


def _season_variants(competition: str, season: str) -> list[str]:
    """Devuelve los posibles formatos de `season` que pueden estar en BD."""
    try:
        from utils.season_utils import normalize_season as _norm
    except Exception:  # utils.season_utils may not exist in every checkout
        def _norm(s):  # type: ignore[no-redef]
            return s

    comp_config = get_competition(competition)
    comp_db_name = comp_config["name"] if comp_config else competition

    canonical = _norm(season) or season
    variants: list[str] = [canonical, season]

    parts = canonical.split("/") if "/" in canonical else []
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        a_full, b_full = parts[0], parts[1]
        a_short, b_short = a_full[-2:], b_full[-2:]
        variants += [
            f"{a_short}/{b_short}",
            f"{a_full}/{b_short}",
            f"{a_full}",
            f"{comp_db_name} {a_short}/{b_short}",
            f"{comp_db_name} {a_full}/{b_full}",
        ]
    elif canonical.isdigit():
        variants += [canonical]
    return list(dict.fromkeys(v for v in variants if v))


def get_last_match_date(competition: str, season: str) -> Optional[str]:
    """Última fecha de partido cargada en BD para esa competición/temporada."""
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
    """Verifica qué datos existen en la BD para (competition, season)."""
    from sqlalchemy import text, bindparam
    from loaders.common import engine

    season_start = get_season_start_year(season)
    result: Dict[str, Any] = {
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
            result["match_count"] = row[1] if row else 0

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
# Source-availability helper (shared between CLI and dashboard)
# ─────────────────────────────────────────────────────────────────────
_CONTINENTAL_COUNTRIES = {"Europe", "Europa", "EU"}
_INTERNATIONAL_COUNTRIES = {"International", "Internacional", "World", "WW"}


def _is_international(comp_conf: Dict[str, Any]) -> bool:
    """True si la competición NO es de una liga doméstica de un país."""
    country = (comp_conf.get("country") or "").strip()
    code = (comp_conf.get("country_code") or "").strip().upper()
    if country in _INTERNATIONAL_COUNTRIES or code == "WW":
        return True
    if country in _CONTINENTAL_COUNTRIES or code == "EU":
        return True
    return False


def _reference_has_source(competition: str, season: str, source: str) -> bool:
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
    if not ref_path.exists():
        return True
    with ref_path.open("r", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
            if (
                row.get("competition") == competition
                and row.get("season") == season
                and row.get("source") == source
            ):
                if source in {"sofascore", "statsbomb", "whoscored"}:
                    return bool(row.get("season_id"))
                return True
    return False


def available_sources_for_competition(
    comp_conf: Dict[str, Any], competition: str, season: str
) -> List[str]:
    """Sources with valid config + reference data for the competition/season.

    Understat is excluded for any non-domestic competition (continental /
    international) since its coverage is league-only.
    """
    sources_map = comp_conf.get("sources", {})
    available: List[str] = []

    tm = sources_map.get("transfermarkt", {})
    if tm.get("league_code") and _reference_has_source(competition, season, "transfermarkt"):
        available.append("transfermarkt")

    sf = sources_map.get("sofascore", {})
    if sf.get("tournament_id") is not None and _reference_has_source(competition, season, "sofascore"):
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
    if ws.get("tournament_id") is not None and _reference_has_source(competition, season, "whoscored"):
        available.append("whoscored")

    return available


# ─────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────
def _statsbomb_season_id_from_reference(competition: str, season: str) -> Optional[int]:
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
    if not ref_path.exists():
        return None
    with ref_path.open("r", encoding="utf-8") as f:
        for row in _csv.DictReader(f):
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
) -> None:
    """Ejecuta el scraper de la fuente indicada."""
    comp_config = get_competition(competition) if competition else None
    if competition and not comp_config:
        logger.error("Competición '%s' no encontrada en competitions.py", competition)
        return

    season_start = get_season_start_year(season)

    if from_date:
        logger.info("[INFO] from_date=%s — informativo (los scrapers genéricos "
                    "descargan toda la temporada).", from_date)
    if full_refresh:
        logger.info("[INFO] full_refresh=True — se ignorará la caché local.")

    # ── Understat ──
    if source in ("all", "understat"):
        logger.info("[START] Scraping Understat...")
        if comp_config and comp_config["sources"].get("understat", {}).get("league"):
            try:
                from scrapers.understat_scraper import scrape_understat
                asyncio.run(scrape_understat(
                    competition_name=competition,
                    seasons=[season_start],
                    update=bool(from_date),
                    from_date=from_date,
                    delay=1.5,
                ))
            except Exception as e:
                logger.warning("Understat falló para %s %s: %s", competition, season, e)
        else:
            logger.info("Understat: '%s' no es liga doméstica con cobertura. Omitiendo.",
                        competition)

    # ── SofaScore ──
    if source in ("all", "sofascore"):
        logger.info("[START] Scraping SofaScore...")
        if not comp_config:
            logger.warning("SofaScore requiere una competición configurada. Omitiendo.")
        else:
            tournament_id = comp_config["sources"].get("sofascore", {}).get("tournament_id")
            if tournament_id is None:
                logger.warning("'%s' no tiene tournament_id en SofaScore. Omitiendo.", competition)
            else:
                try:
                    from scrapers.sofascore_scraper import scrape_sofascore
                    try:
                        scrape_sofascore(
                            tournament_id=tournament_id,
                            season_name=season,
                            competition_name=competition,
                            from_date=from_date,
                        )
                    except TypeError:
                        scrape_sofascore(
                            tournament_id=tournament_id,
                            season_name=season,
                        )
                except Exception as e:
                    logger.warning("SofaScore falló para %s %s: %s", competition, season, e)

    # ── Transfermarkt ──
    if source in ("all", "transfermarkt"):
        logger.info("[START] Scraping Transfermarkt...")
        try:
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
                )
            except TypeError:
                scrape_transfermarkt(league_code=league_code, season=season_start)
        except Exception as e:
            logger.warning("Transfermarkt falló para %s %s: %s", competition, season, e)

    # ── StatsBomb ──
    if source in ("all", "statsbomb"):
        logger.info("[START] Scraping StatsBomb...")
        statsbomb_season_id = _statsbomb_season_id_from_reference(competition, season)
        if statsbomb_season_id is None:
            logger.info(
                "StatsBomb: sin season_id Open Data para %s %s. Omitiendo.",
                competition, season,
            )
        else:
            try:
                from scrapers.statsbomb_scraper import scrape_statsbomb, COMPETITION_ID
                competition_id = COMPETITION_ID
                if comp_config:
                    competition_id = comp_config["sources"].get("statsbomb", {}).get("competition_id") or COMPETITION_ID
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
                logger.warning("StatsBomb falló para %s %s: %s", competition, season, e)

    # ── WhoScored ──
    if source in ("all", "whoscored"):
        logger.info("[START] Scraping WhoScored...")
        try:
            from scrapers.whoscored_scraper import scrape_whoscored
            scrape_whoscored(
                season=season,
                competition=competition or "La Liga",
            )
        except Exception as e:
            logger.warning("WhoScored falló: %s", e)


# ─────────────────────────────────────────────────────────────────────
# FASE DE CARGA
# ─────────────────────────────────────────────────────────────────────
def run_load() -> None:
    _ensure_loaders()
    logger.info("── CARGANDO DIMENSIONES ────────────────────────────")
    for name, fn in [("teams", _load_teams), ("players", _load_players), ("matches", _load_matches)]:
        try:
            with _engine.begin() as conn:
                fn(conn)
        except Exception as e:
            logger.error("Error loading %s: %s", name, e, exc_info=True)
    logger.info("── CARGANDO HECHOS (FACTS) ─────────────────────────")
    for name, fn in [("shots", _load_shots), ("events", _load_events), ("injuries", _load_injuries)]:
        try:
            with _engine.begin() as conn:
                fn(conn)
        except Exception as e:
            logger.error("Error loading %s: %s", name, e, exc_info=True)


# ─────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────
def run_pipeline(
    scrape: bool = False,
    competition: Optional[str] = None,
    source: str = "all",
    season: str = "2024/2025",
    match_ids: Optional[list] = None,
    check_only: bool = False,
    from_date: Optional[str] = None,
    update: bool = False,
) -> None:
    logger.info("=================================================================")
    logger.info("   FOOTBALL DATA PIPELINE")
    logger.info("=================================================================")
    if competition:
        logger.info("   Competición: %s", competition)
    logger.info("   Temporada: %s", season)
    logger.info("   Fuente:    %s", source)

    try:
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
            logger.info("── MODO INCREMENTAL: buscando última fecha en BD ────")
            last_date = get_last_match_date(competition or "La Liga", season)
            if last_date:
                from_date = last_date
                logger.info("   Último partido en BD: %s → scraping desde esa fecha", from_date)
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
            logger.info("── FASE 1: SCRAPING ─────────────────────────────────")
            try:
                run_scraping(
                    competition=competition,
                    source=source,
                    season=season,
                    match_ids=match_ids,
                    from_date=from_date,
                    full_refresh=scrape,
                )
            except Exception as e:
                logger.error("Error fatal en fase de scraping: %s", e, exc_info=True)
                raise SystemExit(1)

        logger.info("── FASE 2/3: CARGA EN DB ────────────────────────────")
        try:
            run_load()
        except Exception as e:
            logger.error("Error fatal en fase de carga: %s", e, exc_info=True)
            raise SystemExit(1)

        logger.info("=================================================================")
        logger.info("   PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=================================================================")
    except SystemExit:
        raise
    except Exception as e:
        logger.error("Error inesperado en pipeline: %s", e, exc_info=True)
        raise SystemExit(1)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────
def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Football Data Pipeline (unified)")
    parser.add_argument("--scrape", action="store_true")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--competition", "-c", type=str, default=None)
    parser.add_argument(
        "--source", "-s", default="all",
        choices=["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"],
    )
    current_year = datetime.now().year
    current_month = datetime.now().month
    default_season_year = current_year if current_month >= 7 else current_year - 1
    default_season = f"{default_season_year}/{default_season_year + 1}"
    parser.add_argument("--season", "-t", type=str, default=default_season)
    parser.add_argument("--match-ids", nargs="+", type=int)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--from-date", type=str, default=None)
    return parser


def main() -> None:
    parser = _build_cli_parser()
    args = parser.parse_args()
    if args.list:
        list_available_competitions()
        return
    run_pipeline(
        scrape=args.scrape,
        competition=args.competition,
        source=args.source,
        season=args.season,
        match_ids=args.match_ids,
        check_only=args.check,
        from_date=args.from_date,
        update=args.update,
    )


if __name__ == "__main__":
    main()