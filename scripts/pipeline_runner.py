"""
pipeline_runner.py
==================
Orquestador principal del pipeline ETL de fútbol.

Fases:
    1. SCRAPING  — cada scraper extrae y guarda datos en data/raw/<fuente>/
    2. LOAD DIM  — loaders cargan dimensiones en la DB (dim_team, dim_player, dim_match)
    3. LOAD FACT — loaders cargan hechos en la DB (fact_shots, fact_events, fact_injuries)

Scrapers disponibles:
    - scrapers/understat_scraper.py   -> dim_match, fact_shots
    - scrapers/sofascore_scraper.py   -> dim_match, dim_team, dim_player, fact_shots, fact_events
    - scrapers/transfermarkt_scraper.py -> dim_player (canónico), fact_injuries
    - scrapers/statsbomb_scraper.py   -> dim_match, dim_team, dim_player, fact_events
    - scrapers/whoscored_scraper.py   -> dim_player, fact_events

Uso:
    python -m scripts.pipeline_runner                                       # Solo carga (asume data/raw/ ya existe)
    python -m scripts.pipeline_runner --scrape                              # Scraping completo + carga
    python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --scrape
    python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --update  # Incremental desde BD
    python -m scripts.pipeline_runner --competition "Premier League" --season 2023/2024 --check
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from scripts.competitions import (
    COMPETITIONS,
    get_competition,
    get_season_start_year,
    get_available_seasons,
    list_competitions,
)

# Loaders cargados de forma lazy para evitar error de DB en --list y --check
_loaders_loaded = False
_engine = None
_load_teams = None
_load_players = None
_load_matches = None
_load_shots = None
_load_events = None
_load_injuries = None


def _ensure_loaders():
    """Carga los loaders lazily para evitar error de DB en comandos de solo consulta."""
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


# ── CONSULTA A LA BASE DE DATOS ──────────────────────────────────────

def get_current_season() -> str:
    """Devuelve la temporada de fútbol actual basándose en la fecha de hoy.

    Las temporadas de fútbol corren de agosto a julio:
        - Agosto–Diciembre del año N  → temporada N/N+1
        - Enero–Julio del año N       → temporada N-1/N

    Ejemplos:
        Hoy = 2026-04-30 → "2025/2026"  (mes 4 < 7 → season start = 2025)
        Hoy = 2025-09-01 → "2025/2026"  (mes 9 >= 7 → season start = 2025)
    """
    from datetime import date
    today = date.today()
    if today.month >= 7:
        start = today.year
    else:
        start = today.year - 1
    return f"{start}/{start + 1}"


def get_last_match_date(competition: str, season: str) -> Optional[str]:
    """Consulta la BD y devuelve la fecha del último partido insertado.

    Busca en dim_match el MAX(match_date) para la competición y temporada dadas.

    Args:
        competition: Nombre de la competición (ej: "La Liga")
        season:      Temporada en formato "2024/2025"

    Returns:
        Fecha en formato "YYYY-MM-DD" o None si no hay datos.
    """
    from sqlalchemy import text
    from loaders.common import engine

    # Construir el nombre de temporada que usa SofaScore en la BD
    # "La Liga" + "2024/2025" -> "LaLiga 24/25"
    comp_config = get_competition(competition)
    comp_db_name = comp_config["name"] if comp_config else competition

    parts = season.split("/")
    if len(parts) == 2:
        season_short = f"{parts[0][-2:]}/{parts[1][-2:]}"
    else:
        season_short = season
    db_season = f"{comp_db_name} {season_short}"

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT MAX(match_date) FROM dim_match WHERE season = :season"),
                {"season": db_season},
            ).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception as e:
        logger.error("Error consultando última fecha en BD: %s", e)
    return None


def check_existing_data(competition: str, season: str, source: str = None) -> dict:
    """Verifica qué datos existen en la base de datos para la competición/temporada.

    Returns:
        Dict con información de qué datos existen en la DB.
    """
    from sqlalchemy import text
    from loaders.common import engine

    season_start = get_season_start_year(season)
    result = {
        "competition": competition,
        "season": season,
        "season_start_year": season_start,
        "has_data": False,
    }

    is_la_liga = competition.lower() in ["la liga", "laliga", "liga"]
    parts = season.split("/")
    season_short = parts[0][-2:] + "/" + parts[1][-2:] if len(parts) == 2 else season
    db_competition = "LaLiga" if is_la_liga else competition
    db_season = f"{db_competition} {season_short}"

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT MAX(match_date), COUNT(*) FROM dim_match WHERE season = :season"),
                {"season": db_season},
            ).fetchone()
            result["last_match_date"] = str(row[0]) if row and row[0] else None
            result["match_count"] = row[1] if row else 0

            result["shot_count"] = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_shots f
                JOIN dim_match m ON f.match_id = m.match_id
                WHERE m.season = :season
            """), {"season": db_season}).fetchone()[0] or 0

            result["event_count"] = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_events e
                JOIN dim_match m ON e.match_id = m.match_id
                WHERE m.season = :season
            """), {"season": db_season}).fetchone()[0] or 0

            row = conn.execute(
                text("SELECT COUNT(*), MAX(date_from) FROM fact_injuries WHERE season = :season"),
                {"season": season_short},
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


def print_data_check(check_result: dict):
    """Imprime el resultado de la verificación de datos desde la DB."""
    print("\n" + "=" * 60)
    print("VERIFICACION DE DATOS EN BASE DE DATOS")
    print(f"   Competicion: {check_result['competition']}")
    print(f"   Temporada: {check_result['season']}")
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


def list_available_competitions():
    """Lista todas las competiciones disponibles con sus fuentes."""
    print("\n" + "=" * 60)
    print("COMPETICIONES DISPONIBLES")
    print("=" * 60)

    for comp in list_competitions():
        sources = []
        if comp.get("has_transfermarkt"): sources.append("TM")
        if comp.get("has_sofascore"):     sources.append("SF")
        if comp.get("has_understat"):     sources.append("US")
        if comp.get("has_statsbomb"):     sources.append("SB")

        print(f"\n  {comp['name']} ({comp['country']})")
        print(f"    Fuentes: {', '.join(sources) if sources else 'Ninguna'}")

    print("\n" + "=" * 60)


# ── FASE DE SCRAPING ──────────────────────────────────────────────────

def run_scraping(
    competition: str = None,
    source: str = "all",
    season: str = "2024/2025",
    match_ids: list = None,
    from_date: str = None,
    full_refresh: bool = False,
):
    """Ejecuta el scraper de la fuente indicada.

    Args:
        competition: Nombre de la competición (ej: "La Liga"). Si es None usa valores por defecto.
        source:      'all' | 'understat' | 'sofascore' | 'transfermarkt' | 'statsbomb' | 'whoscored'
        season:      Temporada en formato legible (p.ej. '2024/2025')
        match_ids:   Lista de IDs para WhoScored (solo si source='whoscored')
        from_date:   Fecha inicial para scraping incremental (YYYY-MM-DD).
        full_refresh: Si True, ignora caché local/BD y fuerza descarga completa.
    """
    comp_config = None
    if competition:
        comp_config = get_competition(competition)
        if not comp_config:
            logger.error("Competición '%s' no encontrada", competition)
            return

    season_start = get_season_start_year(season)

    # Understat
    if source in ("all", "understat"):
        logger.info("[START] Scraping Understat...")
        from scrapers.understat_scraper import scrape_understat

        understat_cfg = None
        if comp_config:
            understat_cfg = comp_config["sources"].get("understat", {})

        if not understat_cfg or not understat_cfg.get("league"):
            logger.warning("[SKIP] '%s' no tiene configuración de Understat.", competition)
        else:
            asyncio.run(
                scrape_understat(
                    competition_name=competition or "La Liga",
                    seasons=[season_start],
                    update=bool(from_date),
                )
            )

    # SofaScore
    if source in ("all", "sofascore"):
        logger.info("[START] Scraping SofaScore...")
        from scrapers.sofascore_scraper import scrape_sofascore

        tournament_id = None
        if comp_config:
            tournament_id = comp_config["sources"].get("sofascore", {}).get("tournament_id")

        scrape_sofascore(
            competition_name=competition,
            season_name=season, 
            tournament_id=tournament_id, 
            from_date=from_date,
            full_refresh=full_refresh
        )

    # Transfermarkt
    if source in ("all", "transfermarkt"):
        logger.info("[START] Scraping Transfermarkt...")
        from scrapers.transfermarkt_scraper import scrape_transfermarkt

        league_code = None
        if comp_config:
            league_code = comp_config["sources"].get("transfermarkt", {}).get("league_code")

        scrape_transfermarkt(
            competition_name=competition,
            league_code=league_code, 
            season=season_start, 
            from_date=from_date,
            full_refresh=full_refresh,
            season_label=season
        )

    # StatsBomb
    if source in ("all", "statsbomb"):
        logger.info("[START] Scraping StatsBomb...")
        from scrapers.statsbomb_scraper import scrape_statsbomb

        competition_id = None
        if comp_config:
            competition_id = comp_config["sources"].get("statsbomb", {}).get("competition_id")

        scrape_statsbomb(competition_id=competition_id, season_id=season_start, from_date=from_date)
        scrape_statsbomb(
            competition_name=competition,
            competition_id=competition_id, 
            season_id=season_start, 
            from_date=from_date
        )

    # WhoScored
    if source in ("all", "whoscored"):
        logger.info("[START] Scraping WhoScored...")
        from scrapers.whoscored_scraper import scrape_whoscored
        scrape_whoscored(
            competition=competition or "La Liga",
            season=season,
            from_date=from_date,
            match_ids=match_ids,
            full_refresh=full_refresh
        )


# ── FASE DE CARGA ──────────────────────────────────────────────────────

def run_load(competition: str = None):
    """Carga los datos de data/raw/ en la base de datos para la competición especificada o todas."""
    _ensure_loaders()

    from scripts.competitions import get_competition, list_competitions

    competitions_to_load = [competition] if competition else list_competitions()

    for comp_data in competitions_to_load:
        comp_name = comp_data['name'] if isinstance(comp_data, dict) else comp_data
        comp_config = get_competition(comp_name)
        if not comp_config:
            logger.warning(f"No se encontró configuración para la competición '{comp_name}'. Saltando...")
            continue

        logger.info(f"\n==========================================")
        logger.info(f"   CARGANDO DATOS: {comp_config['name'].upper()}")
        logger.info(f"==========================================")

        logger.info("── CARGANDO DIMENSIONES ────────────────────────────")
        for name, fn in [
            ("teams",   _load_teams),
            ("players", _load_players),
            ("matches", _load_matches),
        ]:
            try:
                with _engine.begin() as conn:
                    fn(conn, comp_name)
            except Exception as e:
                logger.error("Error loading %s for %s: %s", name, comp_name, e, exc_info=True)

        logger.info("── CARGANDO HECHOS (FACTS) ─────────────────────────")
        for name, fn in [
            ("shots",    _load_shots),
            ("events",   _load_events),
            ("injuries", _load_injuries),
        ]:
            try:
                with _engine.begin() as conn:
                    fn(conn, comp_name)
            except Exception as e:
                logger.error("Error loading %s for %s: %s", name, comp_name, e, exc_info=True)


# ── ORCHESTRATOR ───────────────────────────────────────────────────────

def run_pipeline(
    scrape: bool = False,
    competition: str = None,
    source: str = "all",
    season: str = "2024/2025",
    match_ids: list = None,
    check_only: bool = False,
    from_date: str = None,
    update: bool = False,
    full_refresh: bool = False,
    scrape_only: bool = False,
):
    """Orquesta las fases de scraping y carga.

    Args:
        scrape:     Si True, ejecuta fase de scraping antes de cargar.
        competition: Nombre de la competición.
        source:     Fuente de datos ('all' o nombre concreto).
        season:     Temporada (ej: '2024/2025').
        match_ids:  IDs de partido para WhoScored.
        check_only: Si True, solo verifica datos en BD y sale.
        from_date:  Fecha mínima para scraping incremental (YYYY-MM-DD).
        update:     Si True, consulta la BD para obtener from_date automáticamente
                    y lanza scraping incremental + carga.
    """
    logger.info("=================================================================")
    logger.info("   FOOTBALL DATA PIPELINE")
    logger.info("=================================================================")

    if competition:
        logger.info("   Competición: %s", competition)
    logger.info("   Temporada: %s", season)
    logger.info("   Fuente: %s", source)

    try:
        # ── Fase 0: solo verificar ──────────────────────────────
        if check_only:
            logger.info("── FASE 0: VERIFICACIÓN ─────────────────────────────")
            check_result = check_existing_data(
                competition, season, source if source != "all" else None
            )
            print_data_check(check_result)
            return

        # ── Modo incremental (--update) ─────────────────────────
        if update:
            logger.info("── MODO INCREMENTAL: buscando última fecha en BD ────")
            last_date = get_last_match_date(competition or "La Liga", season)
            if last_date:
                from_date = last_date
                logger.info("   Último partido en BD: %s → scraping desde esa fecha", from_date)
                # Calcular la temporada actual desde HOY, no desde --season.
                # Si el último partido es 2025-05-25 (fin de 24/25), los datos
                # nuevos pertenecen a la siguiente temporada (25/26).
                current_season = get_current_season()
                if current_season != season:
                    logger.info(
                        "   Temporada actual detectada: %s (era %s) → scraping sobre la nueva temporada",
                        current_season, season,
                    )
                    season = current_season
            else:
                logger.warning(
                    "No se encontraron partidos en BD para %s %s. "
                    "Se descargará la temporada completa.",
                    competition, season,
                )
            scrape = True  # --update implica scraping

        if from_date:
            logger.info("   Desde fecha: %s", from_date)

        # ── Fase 1: scraping ────────────────────────────────────
        if scrape:
            logger.info("── FASE 1: SCRAPING ─────────────────────────────────")
            try:
                run_scraping(
                    competition=competition,
                    source=source,
                    season=season,
                    match_ids=match_ids,
                    from_date=from_date,
                    full_refresh=full_refresh,
                )
            except Exception as e:
                logger.error("Error fatal en fase de scraping: %s", e, exc_info=True)
                raise SystemExit(1)

        # ── Fases 2/3: carga ────────────────────────────────────
        if scrape_only:
            logger.info("── MODO SCRAPE-ONLY: saltando fase de carga ─────────")
            return

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


# ── PUNTO DE ENTRADA ───────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Football Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Ver qué datos existen para La Liga 2024/25
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --check

  # Listar competiciones disponibles
  python -m scripts.pipeline_runner --list

  # Scraping completo de una temporada
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --scrape

  # Scraping incremental: auto-detecta la última fecha en BD y descarga desde ahí
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --update

  # Scraping incremental de una sola fuente
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --source sofascore --update

  # Scraping desde una fecha manual
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --from-date 2025-03-01 --scrape
        """,
    )
    parser.add_argument(
        "--scrape", action="store_true",
        help="Ejecutar fase de scraping antes de cargar (temporada completa)",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Scraping incremental: consulta la BD para obtener la última fecha "
             "y descarga solo partidos nuevos desde esa fecha",
    )
    parser.add_argument(
        "--competition", "-c", type=str, default=None,
        help="Nombre de la competición (ej: 'La Liga', 'Premier League'). Use --list para ver disponibles.",
    )
    parser.add_argument(
        "--source", "-s", nargs="+", default=["all"],
        help="Fuentes de datos a scrapear (default: all). Opciones: understat, sofascore, transfermarkt, statsbomb, whoscored",
    )
    # Calcular temporada actual por defecto
    current_year = datetime.now().year
    current_month = datetime.now().month
    # Si estamos antes de Julio, la temporada actual es la que empezó el año pasado
    default_season_year = current_year if current_month >= 7 else current_year - 1
    default_season = f"{default_season_year}/{default_season_year + 1}"

    parser.add_argument(
        "--season", "-t", nargs="+", default=[default_season],
        help=f"Temporadas (default: {default_season}). Formato: 2024/2025",
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int,
        help="IDs de partido para WhoScored",
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Solo verificar qué datos existen para la competición/temporada",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Listar todas las competiciones disponibles",
    )
    parser.add_argument(
        "--from-date", type=str, default=None,
        help="Fecha inicial manual para scraping incremental (YYYY-MM-DD). "
             "Descarga solo partidos desde esta fecha. Use --update para auto-detectar desde BD.",
    )
    parser.add_argument(
        "--full-refresh", action="store_true",
        help="Ignorar caché y BD: fuerza descarga completa de todos los datos.",
    )
    parser.add_argument(
        "--scrape-only", action="store_true",
        help="Ejecutar solo la fase de scraping y salir (no cargar en DB)",
    )
    args = parser.parse_args()

    if args.list:
        list_available_competitions()
    else:
        # Soporte para múltiples fuentes y temporadas
        sources = args.source
        if "all" in sources:
            sources = ["all"]
        
        for ssn in args.season:
            for src in sources:
                run_pipeline(
                    scrape=args.scrape,
                    competition=args.competition,
                    source=src,
                    season=ssn,
                    match_ids=args.match_ids,
                    check_only=args.check,
                    from_date=args.from_date,
                    update=args.update,
                    full_refresh=args.full_refresh,
                    scrape_only=args.scrape_only,
                )
