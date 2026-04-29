"""
pipeline_runner.py
==================
Orquestador principal del pipeline ETL de fútbol.

Fases:
    1. SCRAPING  — cada scraper extrae y guarda datos en data/raw/<fuente>/
    2. LOAD DIM  — loaders cargan dimensiones en la DB (dim_team, dim_player, dim_match)
    3. LOAD FACT — loaders cargan hechos en la DB (fact_shots, fact_events, fact_injuries)

Scrapers disponibles (todos siguen el mismo patrón que understat_scraper.py):
    - scrapers/understat_scraper.py   -> dim_match, fact_shots
    - scrapers/sofascore_scraper.py   -> dim_match, dim_team, dim_player, fact_shots, fact_events
    - scrapers/transfermarkt_scraper.py -> dim_player (canónico), fact_injuries
    - scrapers/statsbomb_scraper.py   -> dim_match, dim_team, dim_player, fact_events
    - scrapers/whoscored_scraper.py   -> dim_player, fact_events

Uso:
    python -m scripts.pipeline_runner               # Carga solo (asume que data/raw/ ya existe)
    python -m scripts.pipeline_runner --scrape      # Scraping completo + carga
    python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --scrape
    python -m scripts.pipeline_runner --competition "Premier League" --season 2023/2024 --check
"""

import argparse
import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

# Imports lazy para evitar error de DB en --list y --check
from scripts.competitions import (
    COMPETITIONS, 
    get_competition, 
    get_source_config,
    get_season_start_year,
    get_available_seasons,
    list_competitions
)

# Estos imports se cargan lazily en run_load() para evitar error de DB
_loaders_loaded = False
_engine = None
_load_teams = None
_load_players = None
_load_matches = None
_load_shots = None
_load_events = None
_load_injuries = None


def _ensure_loaders():
    """Carga los loaders lazily para evitar error de DB."""
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

# Directorio raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ── VERIFICACIÓN DE DATOS EN BASE DE DATOS ─────────────────────

def check_existing_data(competition: str, season: str, source: str = None) -> dict:
    """Verifica qué datos existen en la base de datos para la competición/temporada.
    
    Args:
        competition: Nombre de la competición (ej: "La Liga")
        season: Temporada en formato "2024/25"
        source: Fuente específica a verificar (None = todas)
    
    Returns:
        Dict con información de qué datos existen en la DB
    """
    from sqlalchemy import text
    from loaders.common import engine
    
    season_start = get_season_start_year(season)
    result = {
        "competition": competition,
        "season": season,
        "season_start_year": season_start,
        "sources": {},
        "has_any_data": False,
    }
    
    # Obtener ID de competición en la DB
    comp_config = get_competition(competition)
    if not comp_config:
        result["error"] = f"Competición '{competition}' no encontrada"
        return result
    
    db_comp_id = comp_config.get("db_id")
    
    sources_to_check = [source] if source else ["understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"]
    
    with engine.connect() as conn:
        for src in sources_to_check:
            try:
                src_config = get_source_config(competition, src)
            except ValueError:
                result["sources"][src] = {"available": False, "error": "Fuente no disponible para esta competición"}
                continue
            
            source_info = {
                "available": True,
                "has_data": False,
                "tables": {},
            }
            
            # Verificar cada tabla según la fuente
            # ARQUITECTURA DE DATOS:
            # - Transfermarkt: Players + Injuries (todas las competiciones)
            # - Sofascore: Matches + Events (base para La Liga, base para otras)
            # - Understat: Shots para La Liga, shots complemento para otras
            # - WhoScored: Events para otras competiciones (con Sofascore base)
            # - StatsBomb: No usado actualmente
            
            # Determinar si es La Liga (caso especial)
            is_la_liga = competition.lower() in ["la liga", "laliga", "liga"]
            
            # El usuario pasa "2024/2025" pero la DB usa formatos diferentes:
            # - fact_injuries: "24/25"
            # - dim_match: "LaLiga 24/25" o "La Liga 24/25"
            # Convertir "2024/2025" -> "24/25"
            # "2024/2025" -> tomar los últimos 2 dígitos de cada año: "24/25"
            parts = season.split("/")
            season_short = parts[0][-2:] + "/" + parts[1][-2:]  # "2024/2025" -> "24/25"
            
            # El competition en la DB puede ser "LaLiga" o "La Liga"
            db_competition = "LaLiga" if is_la_liga else competition
            db_season = f"{db_competition} {season_short}"  # ej: "LaLiga 24/25"
            
            if src == "understat":
                if is_la_liga:
                    # La Liga: shots de understat
                    tables_to_check = {
                        "fact_shots": f"""SELECT COUNT(*) FROM fact_shots f 
                            JOIN dim_match m ON f.match_id = m.match_id 
                            WHERE f.data_source = 'understat' AND m.season = '{db_season}'""",
                    }
                    # Query para última fecha
                    progress_query = f"""SELECT MAX(m.match_date), COUNT(DISTINCT m.match_id)
                        FROM fact_shots f 
                        JOIN dim_match m ON f.match_id = m.match_id 
                        WHERE f.data_source = 'understat' AND m.season = '{db_season}'"""
                else:
                    # Otras: shots como complemento
                    tables_to_check = {
                        "fact_shots": f"""SELECT COUNT(*) FROM fact_shots f 
                            JOIN dim_match m ON f.match_id = m.match_id 
                            WHERE f.data_source = 'understat' AND m.season = '{db_season}'""",
                    }
                    progress_query = f"""SELECT MAX(m.match_date), COUNT(DISTINCT m.match_id)
                        FROM fact_shots f 
                        JOIN dim_match m ON f.match_id = m.match_id 
                        WHERE f.data_source = 'understat' AND m.season = '{db_season}'"""
            elif src == "sofascore":
                # SofaScore: BASE - Matches, Teams, Players
                tables_to_check = {
                    "dim_match": f"SELECT COUNT(*) FROM dim_match WHERE data_source = 'sofascore' AND season = '{db_season}'",
                    "dim_team": "SELECT COUNT(*) FROM dim_team WHERE id_sofascore IS NOT NULL",
                    "dim_player": "SELECT COUNT(*) FROM dim_player WHERE id_sofascore IS NOT NULL",
                    "fact_shots": f"""SELECT COUNT(*) FROM fact_shots f 
                        JOIN dim_match m ON f.match_id = m.match_id 
                        WHERE m.data_source = 'sofascore' AND m.season = '{db_season}'""",
                    "fact_events": f"""SELECT COUNT(*) FROM fact_events e 
                        JOIN dim_match m ON e.match_id = m.match_id 
                        WHERE m.data_source = 'sofascore' AND m.season = '{db_season}'""",
                }
                # Query para última fecha
                progress_query = f"""SELECT MAX(match_date), COUNT(*)
                    FROM dim_match 
                    WHERE data_source = 'sofascore' AND season = '{db_season}'"""
            elif src == "transfermarkt":
                # Transfermarkt: Players + Injuries (universal)
                tables_to_check = {
                    "dim_player": "SELECT COUNT(*) FROM dim_player WHERE id_transfermarkt IS NOT NULL",
                    "fact_injuries": f"SELECT COUNT(*) FROM fact_injuries WHERE season = '{season_short}'",
                }
                # Query para última fecha de injuries
                progress_query = f"""SELECT MAX(date_from), COUNT(*)
                    FROM fact_injuries 
                    WHERE season = '{season_short}'"""
            elif src == "statsbomb":
                # StatsBomb: No usado actualmente
                tables_to_check = {}
                progress_query = None
            elif src == "whoscored":
                if is_la_liga:
                    # La Liga: WhoScored no se usa (events de sofascore)
                    tables_to_check = {}
                    progress_query = None
                else:
                    # Otras: Events (con Sofascore como base para matches)
                    tables_to_check = {
                        "dim_player": "SELECT COUNT(*) FROM dim_player WHERE id_whoscored IS NOT NULL",
                        "fact_events": f"""SELECT COUNT(*) FROM fact_events e 
                            JOIN dim_match m ON e.match_id = m.match_id 
                            WHERE m.data_source = 'whoscored' AND m.season = '{db_season}'""",
                    }
                    # Query para última fecha
                    progress_query = f"""SELECT MAX(m.match_date), COUNT(DISTINCT m.match_id)
                        FROM fact_events e 
                        JOIN dim_match m ON e.match_id = m.match_id 
                        WHERE m.data_source = 'whoscored' AND m.season = '{db_season}'"""
            else:
                tables_to_check = {}
                progress_query = None
            
            total_records = 0
            for table, query in tables_to_check.items():
                try:
                    row = conn.execute(text(query)).fetchone()
                    count = row[0] if row else 0
                    source_info["tables"][table] = count
                    total_records += count
                except Exception as e:
                    source_info["tables"][table] = f"Error: {str(e)[:30]}"
            
            # Ejecutar query de progreso (última fecha)
            if progress_query:
                try:
                    row = conn.execute(text(progress_query)).fetchone()
                    if row and row[0]:
                        source_info["last_date"] = str(row[0])
                        source_info["match_count"] = row[1] if len(row) > 1 else None
                except Exception as e:
                    source_info["progress_error"] = str(e)[:50]
            
            source_info["has_data"] = total_records > 0
            if total_records > 0:
                result["has_any_data"] = True
            
            result["sources"][src] = source_info
    
    return result


def print_data_check(check_result: dict):
    """Imprime el resultado de la verificación de datos desde la DB."""
    print("\n" + "=" * 60)
    print(f"VERIFICACION DE DATOS EN BASE DE DATOS")
    print(f"   Competicion: {check_result['competition']}")
    print(f"   Temporada: {check_result['season']}")
    print("=" * 60)
    
    if check_result.get("error"):
        print(f"\n[ERROR] {check_result['error']}")
        print("\n" + "=" * 60)
        return
    
    for source, info in check_result["sources"].items():
        status = "[OK]" if info.get("has_data") else "[EMPTY]"
        available = "[*]" if info.get("available") else "[-]"
        
        print(f"\n{available} {source.upper()}:")
        if not info.get("available"):
            print(f"   {info.get('error', 'No disponible')}")
            continue
        
        print(f"   {status} {'Tiene datos' if info.get('has_data') else 'Sin datos'}")
        
        # Mostrar ultima fecha de datos (progreso)
        if info.get("last_date"):
            match_count = info.get("match_count")
            if match_count:
                print(f"      -> Hasta: {info['last_date']} ({match_count:,} partidos)")
            else:
                print(f"      -> Hasta: {info['last_date']}")
        
        # Mostrar tablas y registros
        tables = info.get("tables", {})
        if tables:
            for table, count in tables.items():
                if isinstance(count, int):
                    print(f"      {table}: {count:,} registros")
                else:
                    print(f"      {table}: {count}")
    
    print("\n" + "=" * 60)


def list_available_competitions():
    """Lista todas las competiciones disponibles con sus fuentes."""
    print("\n" + "=" * 60)
    print("COMPETICIONES DISPONIBLES")
    print("=" * 60)
    
    for comp in list_competitions():
        sources = []
        if comp.get("has_transfermarkt"): sources.append("TM")
        if comp.get("has_sofascore"): sources.append("SF")
        if comp.get("has_understat"): sources.append("US")
        if comp.get("has_statsbomb"): sources.append("SB")
        
        print(f"\n  {comp['name']} ({comp['country']})")
        print(f"    Fuentes: {', '.join(sources) if sources else 'Ninguna'}")
    
    print("\n" + "=" * 60)


# ── FASE DE SCRAPING ─────────────────────────────────────────────

def run_scraping(
    competition: str = None,
    source: str = "all", 
    season: str = "2024/2025", 
    match_ids: list = None
):
    """Ejecuta el scraper de la fuente indicada.

    Args:
        competition: Nombre de la competición (ej: "La Liga"). Si es None, usa valores por defecto.
        source:      'all' | 'understat' | 'sofascore' | 'transfermarkt' | 'statsbomb' | 'whoscored'
        season:      Temporada en formato legible (p.ej. '2024/2025')
        match_ids:   Lista de IDs de partido para WhoScored (solo necesario si source='whoscored')
    """
    # Obtener configuración de la competición si se especifica
    comp_config = None
    if competition:
        comp_config = get_competition(competition)
        if not comp_config:
            logger.error(f"Competición '{competition}' no encontrada")
            return
    
    season_start = get_season_start_year(season)
    
    # Understat
    if source in ("all", "understat"):
        logger.info("[START] Scraping Understat...")
        from scrapers.understat_scraper import scrape_laliga, SEASONS_DEFAULT
        
        league_code = None
        if comp_config:
            understat_config = comp_config["sources"].get("understat", {})
            league_code = understat_config.get("league")
        
        asyncio.run(scrape_laliga([season_start], league=league_code))

    # SofaScore
    if source in ("all", "sofascore"):
        logger.info("[START] Scraping SofaScore...")
        from scrapers.sofascore_scraper import scrape_sofascore
        
        tournament_id = None
        if comp_config:
            sofascore_config = comp_config["sources"].get("sofascore", {})
            tournament_id = sofascore_config.get("tournament_id")
        
        scrape_sofascore(season_name=season, tournament_id=tournament_id)

    # Transfermarkt
    if source in ("all", "transfermarkt"):
        logger.info("[START] Scraping Transfermarkt...")
        from scrapers.transfermarkt_scraper import scrape_transfermarkt
        
        league_code = None
        if comp_config:
            tm_config = comp_config["sources"].get("transfermarkt", {})
            league_code = tm_config.get("league_code")
        
        scrape_transfermarkt(league_code=league_code, season=season_start)

    # StatsBomb
    if source in ("all", "statsbomb"):
        logger.info("[START] Scraping StatsBomb...")
        from scrapers.statsbomb_scraper import scrape_statsbomb
        
        competition_id = None
        if comp_config:
            sb_config = comp_config["sources"].get("statsbomb", {})
            competition_id = sb_config.get("competition_id")
        
        # StatsBomb usa season_ids diferentes, necesitamos mapeo
        # Por ahora usamos el año como referencia
        scrape_statsbomb(competition_id=competition_id, season_id=season_start)

    # WhoScored
    if source in ("all", "whoscored"):
        logger.info("[START] Scraping WhoScored...")
        if not match_ids:
            logger.warning("WhoScored requiere --match-ids. Omitiendo.")
        else:
            from scrapers.whoscored_scraper import scrape_whoscored
            
            region_id = None
            tournament_id = None
            if comp_config:
                ws_config = comp_config["sources"].get("whoscored", {})
                region_id = ws_config.get("region_id")
                tournament_id = ws_config.get("tournament_id")
            
            scrape_whoscored(match_ids=match_ids)


# ── FASE DE CARGA ────────────────────────────────────────────────

def run_load():
    """Carga todos los datos de data/raw/ en la base de datos."""
    # Cargar los loaders lazily
    _ensure_loaders()
    
    # ── Dimensiones ───────────────────────────────────────────
    logger.info("── CARGANDO DIMENSIONES ─────────────────────────────")
    
    try:
        with _engine.begin() as conn:
            _load_teams(conn)
    except Exception as e:
        logger.error("Error loading teams: %s", e, exc_info=True)
    
    try:
        with _engine.begin() as conn:
            _load_players(conn)
    except Exception as e:
        logger.error("Error loading players: %s", e, exc_info=True)
    
    try:
        with _engine.begin() as conn:
            _load_matches(conn)
    except Exception as e:
        logger.error("Error loading matches: %s", e, exc_info=True)

    # ── Hechos ────────────────────────────────────────────────
    logger.info("── CARGANDO HECHOS (FACTS) ──────────────────────────")
    
    try:
        with _engine.begin() as conn:
            _load_shots(conn)
    except Exception as e:
        logger.error("Error loading shots: %s", e, exc_info=True)
    
    try:
        with _engine.begin() as conn:
            _load_events(conn)
    except Exception as e:
        logger.error("Error loading events: %s", e, exc_info=True)
    
    try:
        with _engine.begin() as conn:
            _load_injuries(conn)
    except Exception as e:
        logger.error("Error loading injuries: %s", e, exc_info=True)


# ── ORCHESTRATOR ─────────────────────────────────────────────────

def run_pipeline(
    scrape: bool = False, 
    competition: str = None,
    source: str = "all", 
    season: str = "2024/2025", 
    match_ids: list = None,
    check_only: bool = False
):
    logger.info("=================================================================")
    logger.info("   FOOTBALL DATA PIPELINE                        ")
    logger.info("=================================================================")
    
    if competition:
        logger.info(f"   Competición: {competition}")
    logger.info(f"   Temporada: {season}")
    logger.info(f"   Fuente: {source}")

    try:
        # Fase de verificación
        if check_only:
            logger.info("── FASE 0: VERIFICACIÓN ────────────────────────────")
            check_result = check_existing_data(competition, season, source if source != "all" else None)
            print_data_check(check_result)
            return

        if scrape:
            logger.info("── FASE 1: SCRAPING ─────────────────────────────────")
            try:
                run_scraping(competition=competition, source=source, season=season, match_ids=match_ids)
            except Exception as e:
                logger.error("Fatal error during scraping phase: %s", e, exc_info=True)
                raise SystemExit(1)

        logger.info("── FASE 2/3: CARGA EN DB ────────────────────────────")
        try:
            run_load()
        except Exception as e:
            logger.error("Fatal error during load phase: %s", e, exc_info=True)
            raise SystemExit(1)

        logger.info("=================================================================")
        logger.info("   PIPELINE COMPLETADO EXITOSAMENTE             ")
        logger.info("=================================================================")

    except SystemExit:
        raise
    except Exception as e:
        logger.error("Unexpected error in pipeline: %s", e, exc_info=True)
        raise SystemExit(1)


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
  
  # Scraping de una competición específica
  python -m scripts.pipeline_runner --competition "Premier League" --season 2024/2025 --scrape
  
  # Scraping de una sola fuente
  python -m scripts.pipeline_runner --competition "La Liga" --season 2024/2025 --source understat --scrape
        """
    )
    parser.add_argument(
        "--scrape", action="store_true",
        help="Ejecutar fase de scraping antes de cargar (por defecto solo carga)"
    )
    parser.add_argument(
        "--competition", "-c", type=str, default=None,
        help="Nombre de la competición (ej: 'La Liga', 'Premier League'). Use --list para ver disponibles."
    )
    parser.add_argument(
        "--source", "-s", default="all",
        choices=["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"],
        help="Fuente de datos a scrapear (default: all)"
    )
    parser.add_argument(
        "--season", "-t", default="2024/2025",
        help="Temporada a scrapear (default: 2024/2025). Formato: 2024/2025"
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int,
        help="IDs de partido para WhoScored"
    )
    parser.add_argument(
        "--check", action="store_true",
        help="Solo verificar qué datos existen para la competición/temporada"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Listar todas las competiciones disponibles"
    )
    args = parser.parse_args()

    # Listar competiciones
    if args.list:
        list_available_competitions()
    else:
        run_pipeline(
            scrape=args.scrape,
            competition=args.competition,
            source=args.source,
            season=args.season,
            match_ids=args.match_ids,
            check_only=args.check,
        )

