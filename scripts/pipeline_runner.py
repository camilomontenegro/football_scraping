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
    python -m scripts.pipeline_runner --source sofascore --season 2020/2021
"""

import argparse
import asyncio
import logging

from loaders.common import engine
from loaders.team_loader import load_teams
from loaders.player_loader import load_players
from loaders.match_loader import load_matches
from loaders.fact_loader import load_shots, load_events, load_injuries

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ── FASE DE SCRAPING ─────────────────────────────────────────────

def run_scraping(source: str = "all", season: str = "2020/2021", match_ids: list = None):
    """Ejecuta el scraper de la fuente indicada.

    Args:
        source:    'all' | 'understat' | 'sofascore' | 'transfermarkt' | 'statsbomb' | 'whoscored'
        season:    Temporada en formato legible (p.ej. '2020/2021')
        match_ids: Lista de IDs de partido para WhoScored (solo necesario si source='whoscored')
    """
    if source in ("all", "understat"):
        logger.info("[START] Scraping Understat...")
        from scrapers.understat_scraper import scrape_laliga, SEASONS
        asyncio.run(scrape_laliga(SEASONS))

    if source in ("all", "sofascore"):
        logger.info("[START] Scraping SofaScore...")
        from scrapers.sofascore_scraper import scrape_sofascore, TOURNAMENT_ID
        scrape_sofascore(season_name=season, tournament_id=TOURNAMENT_ID)

    if source in ("all", "transfermarkt"):
        logger.info("[START] Scraping Transfermarkt...")
        from scrapers.transfermarkt_scraper import scrape_transfermarkt, LEAGUE_CODE, SEASON
        scrape_transfermarkt(league_code=LEAGUE_CODE, season=SEASON)

    if source in ("all", "statsbomb"):
        logger.info("[START] Scraping StatsBomb...")
        from scrapers.statsbomb_scraper import scrape_statsbomb, COMPETITION_ID, SEASON_ID
        scrape_statsbomb(competition_id=COMPETITION_ID, season_id=SEASON_ID)

    if source in ("all", "whoscored"):
        logger.info("[START] Scraping WhoScored...")
        if not match_ids:
            logger.warning("WhoScored requiere --match-ids. Omitiendo.")
        else:
            from scrapers.whoscored_scraper import scrape_whoscored
            scrape_whoscored(match_ids=match_ids)


# ── FASE DE CARGA ────────────────────────────────────────────────

def run_load():
    """Carga todos los datos de data/raw/ en la base de datos."""
    # ── Dimensiones ───────────────────────────────────────────
    logger.info("── CARGANDO DIMENSIONES ─────────────────────────────")
    
    try:
        with engine.begin() as conn:
            load_teams(conn)
    except Exception as e:
        logger.error("Error loading teams: %s", e, exc_info=True)
    
    try:
        with engine.begin() as conn:
            load_players(conn)
    except Exception as e:
        logger.error("Error loading players: %s", e, exc_info=True)
    
    try:
        with engine.begin() as conn:
            load_matches(conn)
    except Exception as e:
        logger.error("Error loading matches: %s", e, exc_info=True)

    # ── Hechos ────────────────────────────────────────────────
    logger.info("── CARGANDO HECHOS (FACTS) ──────────────────────────")
    
    try:
        with engine.begin() as conn:
            load_shots(conn)
    except Exception as e:
        logger.error("Error loading shots: %s", e, exc_info=True)
    
    try:
        with engine.begin() as conn:
            load_events(conn)
    except Exception as e:
        logger.error("Error loading events: %s", e, exc_info=True)
    
    try:
        with engine.begin() as conn:
            load_injuries(conn)
    except Exception as e:
        logger.error("Error loading injuries: %s", e, exc_info=True)


# ── ORCHESTRATOR ─────────────────────────────────────────────────

def run_pipeline(scrape: bool = False, source: str = "all", season: str = "2020/2021", match_ids: list = None):
    logger.info("=================================================================")
    logger.info("   FOOTBALL DATA PIPELINE                        ")
    logger.info("=================================================================")

    try:
        if scrape:
            logger.info("── FASE 1: SCRAPING ─────────────────────────────────")
            try:
                run_scraping(source=source, season=season, match_ids=match_ids)
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
    parser = argparse.ArgumentParser(description="Football Data Pipeline")
    parser.add_argument(
        "--scrape", action="store_true",
        help="Ejecutar fase de scraping antes de cargar (por defecto solo carga)"
    )
    parser.add_argument(
        "--source", default="all",
        choices=["all", "understat", "sofascore", "transfermarkt", "statsbomb", "whoscored"],
        help="Fuente de datos a scrapear (default: all)"
    )
    parser.add_argument(
        "--season", default="2020/2021",
        help="Temporada a scrapear (default: 2020/2021)"
    )
    parser.add_argument(
        "--match-ids", nargs="+", type=int,
        help="IDs de partido para WhoScored"
    )
    args = parser.parse_args()

    run_pipeline(
        scrape=args.scrape,
        source=args.source,
        season=args.season,
        match_ids=args.match_ids,
    )

