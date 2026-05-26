import os
import sys
import argparse
import logging
from pathlib import Path

# Configuración de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Asegurar que la raíz del proyecto esté en el path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def run_load_sequence(competition_name, season):
    """
    Ejecuta la secuencia de carga de datos para una liga y temporada específica.
    Utiliza los loaders canónicos que descubren CSVs automáticamente
    bajo data/clean/<comp>/<season>/<source>/.
    """
    logger.info("=" * 65)
    logger.info(f"   ORQUESTADOR DE CARGA: {competition_name} | {season}")
    logger.info("=" * 65)

    try:
        from wizard.competitions import get_competition
        from loaders.common import engine
        from loaders.team_loader import load_teams
        from loaders.player_loader import load_players
        from loaders.match_loader import load_matches
        from loaders.fact_loader import load_shots, load_events, load_injuries

        # 0. Obtener configuración de la competición
        comp_cfg = get_competition(competition_name)
        if not comp_cfg:
            logger.error(f"No se encontró configuración para la competición: {competition_name}")
            return

        comp_id = comp_cfg.get("canonical_id")
        if not comp_id:
            logger.error(f"La competición {competition_name} no tiene un canonical_id asignado.")
            return

        # 1. CARGA DE DIMENSIONES (Orden Crítico)
        logger.info("── PASO 1: CARGANDO DIMENSIONES ────────────────────────────")

        logger.info("[1/4] Cargando/Enriqueciendo Equipos (dim_team)...")
        with engine.begin() as conn:
            load_teams(conn, comp_name=competition_name)

        logger.info("[2/4] Cargando Jugadores (dim_player)...")
        with engine.begin() as conn:
            load_players(conn, comp_name=competition_name)

        logger.info("[3/4] Cargando Partidos (dim_match)...")
        with engine.begin() as conn:
            load_matches(conn, comp_name=competition_name)

        # 2. CARGA DE HECHOS (Dependen de dim_match y dim_player)
        logger.info("── PASO 2: CARGANDO HECHOS (FACTS) ─────────────────────────")

        logger.info("[4/4] Cargando Tiros, Eventos y Lesiones...")
        with engine.begin() as conn:
            load_shots(conn, comp_name=competition_name)
            load_events(conn, comp_name=competition_name)
            load_injuries(conn, comp_name=competition_name)

        logger.info("=" * 65)
        logger.info("   PROCESO COMPLETADO EXITOSAMENTE")
        logger.info("=" * 65)

    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga datos de una liga/temporada específica en la DB.")
    parser.add_argument("competition", help="Nombre de la competición (ej: 'La Liga')")
    parser.add_argument("season", help="Temporada en formato YYYY/YYYY (ej: '2024/2025')")
    
    args = parser.parse_args()
    run_load_sequence(args.competition, args.season)
