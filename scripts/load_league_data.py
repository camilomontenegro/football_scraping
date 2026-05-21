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
    Utiliza los loaders genéricos que permiten parametrización por ruta.
    """
    logger.info("=" * 65)
    logger.info(f"   ORQUESTADOR DE CARGA: {competition_name} | {season}")
    logger.info("=" * 65)

    try:
        from wizard.competitions import get_competition
        from loaders.common import engine
        from loaders.team_loader import load_teams
        from loaders.player_loader_generico import load_players
        from loaders.match_loader_generico import (
            _load_from_sofascore as load_matches_ss,
            _load_from_understat as load_matches_us,
            _load_from_statsbomb as load_matches_sb,
            _load_from_whoscored as load_matches_ws
        )
        from loaders.fact_loader_generico import (
            load_shots,
            load_events,
            load_injuries
        )
        
        # 0. Obtener configuración de la competición
        comp_cfg = get_competition(competition_name)
        if not comp_cfg:
            logger.error(f"No se encontró configuración para la competición: {competition_name}")
            return
        
        comp_id = comp_cfg.get("canonical_id")
        if not comp_id:
            logger.error(f"La competición {competition_name} no tiene un canonical_id asignado.")
            return

        # Definir rutas basadas en la estructura de Osen: data/raw/<fuente>/<slug>/season=YYYY/
        season_folder = f"season={season.split('/')[0]}"
        
        def get_src_path(source):
            src_cfg = comp_cfg.get("sources", {}).get(source, {})
            slug = src_cfg.get("slug") or src_cfg.get("name") or competition_name.lower().replace(" ", "-")
            return PROJECT_ROOT / "data" / "raw" / source / slug / season_folder

        paths = {
            "ss": get_src_path("sofascore"),
            "tm": get_src_path("transfermarkt"),
            "us": get_src_path("understat"),
            "sb": get_src_path("statsbomb"),
            "ws": get_src_path("whoscored")
        }

        # 1. CARGA DE DIMENSIONES (Orden Crítico)
        logger.info("── PASO 1: CARGANDO DIMENSIONES ────────────────────────────")
        
        # Equipos (Global o por archivos encontrados)
        logger.info("[1/4] Cargando/Enriqueciendo Equipos (dim_team)...")
        with engine.begin() as conn:
            load_teams(conn)
        
        # Jugadores (Parametrizado por rutas de la temporada)
        logger.info("[2/4] Cargando Jugadores (dim_player)...")
        with engine.begin() as conn:
            load_players(
                conn, 
                tm_path=paths["tm"] if paths["tm"].exists() else None,
                ss_path=paths["ss"] if paths["ss"].exists() else None,
                us_path=paths["us"] if paths["us"].exists() else None,
                sb_path=paths["sb"] if paths["sb"].exists() else None,
                ws_path=paths["ws"] if paths["ws"].exists() else None
            )
            
        # Partidos (Parametrizado por rutas y competition_id)
        logger.info("[3/4] Cargando Partidos (dim_match)...")
        with engine.begin() as conn:
            if paths["ss"].exists():
                load_matches_ss(conn, paths["ss"], comp_id)
            if paths["us"].exists():
                load_matches_us(conn, paths["us"], comp_id)
            if paths["sb"].exists():
                load_matches_sb(conn, paths["sb"], comp_id)
            if paths["ws"].exists():
                load_matches_ws(conn, paths["ws"], comp_id)
            
        # 2. CARGA DE HECHOS (Dependen de dim_match y dim_player)
        logger.info("── PASO 2: CARGANDO HECHOS (FACTS) ─────────────────────────")
        
        logger.info("[4/4] Cargando Tiros, Eventos e Injurias...")
        with engine.begin() as conn:
            # Tiros
            if paths["ss"].exists():
                load_shots(conn, paths["ss"], comp_id, us_path=paths["us"] if paths["us"].exists() else None)
            
            # Eventos
            load_events(
                conn,
                ss_path=paths["ss"] if paths["ss"].exists() else None,
                sb_path=paths["sb"] if paths["sb"].exists() else None,
                ws_path=paths["ws"] if paths["ws"].exists() else None
            )
            
            # Lesiones
            if paths["tm"].exists():
                load_injuries(conn, paths["tm"])

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
