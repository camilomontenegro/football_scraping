"""
load_league_data.py
===================
Orquesta la carga completa (dimensiones + facts) para UNA competicion/temporada.

Uso:
    python -m scripts.load_league_data "La Liga" 2025_2026
    python -m scripts.load_league_data "Champions League" 2024/2025
"""

import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_load_sequence(competition_name: str, season: str) -> None:
    logger.info("=" * 65)
    logger.info("   ORQUESTADOR DE CARGA: %s | %s", competition_name, season)
    logger.info("=" * 65)

    try:
        from sqlalchemy import text

        from loaders.common import engine
        from loaders.team_loader_generico import load_teams
        from loaders.player_loader_generico import load_players
        from loaders.match_loader_generico import load_matches
        from loaders.fact_loader_generico import load_shots, load_events, load_injuries
        from utils.data_paths import clean_dir, normalize_season
        from wizard.competitions import get_competition

        comp_cfg = get_competition(competition_name)
        if not comp_cfg:
            logger.error("No se encontro configuracion para la competicion: %s", competition_name)
            return

        league_code = comp_cfg.get("sources", {}).get("transfermarkt", {}).get("league_code")
        if not league_code:
            logger.error("La competicion %s no tiene league_code de Transfermarkt.", competition_name)
            return

        with engine.begin() as conn:
            comp_id = conn.execute(
                text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"),
                {"c": league_code},
            ).scalar()

        if not comp_id:
            logger.error(
                "No hay fila en dim_competition para %s (id_transfermarkt=%s). "
                "Ejecuta antes: python -m scripts.load_dimensions --competitions",
                competition_name, league_code,
            )
            return

        season_path = normalize_season(season) or season
        ss = clean_dir(competition_name, season_path, "sofascore")
        tm = clean_dir(competition_name, season_path, "transfermarkt")
        ws = clean_dir(competition_name, season_path, "whoscored")
        us = clean_dir(competition_name, season_path, "understat")
        sb = clean_dir(competition_name, season_path, "statsbomb")

        # 1. DIMENSIONES (orden critico)
        logger.info("-- PASO 1: DIMENSIONES --")

        logger.info("[1/4] dim_team")
        with engine.begin() as conn:
            load_teams(conn, ss_path=ss, tm_path=tm, ws_path=ws, us_path=us, sb_path=sb)

        logger.info("[2/4] dim_player")
        with engine.begin() as conn:
            load_players(conn, tm_path=tm, ss_path=ss, ws_path=ws, us_path=us, sb_path=sb)

        logger.info("[3/4] dim_match")
        with engine.begin() as conn:
            load_matches(conn, ss_path=ss, competition_id=comp_id, ws_path=ws, us_path=us, sb_path=sb)

        # 2. FACTS
        logger.info("-- PASO 2: FACTS --")
        logger.info("[4/4] shots, events, injuries")

        with engine.begin() as conn:
            load_shots(conn, ss_path=ss, competition_id=comp_id, us_path=us)
        with engine.begin() as conn:
            load_events(conn, ss_path=ss, sb_path=sb, ws_path=ws)
        with engine.begin() as conn:
            load_injuries(conn, tm_path=tm)

        logger.info("=" * 65)
        logger.info("   PROCESO COMPLETADO")
        logger.info("=" * 65)

    except Exception as e:
        logger.error("Error durante la ejecucion: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga datos de una liga/temporada en la DB.")
    parser.add_argument("competition", help="Nombre de la competicion (ej: 'La Liga')")
    parser.add_argument("season", help="Temporada en formato YYYY_YYYY o YYYY/YYYY")

    args = parser.parse_args()
    run_load_sequence(args.competition, args.season)
