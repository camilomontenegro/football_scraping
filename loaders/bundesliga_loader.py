"""
loaders/bundesliga_loader.py
============================
Carga los datos de la Bundesliga en la base de datos.
"""
import logging
from pathlib import Path
from sqlalchemy import text
from loaders.common import engine
from utils.data_paths import clean_dir

from loaders.player_loader_generico import load_players
from loaders.team_loader_generico import load_teams
from loaders.match_loader_generico import load_matches
from loaders.fact_loader_generico import load_shots, load_events, load_injuries
# getLogger a nivel de moódulo
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

_COMP = "Bundesliga"
_SEASON = "2025_2026"
TM_BUNDESLIGA = clean_dir(_COMP, _SEASON, "transfermarkt")
WS_BUNDESLIGA = clean_dir(_COMP, _SEASON, "whoscored")
SS_BUNDESLIGA = clean_dir(_COMP, _SEASON, "sofascore")
US_BUNDESLIGA = clean_dir(_COMP, _SEASON, "understat")


def _setup_logging(log_filename: str) -> None:
    """Configura el logging para escribir en consola y en archivo."""
    log_path = PROJECT_ROOT / "logs" / log_filename
    log_path.parent.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)


def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de la Bundesliga en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'L1'"
    )).scalar()


def _load_dimensions(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Bundesliga — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            with engine.begin() as conn:
                load_teams(conn, ss_path=SS_BUNDESLIGA, tm_path=TM_BUNDESLIGA, ws_path=WS_BUNDESLIGA, us_path=US_BUNDESLIGA)
            log.info("Teams completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, tm_path=TM_BUNDESLIGA, ss_path=SS_BUNDESLIGA, ws_path=WS_BUNDESLIGA, us_path=US_BUNDESLIGA)
            log.info("Players completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, ss_path=SS_BUNDESLIGA, competition_id=competition_id, ws_path=WS_BUNDESLIGA, us_path=US_BUNDESLIGA)
            log.info("Matches completado.")
            log.info("-"*50)


def _load_facts(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Bundesliga — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            with engine.begin() as conn:
                load_shots(conn, ss_path=SS_BUNDESLIGA, us_path=US_BUNDESLIGA, competition_id=competition_id)
            log.info("Shots completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, ss_path=SS_BUNDESLIGA, ws_path=WS_BUNDESLIGA)
            log.info("Events completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, tm_path=TM_BUNDESLIGA)
            log.info("Injuries completado.")
            log.info("-"*50)


def main() -> None:
    # dentro de main poque necesita ejecutarse antes de cualquier operación de logging
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    # configura el logging  para este loader. 
    _setup_logging("bundesliga_loader.log")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)

    _load_dimensions(competition_id)
    _load_facts(competition_id)


if __name__ == "__main__":
    main()