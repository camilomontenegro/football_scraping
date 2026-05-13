"""
loaders/ligue1_loader.py
============================
Carga los datos de la Ligue 1 en la base de datos.
"""
import logging
from pathlib import Path
from sqlalchemy import text
from loaders.common import engine

from loaders.player_loader_generico import load_players
from loaders.team_loader_generico import load_teams
from loaders.match_loader_generico import load_matches
from loaders.fact_loader_generico import load_shots, load_events, load_injuries
# getLogger a nivel de moódulo 
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

TM_LIGUE1 = PROJECT_ROOT / "data" / "raw" / "transfermarkt" / "ligue_1"
WS_LIGUE1 = PROJECT_ROOT / "data" / "raw" / "whoscored" / "ligue-1"
SS_LIGUE1 = PROJECT_ROOT / "data" / "raw" / "sofascore" / "ligue_1"
US_LIGUE1 = PROJECT_ROOT / "data" / "raw" / "understat" / "ligue_1"


def _setup_logging(log_filename: str) -> None:
    """Configura el logging para escribir en consola y en archivo."""
    log_path = PROJECT_ROOT / "logs" / log_filename
    log_path.parent.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)


def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de la Ligue 1 en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'FR1'"
    )).scalar()


def _load_dimensions(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Ligue 1 — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            with engine.begin() as conn:
                load_teams(conn, ss_path=SS_LIGUE1, tm_path=TM_LIGUE1, ws_path=WS_LIGUE1, us_path=US_LIGUE1)
            log.info("Teams completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, tm_path=TM_LIGUE1, ss_path=SS_LIGUE1, ws_path=WS_LIGUE1, us_path=US_LIGUE1)
            log.info("Players completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, ss_path=SS_LIGUE1, competition_id=competition_id, ws_path=WS_LIGUE1, us_path=US_LIGUE1)
            log.info("Matches completado.")
            log.info("-"*50)


def _load_facts(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Ligue 1 — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            with engine.begin() as conn:
                load_shots(conn, ss_path=SS_LIGUE1, us_path=US_LIGUE1, competition_id=competition_id)
            log.info("Shots completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, ss_path=SS_LIGUE1, ws_path=WS_LIGUE1)
            log.info("Events completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, tm_path=TM_LIGUE1)
            log.info("Injuries completado.")
            log.info("-"*50)


def main() -> None:
    # dentro de main poque necesita ejecutarse antes de cualquier operación de logging
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    # configura el logging  para este loader. 
    _setup_logging("ligue1_loader.log")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)

    _load_dimensions(competition_id)
    _load_facts(competition_id)


if __name__ == "__main__":
    main()