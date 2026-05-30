"""
loaders/serie_a_loader.py
============================
Carga los datos de la Serie A en la base de datos.
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

TM_SERIE_A = PROJECT_ROOT / "data" / "raw" / "transfermarkt" / "serie_a"
WS_SERIE_A = PROJECT_ROOT / "data" / "raw" / "whoscored" / "serie_a"
SS_SERIE_A = PROJECT_ROOT / "data" / "raw" / "sofascore" / "serie_a"
US_SERIE_A = PROJECT_ROOT / "data" / "raw" / "understat" / "serie_a"


def _setup_logging(log_filename: str) -> None:
    """Configura el logging para escribir en consola y en archivo."""
    log_path = PROJECT_ROOT / "logs" / log_filename
    log_path.parent.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)


def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de la Serie A   en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'IT1'"
    )).scalar()


def _load_dimensions(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Serie A — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            with engine.begin() as conn:
                load_teams(conn, ss_path=SS_SERIE_A, tm_path=TM_SERIE_A, ws_path=WS_SERIE_A, us_path=US_SERIE_A)
            log.info("Teams completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, tm_path=TM_SERIE_A, ss_path=SS_SERIE_A, ws_path=WS_SERIE_A, us_path=US_SERIE_A)
            log.info("Players completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, ss_path=SS_SERIE_A, competition_id=competition_id, ws_path=WS_SERIE_A, us_path=US_SERIE_A)
            log.info("Matches completado.")
            log.info("-"*50)


def _load_facts(competition_id: int) -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Serie A — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            with engine.begin() as conn:
                load_shots(conn, ss_path=SS_SERIE_A , us_path=US_SERIE_A, competition_id=competition_id)
            log.info("Shots completado.")
            log.info("-"*50)
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, ss_path=SS_SERIE_A, ws_path=WS_SERIE_A)
            log.info("Events completado.")
            log.info("-"*50)
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, tm_path=TM_SERIE_A)
            log.info("Injuries completado.")
            log.info("-"*50)


def main() -> None:
    # dentro de main poque necesita ejecutarse antes de cualquier operación de logging
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    # configura el logging  para este loader. 
    _setup_logging("serie_a_loader.log")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)

    _load_dimensions(competition_id)
    _load_facts(competition_id)


if __name__ == "__main__":
    main()