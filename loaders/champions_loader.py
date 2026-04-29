"""
loaders/champions_loader.py
============================
Carga los datos de la UEFA Champions League en la base de datos.

La idea es tener un archivo de carga por competicion ( u)
Se mantienn  los loaders de dimensiones y hechos genericos, con metodos genericos que toman el id de la competicion y la ruta 
(player_loader, match_loader, team_loader, etc)

"""
import logging
from pathlib import Path
from sqlalchemy import text
from loaders.common import engine


from loaders.player_loader_generico import load_players
from loaders.team_loader_generico import load_teams
from loaders.match_loader_generico import load_matches
from loaders.fact_loader_generico  import load_shots,load_events, load_injuries



log = logging.getLogger(__name__)

# Para la Champions, no hay datos en understat y statsbomb. 

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TM_CHAMPIONS = PROJECT_ROOT / "data" / "raw" / "transfermarkt" / "champions"
WS_CHAMPIONS = PROJECT_ROOT / "data" / "raw" / "whoscored" / "champions"
SS_CHAMPIONS = PROJECT_ROOT / "data" / "raw" / "sofascore" / "champions"

def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de la Champions League en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'CL'"
    )).scalar()


def _load_dimensions(conn, competition_id: int) -> None:
    """
    Menú para cargar las tablas de dimensiones de la Champions League.
    Debe ejecutarse antes que los hechos.
    """
    opcion = None
    while opcion != "4":
        print("\n=== Champions League — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            load_teams(conn, ss_path=SS_CHAMPIONS, tm_path=TM_CHAMPIONS, ws_path=WS_CHAMPIONS)
            log.info("Teams completado.")
        elif opcion == "2":
            log.info("Cargando players...")
            load_players(conn, tm_path=TM_CHAMPIONS, ss_path=SS_CHAMPIONS, ws_path=WS_CHAMPIONS)
            log.info("Players completado.")
        elif opcion == "3":
            log.info("Cargando matches...")
            load_matches(conn, ss_path=SS_CHAMPIONS, competition_id=competition_id, ws_path=WS_CHAMPIONS)
            log.info("Matches completado.")


def _load_facts(conn, competition_id: int) -> None:
    """
    Menú para cargar las tablas de hechos de la Champions League.
    Requiere que las dimensiones estén cargadas previamente.
    """
    opcion = None
    while opcion != "4":
        print("\n=== Champions League — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            load_shots(conn, ss_path=SS_CHAMPIONS, competition_id=competition_id)
            log.info("Shots completado.")
        elif opcion == "2":
            log.info("Cargando events...")
            load_events(conn, ws_path=WS_CHAMPIONS)
            log.info("Events completado.")
        elif opcion == "3":
            log.info("Cargando injuries...")
            load_injuries(conn, tm_path=TM_CHAMPIONS)
            log.info("Injuries completado.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)
        _load_dimensions(conn, competition_id)
        _load_facts(conn, competition_id)


if __name__ == "__main__":
    main()