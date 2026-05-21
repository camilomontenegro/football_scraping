"""
loaders/premier_league_loader.py
============================
Carga los datos de la Premier  en la base de datos.

La idea es tener un archivo de carga por competicion ( u)
Se mantienen los loaders de dimensiones y hechos genericos, con metodos genericos que toman el id de la competicion y la ruta (player_loader_generico, match_loader_generico, team_loader_generico, etc)

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

TM_PREMIER = PROJECT_ROOT / "data" / "raw" / "transfermarkt" / "premier_league"
WS_PREMIER = PROJECT_ROOT / "data" / "raw" / "whoscored" / "premier_league"
SS_PREMIER= PROJECT_ROOT / "data" / "raw" / "sofascore" / "premier_league"
US_PREMIER= PROJECT_ROOT / "data" / "raw" / "understat" / "premier_league"

def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de la Premier League en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'GB1'"
    )).scalar()


def _load_dimensions(competition_id: int) -> None:
    """
    Menú para cargar las tablas de dimensiones de la Premier League.
    Debe ejecutarse antes que los hechos.
    Cada operación abre su propia conexión y hace commit al terminar.
    """
    opcion = None
    while opcion != "4":
        print("\n=== Premier League — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            with engine.begin() as conn:
                load_teams(conn, ss_path=SS_PREMIER, tm_path=TM_PREMIER, ws_path=WS_PREMIER, us_path=US_PREMIER)
            log.info("Teams completado.")
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, tm_path=TM_PREMIER, ss_path=SS_PREMIER, ws_path=WS_PREMIER, us_path=US_PREMIER)
            log.info("Players completado.")
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, ss_path=SS_PREMIER, competition_id=competition_id, ws_path=WS_PREMIER, us_path=US_PREMIER)
            log.info("Matches completado.")


def _load_facts(competition_id: int) -> None:
    """
    Menú para cargar las tablas de hechos de la Premier League
    Requiere que las dimensiones estén cargadas previamente.
    Cada operación abre su propia conexión y hace commit al terminar.
    """
    opcion = None
    while opcion != "4":
        print("\n=== Premier League — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            with engine.begin() as conn:
                load_shots(conn, ss_path=SS_PREMIER, us_path=US_PREMIER,competition_id=competition_id)
            log.info("Shots completado.")
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, ws_path=WS_PREMIER,ss_path=SS_PREMIER)
            log.info("Events completado.")
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, tm_path=TM_PREMIER)
            log.info("Injuries completado.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)

    _load_dimensions(competition_id)
    _load_facts(competition_id)


if __name__ == "__main__":
    main()