"""
loaders/laliga_loader.py
=========================
Carga los datos de La Liga en la base de datos.
Utiliza los loaders genericos, en lso que lso metodos  toman la ruta como paramentro,  para cargar los datos de las tablas 
desde la rutas  especificas para los datos de la liga. 

Regumen de  fuentes  maestras 
DIMENSIONES 
dim_player -> transfermarkt. Del resto de fuentes se introducen los id de los jugadores en las respectivas fuentes.
dim_team -> sofascore. Tranfermarkt  secundaria. Resto de fuentes cargan ids del equipo en la fuente.
dim_matches -> sofascore. 

HECHOS
fact_events -> whoscored principal. sofascored secundaria
fact_shots ->  understat. para sofascore falla la inserccion. 
fact_injuries -> transfermakrt fuente única



"""


import logging
from pathlib import Path
from sqlalchemy import text
from loaders.common import engine

from loaders.player_loader_generico import load_players
from loaders.team_loader_generico import load_teams
from loaders.match_loader_generico import load_matches
from loaders.fact_loader_generico import load_shots, load_events, load_injuries

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TM_LALIGA = PROJECT_ROOT / "data" / "raw" / "transfermarkt" / "la_liga"
SS_LALIGA = PROJECT_ROOT / "data" / "raw" / "sofascore" / "la_liga"
WS_LALIGA = PROJECT_ROOT / "data" / "raw" / "whoscored" / "la_liga"
US_LALIGA = PROJECT_ROOT / "data" / "raw" / "understat" / "la_liga"
SB_LALIGA = PROJECT_ROOT / "data" / "raw" / "statsbomb" / "la_liga"


def _get_competition_id(conn) -> int:
    """Obtiene el canonical_id de La Liga en dim_competition."""
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'ES1'"
    )).scalar()


def _load_dimensions( competition_id: int) -> None:
    """
    
    Menú para cargar dimensiones de La Liga.
    Cada operación abre su propia conexión — hace commit al terminar
    y rollback automático si hay error. Así si se interrumpe el proceso
    lo ya cargado queda guardado.
    Debe ejecutarse antes que los hechos.
    """
    opcion = None
    while opcion != "4":
        print("\n=== La Liga — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando teams...")
            with engine.begin() as conn:
                load_teams(conn, ss_path=SS_LALIGA, tm_path=TM_LALIGA, ws_path=WS_LALIGA, us_path=US_LALIGA, sb_path=SB_LALIGA)
            log.info("Teams completado.")
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, tm_path=TM_LALIGA, ss_path=SS_LALIGA, ws_path=WS_LALIGA, us_path=US_LALIGA, sb_path=SB_LALIGA)
            log.info("Players completado.")
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, ss_path=SS_LALIGA, competition_id=competition_id, ws_path=WS_LALIGA, us_path=US_LALIGA, sb_path=SB_LALIGA)
            log.info("Matches completado.")



def _load_facts(competition_id: int) -> None:
    """
    Menú para cargar las tablas de hechos de La Liga.
    Cada operación abre su propia conexión — hace commit al terminar
    y rollback automático si hay error. Así si se interrumpe el proceso
    lo ya cargado queda guardado.
    Requiere que las dimensiones estén cargadas previamente.
    """
    opcion = None
    while opcion != "4":
        print("\n=== La Liga — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")

        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            log.info("Cargando shots...")
            with engine.begin() as conn:
                load_shots(conn, ss_path=SS_LALIGA, competition_id=competition_id, us_path=US_LALIGA)
            log.info("Shots completado.")
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, ss_path=SS_LALIGA, ws_path=WS_LALIGA, sb_path=SB_LALIGA)
            log.info("Events completado.")
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, tm_path=TM_LALIGA)
            log.info("Injuries completado.")

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    with engine.begin() as conn:
        competition_id = _get_competition_id(conn)

    _load_dimensions(competition_id)
    _load_facts(competition_id)

if __name__ == "__main__":
    main()