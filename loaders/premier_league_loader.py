"""
loaders/premier_league_loader.py
=================================
Orquestador interactivo para cargar la Premier League en la base de datos.

Delega en los loaders canónicos pasando `comp_name="Premier League"`, de modo
que sólo se procesan los CSVs bajo
`data/clean/premier_league/<season>/<source>/<table>.csv`.
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from loaders.common import engine
from loaders.team_loader   import load_teams
from loaders.player_loader import load_players
from loaders.match_loader  import load_matches
from loaders.fact_loader   import load_shots, load_events, load_injuries

log = logging.getLogger(__name__)

COMPETITION_NAME = "Premier League"


def _get_competition_id(conn) -> int:
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'GB1'"
    )).scalar()


def _load_dimensions() -> None:
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
                load_teams(conn, comp_name=COMPETITION_NAME)
            log.info("Teams completado.")
        elif opcion == "2":
            log.info("Cargando players...")
            with engine.begin() as conn:
                load_players(conn, comp_name=COMPETITION_NAME)
            log.info("Players completado.")
        elif opcion == "3":
            log.info("Cargando matches...")
            with engine.begin() as conn:
                load_matches(conn, comp_name=COMPETITION_NAME)
            log.info("Matches completado.")


def _load_facts() -> None:
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
                load_shots(conn, comp_name=COMPETITION_NAME)
            log.info("Shots completado.")
        elif opcion == "2":
            log.info("Cargando events...")
            with engine.begin() as conn:
                load_events(conn, comp_name=COMPETITION_NAME)
            log.info("Events completado.")
        elif opcion == "3":
            log.info("Cargando injuries...")
            with engine.begin() as conn:
                load_injuries(conn, comp_name=COMPETITION_NAME)
            log.info("Injuries completado.")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    _load_dimensions()
    _load_facts()


if __name__ == "__main__":
    main()
