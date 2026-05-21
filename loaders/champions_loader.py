"""
loaders/champions_loader.py
============================
Orquestador interactivo para cargar la UEFA Champions League.

Delega en los loaders canónicos pasando `comp_name="Champions League"`, de modo
que sólo se procesan los CSVs bajo
`data/clean/champions_league/<season>/<source>/<table>.csv`.

Nota: en Champions no hay datos de Understat ni StatsBomb. Los loaders
canónicos lo manejan de forma transparente (simplemente no encuentran CSV).
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

COMPETITION_NAME = "Champions League"


def _get_competition_id(conn) -> int:
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'CL'"
    )).scalar()


def _load_dimensions() -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Champions League — Dimensiones ===")
        print("1. Teams")
        print("2. Players")
        print("3. Matches")
        print("4. Continuar a hechos")
        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            with engine.begin() as conn:
                load_teams(conn, comp_name=COMPETITION_NAME)
        elif opcion == "2":
            with engine.begin() as conn:
                load_players(conn, comp_name=COMPETITION_NAME)
        elif opcion == "3":
            with engine.begin() as conn:
                load_matches(conn, comp_name=COMPETITION_NAME)


def _load_facts() -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Champions League — Hechos ===")
        print("1. Shots")
        print("2. Events")
        print("3. Injuries")
        print("4. Salir")
        opcion = input("Selecciona (1-4): ").strip()

        if opcion == "1":
            with engine.begin() as conn:
                load_shots(conn, comp_name=COMPETITION_NAME)
        elif opcion == "2":
            with engine.begin() as conn:
                load_events(conn, comp_name=COMPETITION_NAME)
        elif opcion == "3":
            with engine.begin() as conn:
                load_injuries(conn, comp_name=COMPETITION_NAME)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    _load_dimensions()
    _load_facts()


if __name__ == "__main__":
    main()
