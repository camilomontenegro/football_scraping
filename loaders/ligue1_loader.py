"""
loaders/ligue1_loader.py
=========================
Orquestador interactivo para cargar Ligue 1.

Delega en los loaders canónicos pasando `comp_name="Ligue 1"`.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sqlalchemy import text

from loaders.common import engine
from loaders.team_loader   import load_teams
from loaders.player_loader import load_players
from loaders.match_loader  import load_matches
from loaders.fact_loader   import load_shots, load_events, load_injuries

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPETITION_NAME = "Ligue 1"


def _setup_logging(log_filename: str) -> None:
    """Añade un FileHandler para guardar log a `<root>/data/logs/<file>`."""
    from utils.data_paths import LOGS_ROOT
    LOGS_ROOT.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(LOGS_ROOT / log_filename, encoding="utf-8")
    fh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(fh)


def _get_competition_id(conn) -> int:
    return conn.execute(text(
        "SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = 'FR1'"
    )).scalar()


def _load_dimensions() -> None:
    opcion = None
    while opcion != "4":
        print("\n=== Ligue 1 — Dimensiones ===")
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
        print("\n=== Ligue 1 — Hechos ===")
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
    _setup_logging("ligue1_loader.log")
    _load_dimensions()
    _load_facts()


if __name__ == "__main__":
    main()
