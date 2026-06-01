"""
load_dimensions.py
==================
Cargar dimensiones (dim_competition, dim_player, dim_team, dim_match) iterando
sobre las competiciones soportadas (WORKING_COMPETITION_NAMES) para la temporada
indicada por --season (por defecto la actual segun get_current_season()).

Uso:
    python -m scripts.load_dimensions --competitions   # Cargar solo competiciones
    python -m scripts.load_dimensions --teams          # Cargar solo equipos
    python -m scripts.load_dimensions --players        # Cargar solo jugadores
    python -m scripts.load_dimensions --matches        # Cargar solo partidos
    python -m scripts.load_dimensions --all            # Cargar todos
    python -m scripts.load_dimensions                  # Sin args = --all
    python -m scripts.load_dimensions --season 2024_2025
"""

import argparse
import logging
import sys

from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

from loaders.common import engine
from loaders.competition_loader import load_competitions
from loaders.team_loader_generico import load_teams
from loaders.player_loader_generico import load_players
from loaders.match_loader_generico import load_matches
from utils.data_paths import clean_dir, normalize_season
from wizard.competitions import (
    WORKING_COMPETITION_NAMES,
    get_competition,
)


def _competition_id(conn, comp_name: str):
    """Resuelve el canonical_id de dim_competition a partir del league_code TM."""
    comp = get_competition(comp_name)
    if not comp:
        return None
    code = comp.get("sources", {}).get("transfermarkt", {}).get("league_code")
    if not code:
        return None
    return conn.execute(
        text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"),
        {"c": code},
    ).scalar()


def _paths(comp_name: str, season: str) -> dict:
    return {
        "ss": clean_dir(comp_name, season, "sofascore"),
        "tm": clean_dir(comp_name, season, "transfermarkt"),
        "ws": clean_dir(comp_name, season, "whoscored"),
        "us": clean_dir(comp_name, season, "understat"),
        "sb": clean_dir(comp_name, season, "statsbomb"),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Cargar dimensiones individuales en la base de datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--competitions", action="store_true", help="Cargar dim_competition")
    parser.add_argument("--teams", action="store_true", help="Cargar dim_team")
    parser.add_argument("--players", action="store_true", help="Cargar dim_player")
    parser.add_argument("--matches", action="store_true", help="Cargar dim_match")
    parser.add_argument("--all", action="store_true", help="Cargar todo")
    parser.add_argument("--season", default="2025_2026", help="Temporada (YYYY_YYYY)")
    parser.add_argument(
        "--competition",
        action="append",
        help="Limitar a una competicion (repetible). Por defecto: todas las WORKING.",
    )

    args = parser.parse_args()

    if not any([args.competitions, args.teams, args.players, args.matches, args.all]):
        args.all = True

    season = normalize_season(args.season) or args.season
    comps = args.competition or sorted(WORKING_COMPETITION_NAMES)

    try:
        # dim_competition es global; se carga una sola vez.
        if args.all or args.competitions:
            print("\n" + "=" * 60)
            print("[+] Cargando DIM_COMPETITION")
            print("=" * 60)
            with engine.begin() as conn:
                load_competitions(conn)
            print("[OK] dim_competition cargado exitosamente")

        for comp_name in comps:
            paths = _paths(comp_name, season)
            print("\n" + "=" * 60)
            print(f"[>] {comp_name} ({season})")
            print("=" * 60)

            with engine.begin() as conn:
                comp_id = _competition_id(conn, comp_name)

            if (args.all or args.matches) and not comp_id:
                log.warning(
                    "Saltando %s: no se encontro competition_id en dim_competition. "
                    "Ejecuta primero --competitions.",
                    comp_name,
                )
                continue

            if args.all or args.teams:
                log.info("  > dim_team")
                with engine.begin() as conn:
                    load_teams(
                        conn,
                        ss_path=paths["ss"],
                        tm_path=paths["tm"],
                        ws_path=paths["ws"],
                        us_path=paths["us"],
                        sb_path=paths["sb"],
                    )

            if args.all or args.players:
                log.info("  > dim_player")
                with engine.begin() as conn:
                    load_players(
                        conn,
                        tm_path=paths["tm"],
                        ss_path=paths["ss"],
                        ws_path=paths["ws"],
                        us_path=paths["us"],
                        sb_path=paths["sb"],
                    )

            if args.all or args.matches:
                log.info("  > dim_match")
                with engine.begin() as conn:
                    load_matches(
                        conn,
                        ss_path=paths["ss"],
                        competition_id=comp_id,
                        ws_path=paths["ws"],
                        us_path=paths["us"],
                        sb_path=paths["sb"],
                    )

        print("\n" + "=" * 60)
        print("[OK] DIMENSIONES CARGADAS")
        print("=" * 60)
        print("\nProximo paso:")
        print("  python -m scripts.load_facts --all")

    except Exception as e:
        log.error("[FATAL] El proceso de carga fallo: %s", e, exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
