"""
load_facts.py
=============
Cargar facts (fact_shots, fact_events, fact_injuries) iterando sobre las
competiciones soportadas (WORKING_COMPETITION_NAMES) para la temporada
indicada por --season (por defecto 2025_2026).

Uso:
    python -m scripts.load_facts --shots
    python -m scripts.load_facts --events
    python -m scripts.load_facts --injuries
    python -m scripts.load_facts --all
    python -m scripts.load_facts                     # = --all
    python -m scripts.load_facts --competition "La Liga" --shots
    python -m scripts.load_facts --events --all-seasons
"""

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

from loaders.common import engine
from loaders.fact_loader_generico import load_shots, load_events, load_injuries
from utils.data_paths import CLEAN_ROOT, clean_dir, normalize_season, slugify_competition
from wizard.competitions import (
    WORKING_COMPETITION_NAMES,
    get_competition,
)


def _discover_comp_seasons(comp_names: list[str]) -> list[tuple[str, str]]:
    """Pares (competición, temporada) con al menos un events.csv en clean/."""
    pairs: list[tuple[str, str]] = []
    for comp_name in comp_names:
        slug = slugify_competition(comp_name)
        base = CLEAN_ROOT / slug
        if not base.is_dir():
            continue
        for season_dir in sorted(base.iterdir()):
            if not season_dir.is_dir():
                continue
            has_events = any(
                (season_dir / src / "events.csv").exists()
                for src in ("whoscored", "sofascore", "statsbomb")
            )
            if has_events:
                pairs.append((comp_name, season_dir.name))
    return pairs


def _competition_id(conn, comp_name: str):
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
        description="Cargar facts individuales en la base de datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--shots", action="store_true")
    parser.add_argument("--events", action="store_true")
    parser.add_argument("--injuries", action="store_true")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--season", default="2025_2026")
    parser.add_argument("--all-seasons", action="store_true",
                        help="Iterar todas las temporadas con events.csv en data/clean/")
    parser.add_argument("--competition", action="append")

    args = parser.parse_args()

    if not any([args.shots, args.events, args.injuries, args.all]):
        args.all = True

    season = normalize_season(args.season) or args.season
    comps = args.competition or sorted(WORKING_COMPETITION_NAMES)

    if args.all_seasons:
        comp_seasons = _discover_comp_seasons(comps)
    else:
        comp_seasons = [(c, season) for c in comps]

    any_failure = False

    for comp_name, season_label in comp_seasons:
        paths = _paths(comp_name, season_label)
        print("\n" + "=" * 60)
        print(f"[>] {comp_name} ({season_label})")
        print("=" * 60)

        with engine.begin() as conn:
            comp_id = _competition_id(conn, comp_name)

        if not comp_id:
            log.warning("Saltando %s: no se encontro competition_id en dim_competition.", comp_name)
            continue

        # Cada fact en su propia transaccion: un fallo en uno no aborta los demas.
        if args.all or args.shots:
            try:
                with engine.begin() as conn:
                    load_shots(conn, ss_path=paths["ss"], competition_id=comp_id, us_path=paths["us"])
            except Exception as e:
                any_failure = True
                log.error("[ERROR] shots de %s %s: %s", comp_name, season_label, e, exc_info=True)

        if args.all or args.events:
            try:
                with engine.begin() as conn:
                    load_events(conn, ss_path=paths["ss"], sb_path=paths["sb"], ws_path=paths["ws"])
            except Exception as e:
                any_failure = True
                log.error("[ERROR] events de %s %s: %s", comp_name, season_label, e, exc_info=True)

        if args.all or args.injuries:
            try:
                with engine.begin() as conn:
                    load_injuries(conn, tm_path=paths["tm"])
            except Exception as e:
                any_failure = True
                log.error("[ERROR] injuries de %s %s: %s", comp_name, season_label, e, exc_info=True)

    print("\n" + "=" * 60)
    if any_failure:
        print("[!] FACTS CARGADOS CON ERRORES - revisa el log")
    else:
        print("[OK] FACTS CARGADOS EXITOSAMENTE")
    print("=" * 60)

    return 1 if any_failure else 0


if __name__ == "__main__":
    sys.exit(main())
