"""
Smoke tests para loaders — todo en transacciones que ROLLBACK al final.

No modifica la BD. Solo verifica que cada loader se ejecuta de principio a
fin sin lanzar excepciones, y reporta cuantas filas habria insertado/
actualizado de haberse commiteado.

Uso:
    python -m scripts._smoke_loaders                    # todas las working comps
    python -m scripts._smoke_loaders --competition "La Liga"
    python -m scripts._smoke_loaders --skip-facts       # solo dimensiones
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from typing import Callable

from sqlalchemy import text

from loaders.common import engine
from loaders.competition_loader import load_competitions
from loaders.team_loader_generico import load_teams
from loaders.player_loader_generico import load_players
from loaders.match_loader_generico import load_matches
from loaders.fact_loader_generico import load_shots, load_events, load_injuries
from utils.data_paths import clean_dir, normalize_season
from wizard.competitions import WORKING_COMPETITION_NAMES, get_competition

logging.basicConfig(
    level=logging.WARNING,  # bajamos a WARNING para que no se ahogue la salida
    format="[%(asctime)s] %(levelname)s %(name)s — %(message)s",
)


def _competition_id(conn, name: str) -> int | None:
    conf = get_competition(name)
    if not conf:
        return None
    code = conf.get("sources", {}).get("transfermarkt", {}).get("league_code")
    if not code:
        return None
    return conn.execute(
        text("SELECT canonical_id FROM dim_competition WHERE id_transfermarkt = :c"),
        {"c": code},
    ).scalar()


def _delta(conn, table: str, base: int) -> int:
    n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
    return n - base


def _run_in_rollback(label: str, fn: Callable, table_to_count: str | None = None) -> tuple[bool, str]:
    """Corre `fn(conn)` dentro de una conexion con rollback al final.

    Devuelve (ok, mensaje). Si table_to_count se pasa, reporta el delta de
    filas que se habrian agregado si hubiesemos commiteado.
    """
    try:
        with engine.connect() as conn:
            with conn.begin() as outer:
                base = (
                    conn.execute(text(f"SELECT COUNT(*) FROM {table_to_count}")).scalar() or 0
                    if table_to_count
                    else 0
                )
                fn(conn)
                delta = (
                    (conn.execute(text(f"SELECT COUNT(*) FROM {table_to_count}")).scalar() or 0) - base
                    if table_to_count
                    else None
                )
                outer.rollback()
        msg = f"OK  {label}"
        if delta is not None:
            msg += f"  (delta {table_to_count}={delta:+d}, rolled back)"
        return True, msg
    except Exception as e:
        tb = traceback.format_exc(limit=4)
        return False, f"FAIL {label}: {type(e).__name__}: {e}\n{tb}"


def smoke_competitions() -> tuple[bool, str]:
    return _run_in_rollback("load_competitions", load_competitions, "dim_competition")


def smoke_comp(comp_name: str, season: str, skip_facts: bool) -> list[tuple[bool, str]]:
    paths = {
        "ss": clean_dir(comp_name, season, "sofascore"),
        "tm": clean_dir(comp_name, season, "transfermarkt"),
        "ws": clean_dir(comp_name, season, "whoscored"),
        "us": clean_dir(comp_name, season, "understat"),
        "sb": clean_dir(comp_name, season, "statsbomb"),
    }
    # Resolver comp_id usando una conexion read-only.
    with engine.connect() as conn:
        comp_id = _competition_id(conn, comp_name)
    if not comp_id:
        return [(False, f"FAIL {comp_name}: sin competition_id en dim_competition")]

    results: list[tuple[bool, str]] = []

    results.append(_run_in_rollback(
        f"{comp_name} :: load_teams",
        lambda c: load_teams(c, ss_path=paths["ss"], tm_path=paths["tm"],
                             ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"]),
        "dim_team",
    ))
    results.append(_run_in_rollback(
        f"{comp_name} :: load_players",
        lambda c: load_players(c, tm_path=paths["tm"], ss_path=paths["ss"],
                               ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"]),
        "dim_player",
    ))
    results.append(_run_in_rollback(
        f"{comp_name} :: load_matches",
        lambda c: load_matches(c, ss_path=paths["ss"], competition_id=comp_id,
                               ws_path=paths["ws"], us_path=paths["us"], sb_path=paths["sb"]),
        "dim_match",
    ))

    if skip_facts:
        return results

    results.append(_run_in_rollback(
        f"{comp_name} :: load_shots",
        lambda c: load_shots(c, ss_path=paths["ss"], competition_id=comp_id, us_path=paths["us"]),
        "fact_shots",
    ))
    results.append(_run_in_rollback(
        f"{comp_name} :: load_events",
        lambda c: load_events(c, ss_path=paths["ss"], sb_path=paths["sb"], ws_path=paths["ws"]),
        "fact_events",
    ))
    results.append(_run_in_rollback(
        f"{comp_name} :: load_injuries",
        lambda c: load_injuries(c, tm_path=paths["tm"]),
        "fact_injuries",
    ))
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description="Smoke tests para loaders (rollback only)")
    ap.add_argument("--competition", action="append",
                    help="Limita a una competicion (repetible). Default: todas las WORKING.")
    ap.add_argument("--season", default="2025_2026")
    ap.add_argument("--skip-facts", action="store_true", help="Solo dimensiones, no facts")
    ap.add_argument("--skip-competitions", action="store_true",
                    help="No correr load_competitions")
    args = ap.parse_args()

    season = normalize_season(args.season) or args.season
    comps = args.competition or sorted(WORKING_COMPETITION_NAMES)

    all_results: list[tuple[bool, str]] = []

    if not args.skip_competitions:
        all_results.append(smoke_competitions())

    for comp_name in comps:
        all_results.extend(smoke_comp(comp_name, season, args.skip_facts))

    print()
    print("=" * 70)
    print("SMOKE TEST SUMMARY")
    print("=" * 70)
    for ok, msg in all_results:
        print(msg)
    print("-" * 70)
    n_ok = sum(1 for ok, _ in all_results if ok)
    n_fail = len(all_results) - n_ok
    print(f"Total: {len(all_results)}  OK: {n_ok}  FAIL: {n_fail}")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
