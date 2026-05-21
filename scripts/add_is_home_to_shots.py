"""
scripts/add_is_home_to_shots.py
================================
Añade el campo `is_home` a los `shots.csv` de SofaScore leyendo los JSON
crudos por partido. Permite derivar `team_id` en el loader sin que el scraper
guarde `team_id_ss`.

Funciona sobre el layout canónico:

    data/raw/<comp>/<season>/sofascore/matches/<match_id>/shots.json
    data/clean/<comp>/<season>/sofascore/shots.csv     ← se sobreescribe

Uso:
    python scripts/add_is_home_to_shots.py                 # todas las comps
    python scripts/add_is_home_to_shots.py --competition "Champions League"
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import iter_clean_csvs, raw_dir as _raw_dir, slugify_competition

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def _shot_row(match_id: int, shot: dict) -> dict:
    player = shot.get("player", {}) or {}
    coords = shot.get("playerCoordinates", {}) or {}
    return {
        "match_id_ss":  match_id,
        "player_id_ss": player.get("id"),
        "player_name":  player.get("name"),
        "is_home":      shot.get("isHome"),
        "minute":       shot.get("time"),
        "x":            coords.get("x"),
        "y":            coords.get("y"),
        "xg":           shot.get("xg"),
        "result":       shot.get("shotType"),
        "shot_type":    shot.get("bodyPart"),
        "situation":    shot.get("situation"),
        "data_source":  "sofascore",
    }


def process_csv(shots_csv: Path) -> int:
    """Para una `shots.csv`, regenera la columna `is_home` desde los JSON crudos."""
    # Reconstruir la ruta del directorio raw correspondiente.
    # shots_csv = data/clean/<comp>/<season>/sofascore/shots.csv
    parts = shots_csv.parts
    comp_slug   = parts[-4]
    season_lbl  = parts[-3]
    raw_match_dir = _raw_dir(comp_slug, season_lbl, "sofascore") / "matches"

    if not raw_match_dir.exists():
        log.warning("No hay carpeta raw matches/ para %s/%s", comp_slug, season_lbl)
        return 0

    try:
        df_existing = pd.read_csv(shots_csv)
    except Exception as e:
        log.warning("No se pudo leer %s: %s", shots_csv, e)
        return 0

    if "is_home" in df_existing.columns:
        log.info("  %s ya tiene is_home — saltando", shots_csv)
        return 0

    all_shots: list[dict] = []
    for match_dir in sorted(raw_match_dir.iterdir()):
        if not match_dir.is_dir():
            continue
        shots_json = match_dir / "shots.json"
        if not shots_json.exists():
            continue
        try:
            with open(shots_json, encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception as e:
            log.warning("Error leyendo %s: %s", shots_json, e)
            continue

        try:
            match_id = int(match_dir.name)
        except ValueError:
            continue

        for shot in data.get("shotmap", []) or []:
            all_shots.append(_shot_row(match_id, shot))

    if not all_shots:
        log.info("  %s — sin tiros en JSON crudos", shots_csv)
        return 0

    df_new = pd.DataFrame(all_shots)
    for col in ("x", "y", "xg"):
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce").round(4)
    df_new["minute"] = pd.to_numeric(df_new["minute"], errors="coerce")

    df_new.to_csv(shots_csv, index=False, encoding="utf-8-sig")
    log.info("  %s — %d tiros guardados con is_home", shots_csv, len(df_new))
    return len(df_new)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition", default=None,
                        help='Filtrar a una competición (ej. "Champions League"). '
                             "Si se omite, procesa todas las encontradas.")
    args = parser.parse_args()

    files = iter_clean_csvs(competition=args.competition, source="sofascore", filename="shots")
    if not files:
        comp_msg = args.competition or "(todas)"
        log.warning("No se encontró ningún shots.csv para %s", comp_msg)
        return

    log.info("Añadiendo is_home a %d shots.csv de SofaScore...", len(files))
    total = 0
    for f in files:
        total += process_csv(f)
    log.info("Completado — %d tiros procesados en total", total)


if __name__ == "__main__":
    main()
