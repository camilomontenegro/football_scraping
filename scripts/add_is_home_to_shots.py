"""
scripts/add_is_home_to_shots.py
================================
Script desechable que lee los JSON crudos de tiros de SofaScore Champions
y regenera los shots_clean.csv añadiendo el campo is_home.

El campo is_home permite derivar el team_id en el loader sin necesidad
de que el scraper guarde team_id_ss (que no viene en el JSON de SofaScore).

    is_home = True  → el jugador pertenece al equipo local  (home_team_id)
    is_home = False → el jugador pertenece al equipo visitante (away_team_id)

Uso:
    python scripts/add_is_home_to_shots.py

Salida:
    Sobreescribe cada shots_clean.csv con el campo is_home añadido.
"""

import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

# ruta base de los datos de SofaScore Champions
SS_CHAMPIONS = Path("data/raw/sofascore/champions")


def process_season(season_dir: Path) -> int:
    """
    Procesa una temporada — lee los JSON crudos de shots y regenera shots_clean.csv
    con el campo is_home añadido.

    Devuelve el número de tiros procesados.
    """
    raw_dir = season_dir / "raw"
    if not raw_dir.exists():
        log.warning("No hay directorio raw en %s", season_dir)
        return 0

    shots_clean_path = season_dir / "shots_clean.csv"
    if not shots_clean_path.exists():
        log.warning("No hay shots_clean.csv en %s", season_dir)
        return 0

    # leer el CSV existente
    df_existing = pd.read_csv(shots_clean_path)

    # si ya tiene is_home no hace falta procesar
    if "is_home" in df_existing.columns:
        log.info("  %s ya tiene is_home — saltando", season_dir.name)
        return 0

    all_shots: list[dict] = []

    # recorre cada carpeta de partido dentro de raw/
    for match_dir in sorted(raw_dir.iterdir()):
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

        # extraer el match_id del nombre de la carpeta (match_<id>)
        match_id = int(match_dir.name.replace("match_", ""))

        for shot in data.get("shotmap", []):
            player = shot.get("player", {})
            all_shots.append({
                "match_id_ss":  match_id,
                "player_id_ss": player.get("id"),
                "player_name":  player.get("name"),
                "is_home":      shot.get("isHome"),       # ← campo nuevo
                "minute":       shot.get("time"),
                "x":            shot.get("playerCoordinates", {}).get("x"),
                "y":            shot.get("playerCoordinates", {}).get("y"),
                "xg":           shot.get("xg"),
                "result":       shot.get("shotType"),
                "shot_type":    shot.get("bodyPart"),
                "situation":    shot.get("situation"),
                "data_source":  "sofascore",
            })

    if not all_shots:
        log.info("  %s — sin tiros en JSON crudos", season_dir.name)
        return 0

    df_new = pd.DataFrame(all_shots)
    df_new["x"]      = pd.to_numeric(df_new["x"],      errors="coerce").round(4)
    df_new["y"]      = pd.to_numeric(df_new["y"],      errors="coerce").round(4)
    df_new["xg"]     = pd.to_numeric(df_new["xg"],     errors="coerce").round(4)
    df_new["minute"] = pd.to_numeric(df_new["minute"], errors="coerce")

    # sobreescribe el CSV existente con is_home incluido
    df_new.to_csv(shots_clean_path, index=False, encoding="utf-8-sig")
    log.info("  %s — %d tiros guardados con is_home", season_dir.name, len(df_new))
    return len(df_new)


def main():
    log.info("Añadiendo is_home a shots_clean.csv de Champions...")
    total = 0

    for season_dir in sorted(SS_CHAMPIONS.iterdir()):
        if not season_dir.is_dir() or not season_dir.name.startswith("season="):
            continue
        log.info("Procesando %s", season_dir.name)
        total += process_season(season_dir)

    log.info("Completado — %d tiros procesados en total", total)


if __name__ == "__main__":
    main()
