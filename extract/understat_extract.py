"""
extract/understat_extract.py
============================
Extrae datos de tiros de Understat para una liga/temporada y los guarda en raw.

Estructura de salida:
    data/raw/understat/
        season={year}/
            match_{id}/
                batch_id={batch}/
                    shots.json
            matches_{year}_batch_{batch}.json

Uso:
    from extract.understat_extract import run_understat_extract
    run_understat_extract(league='La_Liga', season='2020', teams=['Real Madrid','Barcelona'])
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path

from extract.base_extractor import save_json
from scrapers.understat import get_league_matches, get_match_shots
from utils.batch import generate_batch_id

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/understat")


def run_understat_extract(
    league: str = "La_Liga",
    season: str = "2020",
    teams: list[str] | None = None,
    sleep_between: float = 0.8,
) -> dict:
    """
    Descarga partidos y tiros de Understat.

    Args:
        league:          Nombre de liga para Understat (e.g. 'La_Liga')
        season:          Año de inicio de la temporada (e.g. '2020')
        teams:           Lista de equipos a filtrar. None = todos.
        sleep_between:   Segundos entre peticiones

    Returns:
        dict con batch_id, shots_total, matches_processed, errors
    """
    batch_id = generate_batch_id()
    log.info("UNDERSTAT EXTRACT START | league=%s | season=%s | batch=%s", league, season, batch_id)

    stats = {
        "batch_id": batch_id,
        "league": league,
        "season": season,
        "matches_found": 0,
        "matches_processed": 0,
        "shots_total": 0,
        "errors": [],
    }

    # 1. Obtener lista de partidos
    matches = get_league_matches(league, season)
    if not matches:
        log.warning("Sin partidos para %s %s", league, season)
        return stats

    stats["matches_found"] = len(matches)
    log.info("Partidos encontrados: %d", len(matches))

    # Guardar índice de partidos
    season_dir = RAW_BASE / f"season={season}"
    save_json(matches, season_dir / f"matches_{season}_batch_{batch_id}.json")

    # 2. Filtrar partidos por equipo
    teams_lower = {t.lower() for t in (teams or [])}

    for match in matches:
        h_team = match.get("h", {}).get("title", "")
        a_team = match.get("a", {}).get("title", "")
        match_id = str(match.get("id", ""))

        if teams_lower and h_team.lower() not in teams_lower and a_team.lower() not in teams_lower:
            continue

        log.info("Procesando match %s: %s vs %s", match_id, h_team, a_team)

        match_dir = season_dir / f"match_{match_id}" / f"batch_id={batch_id}"
        match_dir.mkdir(parents=True, exist_ok=True)

        try:
            shots = get_match_shots(match_id)

            # Inyectar metadatos en cada tiro
            for s in shots:
                s["match_id"] = match_id
                s["h_team"]   = h_team
                s["a_team"]   = a_team
                s["season"]   = season

            save_json(shots, match_dir / "shots.json")
            stats["shots_total"] += len(shots)
            stats["matches_processed"] += 1
            log.info("  → %d tiros guardados", len(shots))

        except Exception as exc:
            log.error("Error en match %s: %s", match_id, exc)
            stats["errors"].append({"match_id": match_id, "error": str(exc)})

        time.sleep(sleep_between)

    log.info("UNDERSTAT EXTRACT DONE | matches=%d | shots=%d | errors=%d",
             stats["matches_processed"], stats["shots_total"], len(stats["errors"]))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    run_understat_extract(
        league="La_Liga",
        season="2020",
        teams=["Real Madrid", "Barcelona"],
    )
