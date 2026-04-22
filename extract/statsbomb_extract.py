"""
extract/statsbomb_extract.py
============================
Extrae eventos de StatsBomb Open Data (GitHub, libre, sin API key).

Estructura de salida:
    data/raw/statsbomb/
        competition_{id}/
            season_{id}/
                match_{id}/
                    batch_id={batch}/
                        events.json
                        lineups.json

Uso:
    from extract.statsbomb_extract import run_statsbomb_extract
    # La Liga 2020/21 = competition_id=11, season_id=90
    run_statsbomb_extract(competition_id=11, season_id=90)

Ver competiciones disponibles:
    from scrapers.statsbomb import list_competitions
    print(list_competitions()[['competition_id','competition_name','season_id','season_name']])
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd

from extract.base_extractor import save_json
from scrapers.statsbomb import get_events, get_lineups, list_matches
from utils.batch import generate_batch_id

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/statsbomb")


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Convierte DataFrame a lista de dicts serializable."""
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


def run_statsbomb_extract(
    competition_id: int,
    season_id: int,
    sleep_between: float = 0.3,
) -> dict:
    """
    Descarga eventos y lineups de una competición/temporada de StatsBomb Open Data.

    Args:
        competition_id:  ID de la competición en StatsBomb (e.g. 11 = La Liga)
        season_id:       ID de la temporada  (e.g. 90 = 2020/21)
        sleep_between:   Segundos entre peticiones (Open Data no tiene rate limit estricto)

    Returns:
        dict con batch_id, matches_processed, events_total, errors
    """
    batch_id = generate_batch_id()
    log.info("STATSBOMB EXTRACT START | comp=%d | season=%d | batch=%s",
            competition_id, season_id, batch_id)

    stats = {
        "batch_id": batch_id,
        "competition_id": competition_id,
        "season_id": season_id,
        "matches_found": 0,
        "matches_processed": 0,
        "events_total": 0,
        "errors": [],
    }

    # 1. Obtener lista de partidos
    matches_df = list_matches(competition_id, season_id)
    if matches_df.empty:
        log.warning("Sin partidos para competition=%d season=%d", competition_id, season_id)
        return stats

    stats["matches_found"] = len(matches_df)
    log.info("Partidos StatsBomb: %d", len(matches_df))

    comp_dir = RAW_BASE / f"competition_{competition_id}" / f"season_{season_id}"

    for _, row in matches_df.iterrows():
        match_id = int(row["match_id"])
        home = row.get("home_team", {})
        away = row.get("away_team", {})
        home_name = home.get("home_team_name", str(home)) if isinstance(home, dict) else str(home)
        away_name = away.get("away_team_name", str(away)) if isinstance(away, dict) else str(away)

        log.info("Procesando match %d: %s vs %s", match_id, home_name, away_name)

        match_dir = comp_dir / f"match_{match_id}" / f"batch_id={batch_id}"
        match_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Eventos
            events_df = get_events(match_id)
            events_records = _df_to_records(events_df)
            save_json(events_records, match_dir / "events.json")
            stats["events_total"] += len(events_records)

            # Lineups
            lineups = get_lineups(match_id)
            # lineups es un dict {team_name: DataFrame o list}
            lineups_serializable = {}
            for team, data in lineups.items():
                if isinstance(data, pd.DataFrame):
                    lineups_serializable[team] = data.to_dict(orient="records")
                else:
                    lineups_serializable[team] = data
            save_json(lineups_serializable, match_dir / "lineups.json")

            stats["matches_processed"] += 1
            log.info("  → %d eventos guardados", len(events_records))

        except Exception as exc:
            log.error("Error en match %d: %s", match_id, exc)
            stats["errors"].append({"match_id": match_id, "error": str(exc)})

        time.sleep(sleep_between)

    log.info("STATSBOMB EXTRACT DONE | matches=%d | events=%d | errors=%d",
            stats["matches_processed"], stats["events_total"], len(stats["errors"]))
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    from scrapers.statsbomb import list_competitions
    comps = list_competitions()
    if not comps.empty:
        laliga = comps[
            (comps["competition_name"] == "La Liga") &
            (comps["season_name"].str.contains("2020/2021"))
        ]
        if not laliga.empty:
            row = laliga.iloc[0]
            run_statsbomb_extract(
                competition_id=int(row["competition_id"]),
                season_id=int(row["season_id"]),
            )
        else:
            print("La Liga 2020/21 no disponible en StatsBomb Open Data")
            print(comps[["competition_name", "season_name", "competition_id", "season_id"]].to_string())
