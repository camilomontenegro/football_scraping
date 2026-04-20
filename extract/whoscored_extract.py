"""
extract/whoscored_extract.py
============================
Extrae eventos de partidos de WhoScored usando Selenium.

Estructura de salida:
    data/raw/whoscored/
        match_{id}/
            batch_id={batch}/
                events.json       ← matchCentreData completo

Uso:
    from extract.whoscored_extract import run_whoscored_extract
    run_whoscored_extract(match_ids=[1657100, 1657101])
"""
from __future__ import annotations

import logging
import random
import time
from pathlib import Path

from extract.base_extractor import save_json
from scrapers.whoscored import build_driver, fetch_match_data
from utils.batch import generate_batch_id

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/whoscored")


def run_whoscored_extract(
    match_ids: list[int],
    sleep_min: float = 3.0,
    sleep_max: float = 7.0,
    headless: bool = False,
) -> dict:
    """
    Descarga datos de partidos de WhoScored vía Selenium.

    Args:
        match_ids:   Lista de IDs de partido de WhoScored
        sleep_min:   Espera mínima entre peticiones (segundos)
        sleep_max:   Espera máxima entre peticiones (segundos)
        headless:    Si True, intenta Chrome headless (puede ser bloqueado)

    Returns:
        dict con batch_id, processed, errors
    """
    batch_id = generate_batch_id()
    log.info("WHOSCORED EXTRACT START | matches=%d | batch=%s", len(match_ids), batch_id)

    stats = {
        "batch_id": batch_id,
        "matches_requested": len(match_ids),
        "matches_processed": 0,
        "errors": [],
    }

    driver = build_driver(headless=headless)

    try:
        for match_id in match_ids:
            log.info("Procesando WhoScored match %d", match_id)

            try:
                match_data = fetch_match_data(driver, match_id)

                if not match_data:
                    log.warning("Sin datos para match %d", match_id)
                    stats["errors"].append({"match_id": match_id, "error": "no matchCentreData"})
                else:
                    match_dir = RAW_BASE / f"match_{match_id}" / f"batch_id={batch_id}"
                    match_dir.mkdir(parents=True, exist_ok=True)
                    save_json(match_data, match_dir / "events.json")

                    nevents = len(match_data.get("events", []))
                    log.info("  → %d eventos guardados", nevents)
                    stats["matches_processed"] += 1

            except Exception as exc:
                log.error("Error en match %d: %s", match_id, exc)
                stats["errors"].append({"match_id": match_id, "error": str(exc)})

            # Rate limiting — WhoScored es sensible a velocidad
            wait = random.uniform(sleep_min, sleep_max)
            log.debug("Esperando %.1fs...", wait)
            time.sleep(wait)

    finally:
        driver.quit()
        log.info("Driver cerrado")

    log.info("WHOSCORED EXTRACT DONE | processed=%d | errors=%d",
            stats["matches_processed"], len(stats["errors"]))
    return stats


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Extraer datos de WhoScored")
    parser.add_argument("--match-ids", nargs="+", type=int, required=True,
                        help="IDs de partido WhoScored")
    args = parser.parse_args()
    run_whoscored_extract(args.match_ids)
