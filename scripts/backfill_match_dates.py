"""
backfill_match_dates.py
========================
Rellena dim_match.match_date para los partidos cargados desde WhoScored
que tienen match_date NULL en BD.

Estrategia:
    1. Lee dim_match con (match_date IS NULL AND id_whoscored IS NOT NULL).
    2. Por cada partido, abre la URL https://es.whoscored.com/matches/<id>/live
       y extrae el `matchCentreData.startDate` (sin procesar los eventos,
       mucho más rápido que re-scrapear).
    3. UPDATE dim_match SET match_date = ... WHERE match_id = ...

Uso:
    python -m scripts.backfill_match_dates
    python -m scripts.backfill_match_dates --limit 50
    python -m scripts.backfill_match_dates --headless

Ojo: tarda ~3-6 segundos por partido por las pausas anti-bot.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Permitir imports desde la raíz del proyecto
sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from loaders.common import engine  # noqa: E402

# Reusamos el driver, helpers y extractores del scraper de WhoScored
from scrapers.whoscored_scraper import (  # noqa: E402
    create_driver,
    accept_cookies,
    get_match_data,
    DELAY_MIN,
    DELAY_MAX,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill match_date desde WhoScored")
    parser.add_argument("--limit", type=int, default=None,
                        help="Procesar solo los primeros N partidos.")
    parser.add_argument("--season-tag", type=str, default="backfill",
                        help="Etiqueta interna que pasa el scraper (no afecta la BD).")
    args = parser.parse_args()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT match_id, id_whoscored
            FROM dim_match
            WHERE match_date IS NULL
              AND id_whoscored IS NOT NULL
            ORDER BY match_id
        """)).fetchall()

    if not rows:
        print("[OK] Todos los partidos ya tienen match_date. Nada que hacer.")
        return

    if args.limit:
        rows = rows[: args.limit]

    total = len(rows)
    print(f"[+] {total} partidos sin fecha. Iniciando backfill...")
    print(f"    Tiempo estimado: ~{total * (DELAY_MIN + DELAY_MAX) / 2 / 60:.0f} minutos")

    driver = create_driver()
    try:
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        updated = failed = 0
        for i, (match_id, ws_id) in enumerate(rows, 1):
            log.info("[%d/%d] match_id=%d ws=%d", i, total, match_id, ws_id)
            data = get_match_data(driver, str(ws_id), args.season_tag)
            mdate = data.get("match_date") if isinstance(data, dict) else None

            if not mdate:
                failed += 1
                log.warning("  · sin fecha extraída")
                continue

            # UPDATE dim_match
            with engine.begin() as conn2:
                conn2.execute(text("""
                    UPDATE dim_match
                    SET match_date = :d
                    WHERE match_id = :mid AND match_date IS NULL
                """), {"d": mdate, "mid": match_id})
            updated += 1
            log.info("  · fecha=%s", mdate)

            if i % 20 == 0:
                print(f"  -> progreso {i}/{total} | actualizados={updated}, fallidos={failed}")

        print("\n" + "=" * 60)
        print(f"[OK] Backfill terminado: {updated} actualizados, {failed} fallidos")
        print("=" * 60)
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
