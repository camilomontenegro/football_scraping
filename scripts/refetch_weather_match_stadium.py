"""
Borra clima de partidos cuyo estadio real (match_stadium_id) difiere del legacy
stadium_id y vuelve a ejecutar weather_scraper.
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from loaders.common import engine
from scrapers.weather_scraper import enrich_weather

log = logging.getLogger(__name__)

CLEAR_SQL = text("""
    UPDATE dim_match
    SET temperature_c = NULL,
        humidity_pct = NULL,
        precipitation_mm = NULL,
        wind_speed_kmh = NULL,
        weather_code = NULL
    WHERE match_stadium_id IS NOT NULL
      AND stadium_id IS NOT NULL
      AND match_stadium_id <> stadium_id
      AND temperature_c IS NOT NULL
""")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="Solo limpia clima, no llama a NASA/Open-Meteo.")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    with engine.connect() as conn:
        n_clear = conn.execute(text("""
            SELECT COUNT(*) FROM dim_match
            WHERE match_stadium_id IS NOT NULL AND stadium_id IS NOT NULL
              AND match_stadium_id <> stadium_id AND temperature_c IS NOT NULL
        """)).scalar()
        n_pending = conn.execute(text("""
            SELECT COUNT(*) FROM dim_match
            WHERE temperature_c IS NULL AND match_date IS NOT NULL
              AND COALESCE(match_stadium_id, stadium_id) IS NOT NULL
        """)).scalar()

    log.info("Partidos a limpiar (estadio cambiado): %s", n_clear)
    log.info("Partidos sin clima tras limpieza (total pendientes): %s", n_pending)

    if not args.dry_run:
        with engine.begin() as conn:
            cleared = conn.execute(CLEAR_SQL).rowcount
        log.info("Clima borrado en %s partidos", cleared)

    if args.skip_fetch or args.dry_run:
        return 0

    updated = enrich_weather(dry_run=False, limit=args.limit, provider="nasa")
    log.info("Clima re-aplicado en %s partidos", updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
