"""
Re-fetch clima para partidos con match_stadium_id pero sin datos meteorológicos.

Tras eliminar dim_match.stadium_id (legacy), este script solo limpia/re-aplica
clima en partidos pendientes.
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from loaders.common import engine
from scrapers.weather_scraper import enrich_weather

log = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="Solo muestra conteos, no llama a NASA/Open-Meteo.")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    with engine.connect() as conn:
        n_pending = conn.execute(text("""
            SELECT COUNT(*) FROM dim_match
            WHERE temperature_c IS NULL AND match_date IS NOT NULL
              AND match_stadium_id IS NOT NULL
        """)).scalar()

    log.info("Partidos sin clima con match_stadium_id: %s", n_pending)

    if args.skip_fetch or args.dry_run:
        return 0

    updated = enrich_weather(dry_run=False, limit=args.limit, provider="nasa")
    log.info("Clima re-aplicado en %s partidos", updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
