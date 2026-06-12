"""
Rellena city en dim_stadium cuando hay coordenadas pero falta ciudad.

Usa address (si parece ciudad) y Nominatim reverse geocoding.

    python -m scripts.backfill_stadium_city --dry-run
    python -m scripts.backfill_stadium_city
"""

from __future__ import annotations

import argparse
import logging
import re

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import reverse_geocode_city

log = logging.getLogger(__name__)

_CITY_FROM_NAME = re.compile(
    r"(?:de|del|da|di|in|at|à|von|van)\s+([A-ZÁÉÍÓÚÀÂÃÊÔÕÇÄÖÜ][\w\s\-']+?)(?:\s*\(|$)",
    re.I,
)


def _city_from_address(address: str | None, stadium_name: str | None) -> str | None:
    if not address or address == stadium_name:
        return None
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-1]
    if len(address) < 60 and not re.search(r"\d{3,}", address):
        # Evita usar naming rights ("JP Financial") como ciudad
        if len(address.split()) <= 3:
            return None
        return address.strip()
    return None


def _city_from_stadium_name(name: str | None) -> str | None:
    if not name:
        return None
    m = _CITY_FROM_NAME.search(name)
    return m.group(1).strip() if m else None


def backfill(dry_run: bool = False, limit: int | None = None) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, address, city, latitude, longitude
            FROM dim_stadium
            WHERE (city IS NULL OR TRIM(city) = '')
              AND latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY stadium_id
        """)).mappings().all()

    if limit:
        rows = rows[:limit]

    updated = 0
    for i, row in enumerate(rows, 1):
        city = (
            _city_from_address(row.get("address"), row.get("stadium_name"))
            or _city_from_stadium_name(row.get("stadium_name"))
        )
        if not city:
            city = reverse_geocode_city(float(row["latitude"]), float(row["longitude"]))

        if not city:
            log.warning("stadium_id=%s sin ciudad resoluble", row["stadium_id"])
            continue

        updated += 1
        log.info("[%d/%d] stadium_id=%s -> %s", i, len(rows), row["stadium_id"], city)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE dim_stadium
                        SET city = :city, updated_at = NOW()
                        WHERE stadium_id = :id
                    """),
                    {"city": city, "id": row["stadium_id"]},
                )

    return updated


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    n = backfill(dry_run=args.dry_run, limit=args.limit)
    print(f"{'Simulación' if args.dry_run else 'Actualizados'}: {n} estadios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
