"""Copia lat/lon a filas match-venue sin coords (dim_stadium o JSON SofaScore)."""
from __future__ import annotations

import logging

from sqlalchemy import text

from loaders.common import engine
from loaders.match_stadium_resolver import (
    DEFAULT_DATA_ROOT,
    load_sofascore_venues,
    name_similarity,
    normalize_name,
)

log = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    updated = 0
    ss_by_norm: dict[str, object] = {}
    for v in load_sofascore_venues(DEFAULT_DATA_ROOT).values():
        if v.latitude is not None and v.longitude is not None:
            ss_by_norm[normalize_name(v.name)] = v

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, stadium_name, latitude, longitude
            FROM dim_stadium
            WHERE data_source = 'match-venue'
              AND (latitude IS NULL OR longitude IS NULL)
              AND stadium_name IS NOT NULL
        """)).mappings().all()
        for row in rows:
            src = conn.execute(text("""
                SELECT stadium_id, latitude, longitude, city, country
                FROM dim_stadium
                WHERE stadium_id <> :self
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
                  AND LOWER(TRIM(stadium_name)) = LOWER(TRIM(:name))
                ORDER BY CASE WHEN data_source <> 'match-venue' THEN 0 ELSE 1 END
                LIMIT 1
            """), {"self": row["stadium_id"], "name": row["stadium_name"]}).mappings().first()
            if not src:
                norm = normalize_name(row["stadium_name"])
                best_score, best_v = 0.0, None
                for sn, v in ss_by_norm.items():
                    score = name_similarity(norm, sn)
                    if score > best_score:
                        best_score, best_v = score, v
                if best_score >= 0.55 and best_v:
                    conn.execute(text("""
                        UPDATE dim_stadium
                        SET latitude = :lat, longitude = :lon,
                            city = COALESCE(city, :city),
                            country = COALESCE(country, :country),
                            updated_at = NOW()
                        WHERE stadium_id = :id
                    """), {
                        "lat": best_v.latitude, "lon": best_v.longitude,
                        "city": best_v.city, "country": best_v.country,
                        "id": row["stadium_id"],
                    })
                    updated += 1
                    log.info("%s <- SS fuzzy (%.2f) %s", row["stadium_name"], best_score, best_v.name)
                continue
            conn.execute(text("""
                UPDATE dim_stadium
                SET latitude = :lat, longitude = :lon,
                    city = COALESCE(city, :city),
                    country = COALESCE(country, :country),
                    updated_at = NOW()
                WHERE stadium_id = :id
            """), {
                "lat": src["latitude"], "lon": src["longitude"],
                "city": src["city"], "country": src["country"],
                "id": row["stadium_id"],
            })
            updated += 1
            log.info("%s <- coords from stadium_id=%s", row["stadium_name"], src["stadium_id"])
    print(f"updated: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
