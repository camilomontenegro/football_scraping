from __future__ import annotations

import hashlib
import re
import unicodedata

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import _infer_country_from_team, geocode_stadium_fallback


def _slugify(value: str) -> str:
    s = (value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:150]


SQL_BLOCKED_TEAMS = text(
    """
    SELECT DISTINCT t.canonical_id, t.canonical_name, COALESCE(t.country, '') AS country
    FROM dim_match m
    JOIN dim_team t ON t.canonical_id = m.home_team_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
      AND NOT EXISTS (
          SELECT 1
          FROM dim_stadium s
          WHERE s.canonical_team_id = m.home_team_id
      )
    ORDER BY t.canonical_id
    """
)


SQL_INSERT = text(
    """
    INSERT INTO dim_stadium (
        canonical_team_id,
        id_transfermarkt_team,
        team_slug,
        valid_from_season,
        valid_to_season,
        stadium_name,
        city,
        country,
        latitude,
        longitude,
        timezone,
        data_source,
        data_hash,
        created_at,
        updated_at
    ) VALUES (
        :canonical_team_id,
        :id_transfermarkt_team,
        :team_slug,
        '1900/1901',
        '2099/2100',
        :stadium_name,
        NULL,
        :country,
        :latitude,
        :longitude,
        :timezone,
        'synthetic-geocode',
        :data_hash,
        NOW(),
        NOW()
    )
    ON CONFLICT (id_transfermarkt_team, valid_from_season) DO NOTHING
    """
)


def main() -> int:
    with engine.connect() as conn:
        blocked = conn.execute(SQL_BLOCKED_TEAMS).mappings().fetchall()

    inserted = 0
    with_coords = 0
    for row in blocked:
        team = row["canonical_name"]
        country = row["country"] or _infer_country_from_team(team, "")
        geo = geocode_stadium_fallback(f"{team} stadium", team=team, country=country)
        lat = geo.get("latitude")
        lon = geo.get("longitude")
        tz = geo.get("timezone")

        payload = {
            "canonical_team_id": row["canonical_id"],
            "id_transfermarkt_team": -int(row["canonical_id"]),
            "team_slug": _slugify(team),
            "stadium_name": f"{team} (auto geocoded)",
            "country": country or None,
            "latitude": lat,
            "longitude": lon,
            "timezone": tz,
            "data_hash": hashlib.md5(
                f"{row['canonical_id']}|{team}|synthetic-geocode".encode("utf-8"),
            ).hexdigest(),
        }

        with engine.begin() as conn:
            res = conn.execute(SQL_INSERT, payload)
        if res.rowcount:
            inserted += 1
            if lat is not None and lon is not None:
                with_coords += 1

    print(
        f"Inserted synthetic stadium rows: {inserted}/{len(blocked)} | with coords: {with_coords}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
