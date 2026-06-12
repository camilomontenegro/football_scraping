"""Inserta filas sintéticas solo para equipos sin estadio y sin fila TM."""
from __future__ import annotations

import hashlib
import re
import unicodedata

from sqlalchemy import text

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    _entity_label,
    _fetch_entity,
    _infer_country_from_team,
    _lookup_stadium_override,
    _name_looks_like_club,
    resolve_stadium_coords_with_fallback,
)

_TM_EXISTS_SQL = text("""
    SELECT 1 FROM dim_stadium
    WHERE data_source = 'transfermarkt'
      AND (
        team_slug = :slug
        OR team_slug LIKE :slug || '-%'
        OR :slug LIKE team_slug || '-%'
      )
    LIMIT 1
""")

SQL_BLOCKED_TEAMS = text(
    """
    SELECT DISTINCT t.canonical_id, t.canonical_name, COALESCE(t.country, '') AS country
    FROM dim_match m
    JOIN dim_team t ON t.canonical_id = m.home_team_id
    WHERE m.match_date IS NOT NULL
      AND m.temperature_c IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM dim_stadium s WHERE s.canonical_team_id = m.home_team_id
      )
    ORDER BY t.canonical_id
    """
)

SQL_TEAM_SEASON_BOUNDS = text(
    """
    SELECT MIN(m.season) AS min_s, MAX(m.season) AS max_s
    FROM dim_match m
    WHERE :tid IN (m.home_team_id, m.away_team_id) AND m.season IS NOT NULL
    """
)

SQL_GLOBAL_SEASON_BOUNDS = text(
    "SELECT MIN(season) AS min_s, MAX(season) AS max_s FROM dim_match WHERE season IS NOT NULL"
)

SQL_INSERT = text(
    """
    INSERT INTO dim_stadium (
        canonical_team_id, id_transfermarkt_team, team_slug,
        valid_from_season, valid_to_season, stadium_name,
        city, country, latitude, longitude, timezone,
        wikidata_qid, wikipedia_url, image_url,
        data_source, data_hash, created_at, updated_at
    ) VALUES (
        :canonical_team_id, :id_transfermarkt_team, :team_slug,
        :valid_from_season, :valid_to_season, :stadium_name,
        :city, :country, :latitude, :longitude, :timezone,
        :wikidata_qid, :wikipedia_url, :image_url,
        'synthetic-geocode', :data_hash, NOW(), NOW()
    )
    ON CONFLICT (id_transfermarkt_team, valid_from_season) DO NOTHING
    """
)


def _slugify(value: str) -> str:
    s = (value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s[:150]


def _season_bounds(conn, canonical_id: int) -> tuple[str, str]:
    bounds = conn.execute(SQL_TEAM_SEASON_BOUNDS, {"tid": canonical_id}).one()
    if bounds.min_s and bounds.max_s:
        return bounds.min_s, bounds.max_s
    global_bounds = conn.execute(SQL_GLOBAL_SEASON_BOUNDS).one()
    return global_bounds.min_s or "2020/2021", global_bounds.max_s or "2025/2026"


def _stadium_name_from_geo(team: str, geo: dict) -> str | None:
    override = _lookup_stadium_override(team, "")
    if override.get("stadium_name") and not _name_looks_like_club(
        override["stadium_name"], team,
    ):
        return override["stadium_name"]

    qid = geo.get("wikidata_qid")
    if qid:
        ent = _fetch_entity(str(qid))
        if ent:
            label = _entity_label(ent)
            if label and not _name_looks_like_club(label, team):
                return label
    return None


def main() -> int:
    with engine.connect() as conn:
        blocked = conn.execute(SQL_BLOCKED_TEAMS).mappings().fetchall()

    inserted = 0
    skipped_tm = 0
    skipped_nocoords = 0

    for row in blocked:
        team = row["canonical_name"]
        slug = _slugify(team)

        with engine.connect() as conn:
            if conn.execute(_TM_EXISTS_SQL, {"slug": slug}).fetchone():
                skipped_tm += 1
                continue

        country = row["country"] or _infer_country_from_team(team, "")
        geo = resolve_stadium_coords_with_fallback(
            "", team=team, country=country, use_wikidata=True,
        )
        lat, lon = geo.get("latitude"), geo.get("longitude")
        if lat is None or lon is None:
            skipped_nocoords += 1
            continue

        stadium_name = _stadium_name_from_geo(team, geo)
        payload = {
            "canonical_team_id": row["canonical_id"],
            "id_transfermarkt_team": -int(row["canonical_id"]),
            "team_slug": slug,
            "stadium_name": stadium_name,
            "city": geo.get("city"),
            "country": geo.get("country") or country or None,
            "latitude": lat,
            "longitude": lon,
            "timezone": geo.get("timezone"),
            "wikidata_qid": geo.get("wikidata_qid"),
            "wikipedia_url": geo.get("wikipedia_url"),
            "image_url": geo.get("image_url"),
            "data_hash": hashlib.md5(
                f"{row['canonical_id']}|{team}|synthetic-geocode|{lat}|{lon}".encode(),
            ).hexdigest(),
        }

        with engine.begin() as conn:
            valid_from, valid_to = _season_bounds(conn, int(row["canonical_id"]))
            payload["valid_from_season"] = valid_from
            payload["valid_to_season"] = valid_to
            res = conn.execute(SQL_INSERT, payload)
        if res.rowcount:
            inserted += 1

    print(
        f"Inserted synthetic rows: {inserted}/{len(blocked)} | "
        f"skipped (TM exists): {skipped_tm} | skipped (no coords): {skipped_nocoords}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
