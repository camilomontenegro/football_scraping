"""
Rellena campos útiles de dim_stadium que siguen vacíos tras la carga principal.

- country: desde dim_team
- timezone: offline con timezonefinder (lat/lon)
- altitude_m: API Open-Meteo elevation
- wikipedia_url: sitelink en/es desde Wikidata QID
- city: dirección, Wikidata P131, Nominatim reverse
- TM/CSV/Wikidata: seats_total, built_year, surface, owner, operator, architect, tm_url, etc.

    python -m scripts.backfill_stadium_sparse_fields --dry-run
    python -m scripts.backfill_stadium_sparse_fields --all
    python -m scripts.backfill_stadium_sparse_fields --city-only
"""

from __future__ import annotations

import argparse
import logging
import time

from sqlalchemy import text

from loaders.common import engine
from loaders.stadium_loader import city_looks_invalid, parse_city_from_address
from scrapers.wikidata_stadium_enricher import (
    _derive_altitude,
    _derive_timezone,
    _lookup_stadium_override,
    _search_entity_id,
    capacity_from_club_name,
    capacity_from_entity,
    capacity_from_qid_or_venue,
    city_from_wikidata_qid,
    enrichment_from_qid,
    query_wikidata_by_stadium_name,
    reverse_geocode_city,
    reverse_geocode_country,
)

TM_COPY_FIELDS = (
    "seats_total", "vip_boxes", "built_year", "construction_cost",
    "owner", "operator", "surface", "architect", "tm_url", "country",
)

CSV_FIELDS = TM_COPY_FIELDS

WIKIDATA_FIELDS = (
    "architect", "operator", "owner", "surface", "built_year",
    "wikipedia_url", "image_url", "country", "seats_total",
)

log = logging.getLogger(__name__)

def _is_empty(col: str) -> str:
    return f"({col} IS NULL OR TRIM(CAST({col} AS TEXT)) = '')"


def backfill_country(dry_run: bool) -> int:
    updated = 0
    with engine.begin() as conn:
        if dry_run:
            n_team = conn.execute(text(f"""
                SELECT COUNT(*) FROM dim_stadium ds
                JOIN dim_team t ON t.canonical_id = ds.canonical_team_id
                WHERE {_is_empty('ds.country')}
                  AND t.country IS NOT NULL AND TRIM(t.country) <> ''
            """)).scalar()
        else:
            n_team = conn.execute(text(f"""
                UPDATE dim_stadium ds
                SET country = t.country, updated_at = NOW()
                FROM dim_team t
                WHERE t.canonical_id = ds.canonical_team_id
                  AND {_is_empty('ds.country')}
                  AND t.country IS NOT NULL AND TRIM(t.country) <> ''
            """)).rowcount or 0
    updated += n_team

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT stadium_id, latitude, longitude, wikidata_qid
            FROM dim_stadium
            WHERE {_is_empty('country')}
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)).mappings().all()

    for row in rows:
        country = None
        qid = str(row["wikidata_qid"] or "").strip()
        if qid.startswith("Q"):
            wd = enrichment_from_qid(qid)
            country = (wd.get("country") or "").strip() or None
        if not country:
            country = reverse_geocode_country(
                float(row["latitude"]), float(row["longitude"]),
            )
            time.sleep(1.1)
        if not country:
            continue
        updated += 1
        log.info("country stadium_id=%s -> %s", row["stadium_id"], country)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET country = :c, updated_at = NOW() "
                        "WHERE stadium_id = :id"
                    ),
                    {"c": country, "id": row["stadium_id"]},
                )
    return updated


def backfill_from_tm_sibling(dry_run: bool) -> int:
    total = 0
    with engine.begin() as conn:
        for field in TM_COPY_FIELDS:
            sql = f"""
                UPDATE dim_stadium sg
                SET {field} = tm.{field}, updated_at = NOW()
                FROM dim_stadium tm
                WHERE {_is_empty(f'sg.{field}')}
                  AND tm.data_source = 'transfermarkt'
                  AND tm.canonical_team_id = sg.canonical_team_id
                  AND tm.{field} IS NOT NULL
                  AND TRIM(CAST(tm.{field} AS TEXT)) <> ''
            """
            if dry_run:
                n = conn.execute(text(f"""
                    SELECT COUNT(*) FROM dim_stadium sg
                    WHERE {_is_empty(f'sg.{field}')}
                      AND EXISTS (
                        SELECT 1 FROM dim_stadium tm
                        WHERE tm.data_source = 'transfermarkt'
                          AND tm.canonical_team_id = sg.canonical_team_id
                          AND tm.{field} IS NOT NULL
                          AND TRIM(CAST(tm.{field} AS TEXT)) <> ''
                      )
                """)).scalar()
            else:
                n = conn.execute(text(sql)).rowcount or 0
            if n:
                log.info("TM sibling %s: %d", field, n)
            total += n
    return total


def _load_csv_by_slug() -> dict[str, dict]:
    from pathlib import Path
    import pandas as pd

    clean_root = Path(r"C:\Users\Ivan\Desktop\stadiums\clean")
    by_slug: dict[str, dict] = {}
    if not clean_root.is_dir():
        return by_slug

    for path in clean_root.rglob("stadiums.csv"):
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            log.debug("No se pudo leer %s: %s", path, exc)
            continue
        if "team_slug" not in df.columns:
            continue
        for _, row in df.iterrows():
            slug = str(row.get("team_slug") or "").strip()
            if not slug:
                continue
            bucket = by_slug.setdefault(slug, {})
            for col in CSV_FIELDS:
                if col not in df.columns:
                    continue
                val = row.get(col)
                if pd.isna(val) or str(val).strip() == "":
                    continue
                bucket[col] = val
            if "vip_boxes" not in bucket and "seats_vip" in df.columns:
                val = row.get("seats_vip")
                if val is not None and not pd.isna(val) and str(val).strip():
                    bucket["vip_boxes"] = val
    return by_slug


def backfill_from_csv(dry_run: bool) -> int:
    by_slug = _load_csv_by_slug()
    if not by_slug:
        return 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug,
                   seats_total, vip_boxes, built_year, construction_cost,
                   owner, operator, surface, architect, tm_url, country
            FROM dim_stadium
            WHERE team_slug IS NOT NULL
        """)).mappings().all()

    updated = 0
    for row in rows:
        data = by_slug.get(row["team_slug"] or "")
        if not data:
            continue
        changes: dict = {}
        for col in CSV_FIELDS:
            if not _is_empty_val(row.get(col)):
                continue
            val = data.get(col)
            if val is None or str(val).strip() == "":
                continue
            if col in ("seats_total", "vip_boxes", "built_year"):
                try:
                    val = int(float(val))
                except (ValueError, TypeError):
                    continue
            changes[col] = val
        if not changes:
            continue
        updated += 1
        log.info("CSV stadium_id=%s %s %s", row["stadium_id"], row["team_slug"], list(changes))
        if not dry_run:
            set_clause = ", ".join(f"{k} = :{k}" for k in changes)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"UPDATE dim_stadium SET {set_clause}, updated_at = NOW() "
                        f"WHERE stadium_id = :id"
                    ),
                    {**changes, "id": row["stadium_id"]},
                )
    return updated


def _is_empty_val(val) -> bool:
    return val is None or str(val).strip() == ""


QID_COPY_FIELDS = (
    "wikipedia_url", "image_url", "surface", "owner", "built_year",
    "architect", "operator", "seats_total", "construction_cost", "vip_boxes",
    "country", "timezone", "altitude_m",
)


def backfill_from_qid_sibling(dry_run: bool) -> int:
    """Copia metadatos entre filas que comparten el mismo wikidata_qid."""
    total = 0
    with engine.begin() as conn:
        for field in QID_COPY_FIELDS:
            sql = f"""
                UPDATE dim_stadium sg
                SET {field} = src.{field}, updated_at = NOW()
                FROM dim_stadium src
                WHERE {_is_empty(f'sg.{field}')}
                  AND sg.wikidata_qid IS NOT NULL AND TRIM(sg.wikidata_qid) <> ''
                  AND src.wikidata_qid = sg.wikidata_qid
                  AND src.stadium_id <> sg.stadium_id
                  AND src.{field} IS NOT NULL
                  AND TRIM(CAST(src.{field} AS TEXT)) <> ''
            """
            if dry_run:
                n = conn.execute(text(f"""
                    SELECT COUNT(*) FROM dim_stadium sg
                    WHERE {_is_empty(f'sg.{field}')}
                      AND EXISTS (
                        SELECT 1 FROM dim_stadium src
                        WHERE src.wikidata_qid = sg.wikidata_qid
                          AND src.stadium_id <> sg.stadium_id
                          AND src.{field} IS NOT NULL
                          AND TRIM(CAST(src.{field} AS TEXT)) <> ''
                      )
                """)).scalar()
            else:
                n = conn.execute(text(sql)).rowcount or 0
            if n:
                log.info("QID sibling %s: %d", field, n)
            total += n
    return total


def _tm_id_by_slug_from_csv() -> dict[str, int]:
    from pathlib import Path
    import pandas as pd

    roots = (
        Path(r"C:\Users\Ivan\Desktop\stadiums\clean"),
        Path(r"C:\Users\Ivan\Desktop\stadiums"),
    )
    by_slug: dict[str, int] = {}
    for root in roots:
        if not root.is_dir():
            continue
        for path in root.rglob("stadiums.csv"):
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if "team_slug" not in df.columns:
                continue
            id_col = "team_id_tm" if "team_id_tm" in df.columns else (
                "team_id" if "team_id" in df.columns else None
            )
            if not id_col:
                continue
            for _, row in df.iterrows():
                slug = str(row.get("team_slug") or "").strip()
                raw = row.get(id_col)
                if not slug or pd.isna(raw):
                    continue
                try:
                    tm_id = int(float(raw))
                except (ValueError, TypeError):
                    continue
                if tm_id > 0:
                    by_slug[slug] = tm_id
    return by_slug


def backfill_tm_url_from_team(dry_run: bool) -> int:
    """Construye tm_url desde dim_team.id_transfermarkt o team_id_tm en CSV."""
    updated = 0
    by_slug = _tm_id_by_slug_from_csv()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT sg.stadium_id, sg.team_slug, sg.valid_to_season,
                   t.id_transfermarkt
            FROM dim_stadium sg
            LEFT JOIN dim_team t ON t.canonical_id = sg.canonical_team_id
            WHERE sg.team_slug IS NOT NULL AND TRIM(sg.team_slug) <> ''
              AND (sg.tm_url IS NULL OR TRIM(sg.tm_url) = '')
        """)).mappings().all()

    for row in rows:
        slug = row["team_slug"]
        tm_id = row["id_transfermarkt"] or by_slug.get(slug)
        if not tm_id:
            continue
        season = str(row["valid_to_season"] or "").split("/")[-1] or "2025"
        url = (
            f"https://www.transfermarkt.es/{slug}/stadion"
            f"/verein/{int(tm_id)}/saison_id/{season}"
        )
        updated += 1
        log.info("tm_url stadium_id=%s %s -> %s", row["stadium_id"], slug, url[:70])
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET tm_url = :url, updated_at = NOW() "
                        "WHERE stadium_id = :id"
                    ),
                    {"url": url, "id": row["stadium_id"]},
                )
    return updated


def backfill_seats_from_capacity(dry_run: bool) -> int:
    sql = f"""
        UPDATE dim_stadium
        SET seats_total = capacity, updated_at = NOW()
        WHERE {_is_empty('seats_total')}
          AND capacity IS NOT NULL AND capacity > 0
    """
    with engine.begin() as conn:
        if dry_run:
            return conn.execute(text(f"""
                SELECT COUNT(*) FROM dim_stadium
                WHERE {_is_empty('seats_total')}
                  AND capacity IS NOT NULL AND capacity > 0
            """)).scalar()
        return conn.execute(text(sql)).rowcount or 0


def backfill_missing_wikidata_qids(dry_run: bool) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT s.stadium_id, s.stadium_name, s.team_slug,
                   COALESCE(t.canonical_name, s.team_slug) AS team
            FROM dim_stadium s
            LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
            WHERE {_is_empty('s.wikidata_qid')}
              AND s.stadium_name IS NOT NULL AND TRIM(s.stadium_name) <> ''
        """)).mappings().all()

    updated = 0
    for row in rows:
        qid = None
        name = row["stadium_name"] or ""
        team = row["team"] or ""
        wd = query_wikidata_by_stadium_name(name, team=team)
        qid = wd.get("wikidata_qid")
        if not qid:
            qid = _search_entity_id(name, language="en") or _search_entity_id(name, language="es")
        if not qid or not str(qid).startswith("Q"):
            continue
        updated += 1
        log.info("wikidata_qid stadium_id=%s %s -> %s", row["stadium_id"], row["team_slug"], qid)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET wikidata_qid = :qid, updated_at = NOW() "
                        "WHERE stadium_id = :id"
                    ),
                    {"qid": qid, "id": row["stadium_id"]},
                )
        time.sleep(0.25)
    return updated


def backfill_wikidata_metadata(dry_run: bool) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT stadium_id, team_slug, wikidata_qid,
                   architect, operator, owner, surface, built_year,
                   wikipedia_url, image_url, country, seats_total
            FROM dim_stadium
            WHERE wikidata_qid IS NOT NULL AND TRIM(wikidata_qid) <> ''
        """)).mappings().all()

    updated = 0
    for row in rows:
        qid = str(row["wikidata_qid"]).strip()
        if not qid.startswith("Q"):
            continue
        needs = any(_is_empty_val(row.get(f)) for f in WIKIDATA_FIELDS)
        if not needs:
            continue
        wd = enrichment_from_qid(qid)
        if not wd:
            time.sleep(0.12)
            continue
        changes: dict = {}
        for field in WIKIDATA_FIELDS:
            if not _is_empty_val(row.get(field)):
                continue
            val = wd.get(field)
            if val is None or str(val).strip() == "":
                continue
            if field == "built_year":
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    continue
            if field == "seats_total":
                try:
                    val = int(val)
                except (ValueError, TypeError):
                    continue
            changes[field] = val
        if not changes:
            time.sleep(0.12)
            continue
        updated += 1
        log.info("Wikidata meta stadium_id=%s %s %s", row["stadium_id"], row["team_slug"], list(changes))
        if not dry_run:
            set_clause = ", ".join(f"{k} = :{k}" for k in changes)
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"UPDATE dim_stadium SET {set_clause}, updated_at = NOW() "
                        f"WHERE stadium_id = :id"
                    ),
                    {**changes, "id": row["stadium_id"]},
                )
        time.sleep(0.12)
    return updated


def _maybe_rehash(dry_run: bool, *counts: int) -> None:
    if dry_run or not any(counts):
        return
    from scripts.compact_dim_stadium import backfill_hashes
    with engine.begin() as conn:
        backfill_hashes(conn, dry_run=False, force=False)


def backfill_timezone(dry_run: bool) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, latitude, longitude
            FROM dim_stadium
            WHERE (timezone IS NULL OR TRIM(timezone) = '')
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)).mappings().all()

    updated = 0
    for row in rows:
        tz = _derive_timezone(float(row["latitude"]), float(row["longitude"]))
        if not tz:
            continue
        updated += 1
        log.info("timezone stadium_id=%s -> %s", row["stadium_id"], tz)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE dim_stadium SET timezone = :tz, updated_at = NOW() WHERE stadium_id = :id"),
                    {"tz": tz, "id": row["stadium_id"]},
                )
    return updated


def backfill_altitude(dry_run: bool) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, latitude, longitude
            FROM dim_stadium
            WHERE altitude_m IS NULL
              AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)).mappings().all()

    updated = 0
    for row in rows:
        alt = _derive_altitude(float(row["latitude"]), float(row["longitude"]))
        if alt is None:
            time.sleep(0.2)
            continue
        updated += 1
        log.info("altitude stadium_id=%s -> %sm", row["stadium_id"], alt)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE dim_stadium SET altitude_m = :alt, updated_at = NOW() WHERE stadium_id = :id"),
                    {"alt": alt, "id": row["stadium_id"]},
                )
        time.sleep(0.15)
    return updated


def backfill_wikipedia(dry_run: bool) -> int:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, wikidata_qid, stadium_name, team_slug
            FROM dim_stadium
            WHERE (wikipedia_url IS NULL OR TRIM(wikipedia_url) = '')
              AND wikidata_qid IS NOT NULL AND TRIM(wikidata_qid) <> ''
        """)).mappings().all()

    updated = 0
    for row in rows:
        wd = enrichment_from_qid(str(row["wikidata_qid"]).strip())
        url = (wd.get("wikipedia_url") or "").strip() or None
        if not url or "commons.wikipedia.org" in url:
            time.sleep(0.1)
            continue
        updated += 1
        log.info("wikipedia stadium_id=%s %s -> %s", row["stadium_id"], row["team_slug"], url[:60])
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE dim_stadium SET wikipedia_url = :url, updated_at = NOW() WHERE stadium_id = :id"),
                    {"url": url, "id": row["stadium_id"]},
                )
        time.sleep(0.1)
    return updated


def backfill_city(dry_run: bool) -> int:
    """Rellena city vacía o inválida desde override, dirección, Wikidata o Nominatim."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.stadium_id, s.team_slug, s.stadium_name, s.city, s.address,
                   s.latitude, s.longitude, s.wikidata_qid,
                   COALESCE(t.canonical_name, s.team_slug) AS team
            FROM dim_stadium s
            LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
        """)).mappings().all()

    updated = 0
    for row in rows:
        city = (row["city"] or "").strip()
        name = (row["stadium_name"] or "").strip()
        addr = (row["address"] or "").strip()
        if city and not city_looks_invalid(city, name, addr):
            continue

        new_city = None
        override = _lookup_stadium_override(row["team"] or "", name, enrich=False)
        new_city = (override.get("city") or "").strip() or None

        if not new_city and addr:
            new_city = parse_city_from_address(addr)

        if not new_city:
            qid = str(row["wikidata_qid"] or "").strip()
            if qid.startswith("Q"):
                new_city = city_from_wikidata_qid(qid)

        if (
            not new_city
            and row["latitude"] is not None
            and row["longitude"] is not None
        ):
            new_city = reverse_geocode_city(
                float(row["latitude"]), float(row["longitude"])
            )
            time.sleep(1.1)

        if not new_city or city_looks_invalid(new_city, name, addr):
            continue
        if city == new_city:
            continue

        updated += 1
        log.info(
            "city stadium_id=%s %s -> %r (was %r)",
            row["stadium_id"], row["team_slug"], new_city, city or None,
        )
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET city = :city, updated_at = NOW() "
                        "WHERE stadium_id = :id"
                    ),
                    {"city": new_city, "id": row["stadium_id"]},
                )
    return updated


def backfill_capacity_from_tm(dry_run: bool) -> int:
    """Copia capacity desde fila Transfermarkt del mismo equipo."""
    sql = """
        UPDATE dim_stadium sg
        SET capacity = tm.capacity, updated_at = NOW()
        FROM dim_stadium tm
        WHERE sg.capacity IS NULL
          AND tm.data_source = 'transfermarkt'
          AND tm.capacity IS NOT NULL AND tm.capacity > 0
          AND tm.canonical_team_id IS NOT NULL
          AND tm.canonical_team_id = sg.canonical_team_id
    """
    with engine.begin() as conn:
        if dry_run:
            rows = conn.execute(text("""
                SELECT sg.stadium_id, sg.team_slug, tm.capacity
                FROM dim_stadium sg
                JOIN dim_stadium tm ON tm.canonical_team_id = sg.canonical_team_id
                WHERE sg.capacity IS NULL
                  AND tm.data_source = 'transfermarkt'
                  AND tm.capacity IS NOT NULL AND tm.capacity > 0
            """)).fetchall()
            for r in rows:
                log.info("capacity TM copy stadium_id=%s %s -> %s", r.stadium_id, r.team_slug, r.capacity)
            return len(rows)
        return conn.execute(text(sql)).rowcount or 0


def backfill_capacity_from_wikidata(dry_run: bool) -> int:
    """Rellena capacity desde Wikidata P1083 (aforo máximo)."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT s.stadium_id, s.wikidata_qid, s.team_slug, s.stadium_name,
                   COALESCE(t.canonical_name, s.team_slug) AS team
            FROM dim_stadium s
            LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
            WHERE s.capacity IS NULL
        """)).mappings().all()

    updated = 0
    for row in rows:
        cap = None
        qid = str(row["wikidata_qid"] or "").strip()
        if qid.startswith("Q"):
            cap = capacity_from_qid_or_venue(qid)
        if not cap:
            cap = capacity_from_club_name(row["team"] or "")
        if not cap or cap < 500 or cap > 250_000:
            continue
        updated += 1
        log.info(
            "capacity Wikidata stadium_id=%s %s -> %s",
            row["stadium_id"], row["team_slug"], cap,
        )
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET capacity = :cap, updated_at = NOW() "
                        "WHERE stadium_id = :id AND capacity IS NULL"
                    ),
                    {"cap": cap, "id": row["stadium_id"]},
                )
        time.sleep(0.15)
    return updated


# Aforos verificados (Wikipedia / Transfermarkt / federaciones) cuando Wikidata no tiene P1083
# o el wikidata_qid en dim_stadium apunta a otra entidad.
MANUAL_CAPACITY_BY_SLUG: dict[str, int] = {
    "atletic-club-escaldes": 850,
    "connah-s-quay-nomads": 1500,
    "differdange-fc-03": 3000,
    "dynamo-brest": 10169,
    "fc-corvinul-hunedoara": 16500,
    "fc-drita-gjilan": 1000,
    "fc-farul-constanta": 13500,
    "fc-struga-trim-lum": 500,
    "fk-iskra-danilovgrad": 2500,
    "ilves": 3100,
    "inter-club-d-escaldes": 850,
    "kf-egnatia": 5000,
    "kf-shkupi": 4500,
    "kilmarnock": 18128,
    "levski-sofia": 17688,
    "motherwell": 13677,
    "paksi-fc": 6163,
    "progres-niederkorn": 2800,
    "rc-sporting-charleroi": 14891,
    "rodez-af": 5955,
    "sabah-fk": 13000,
    "sp-tre-fiori": 700,
    "ss-folgore-falciano": 6664,
    "valmiera-fc": 1250,
    "vikingur-g-ta": 3000,
}


def backfill_capacity_from_stadium_search(dry_run: bool) -> int:
    """Busca estadio por nombre y lee P1083 (corrige QIDs erróneos en dim_stadium)."""
    from scrapers.wikidata_stadium_enricher import _search_entity_id

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug, stadium_name
            FROM dim_stadium
            WHERE capacity IS NULL AND stadium_name IS NOT NULL AND TRIM(stadium_name) <> ''
        """)).mappings().all()

    updated = 0
    for row in rows:
        name = str(row["stadium_name"]).strip()
        qid = _search_entity_id(name, language="en") or _search_entity_id(name, language="es")
        cap = capacity_from_qid_or_venue(qid) if qid else None
        if not cap or cap < 500 or cap > 250_000:
            continue
        updated += 1
        log.info(
            "capacity stadium search stadium_id=%s %s -> %s (qid=%s)",
            row["stadium_id"], row["team_slug"], cap, qid,
        )
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET capacity = :cap, updated_at = NOW() "
                        "WHERE stadium_id = :id AND capacity IS NULL"
                    ),
                    {"cap": cap, "id": row["stadium_id"]},
                )
        time.sleep(0.2)
    return updated


def backfill_capacity_manual(dry_run: bool) -> int:
    """Rellena capacity desde MANUAL_CAPACITY_BY_SLUG."""
    updated = 0
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug
            FROM dim_stadium
            WHERE capacity IS NULL
        """)).mappings().all()

    for row in rows:
        slug = row["team_slug"]
        cap = MANUAL_CAPACITY_BY_SLUG.get(slug)
        if not cap:
            continue
        updated += 1
        log.info("capacity manual stadium_id=%s %s -> %s", row["stadium_id"], slug, cap)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET capacity = :cap, updated_at = NOW() "
                        "WHERE stadium_id = :id AND capacity IS NULL"
                    ),
                    {"cap": cap, "id": row["stadium_id"]},
                )
    return updated


def backfill_capacity_from_csv(dry_run: bool) -> int:
    """Rellena capacity desde CSVs TM en Desktop/stadiums/clean."""
    from pathlib import Path
    import pandas as pd

    clean_root = Path(r"C:\Users\Ivan\Desktop\stadiums\clean")
    if not clean_root.is_dir():
        log.warning("No existe %s — omitiendo backfill CSV.", clean_root)
        return 0

    by_slug: dict[str, int] = {}
    for path in clean_root.rglob("stadiums.csv"):
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            log.debug("No se pudo leer %s: %s", path, exc)
            continue
        if "team_slug" not in df.columns or "capacity" not in df.columns:
            continue
        for _, row in df.iterrows():
            slug = str(row.get("team_slug") or "").strip()
            cap = row.get("capacity")
            if not slug or pd.isna(cap):
                continue
            try:
                cap_i = int(float(cap))
            except (ValueError, TypeError):
                continue
            if cap_i > 0:
                by_slug[slug] = max(by_slug.get(slug, 0), cap_i)

    if not by_slug:
        return 0

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug
            FROM dim_stadium
            WHERE capacity IS NULL AND team_slug IS NOT NULL
        """)).mappings().all()

    updated = 0
    for row in rows:
        slug = row["team_slug"]
        cap = by_slug.get(slug)
        if not cap:
            continue
        updated += 1
        log.info("capacity CSV stadium_id=%s %s -> %s", row["stadium_id"], slug, cap)
        if not dry_run:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE dim_stadium SET capacity = :cap, updated_at = NOW() "
                        "WHERE stadium_id = :id AND capacity IS NULL"
                    ),
                    {"cap": cap, "id": row["stadium_id"]},
                )
    return updated


def backfill_capacity(dry_run: bool) -> int:
    n_tm = backfill_capacity_from_tm(dry_run)
    n_wd = backfill_capacity_from_wikidata(dry_run)
    n_search = backfill_capacity_from_stadium_search(dry_run)
    n_csv = backfill_capacity_from_csv(dry_run)
    n_manual = backfill_capacity_manual(dry_run)
    log.info(
        "backfill_capacity: TM=%d Wikidata=%d search=%d CSV=%d manual=%d",
        n_tm, n_wd, n_search, n_csv, n_manual,
    )
    return n_tm + n_wd + n_search + n_csv + n_manual


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--capacity-only", action="store_true")
    ap.add_argument(
        "--manual-capacity-only",
        action="store_true",
        help="Solo aplica MANUAL_CAPACITY_BY_SLUG (sin llamadas Wikidata).",
    )
    ap.add_argument("--city-only", action="store_true")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Rellena todos los campos vacíos (incl. imágenes Wikidata).",
    )
    ap.add_argument("--wikipedia", action="store_true",
                    help="Rellenar wikipedia_url desde Wikidata (solo filas vacías).")
    args = ap.parse_args()

    if args.all:
        args.wikipedia = True

    if args.city_only:
        n_city = backfill_city(args.dry_run)
        if not args.dry_run and n_city:
            from scripts.compact_dim_stadium import backfill_hashes
            with engine.begin() as conn:
                backfill_hashes(conn, dry_run=False, force=False)
        print(f"city: {n_city}")
        return 0

    if args.capacity_only or args.manual_capacity_only:
        if args.manual_capacity_only:
            n_cap = backfill_capacity_manual(args.dry_run)
        else:
            n_cap = backfill_capacity(args.dry_run)
        if not args.dry_run and n_cap:
            from scripts.compact_dim_stadium import backfill_hashes
            with engine.begin() as conn:
                backfill_hashes(conn, dry_run=False, force=False)
        print(f"capacity: {n_cap}")
        return 0

    n_country = backfill_country(args.dry_run)
    n_tz = backfill_timezone(args.dry_run)
    n_alt = backfill_altitude(args.dry_run)
    n_city = backfill_city(args.dry_run)
    n_qid_sib = backfill_from_qid_sibling(args.dry_run)
    n_tm_url = backfill_tm_url_from_team(args.dry_run)
    n_tm = backfill_from_tm_sibling(args.dry_run)
    n_csv = backfill_from_csv(args.dry_run)
    n_seats = backfill_seats_from_capacity(args.dry_run)
    n_qid = backfill_missing_wikidata_qids(args.dry_run) if args.all else 0
    n_wd = backfill_wikidata_metadata(args.dry_run) if args.all else 0
    n_cap = backfill_capacity(args.dry_run) if args.all else 0
    n_wiki = backfill_wikipedia(args.dry_run) if args.wikipedia or args.all else 0
    n_img = 0
    if args.all and not args.dry_run:
        from scripts.fetch_stadium_images import fetch_images
        n_img = fetch_images()

    _maybe_rehash(
        args.dry_run,
        n_country, n_tz, n_alt, n_city, n_qid_sib, n_tm_url, n_tm, n_csv, n_seats,
        n_qid, n_wd, n_cap, n_wiki, n_img,
    )

    print(
        f"country: {n_country}  timezone: {n_tz}  altitude: {n_alt}  city: {n_city}  "
        f"qid_sibling: {n_qid_sib}  tm_url: {n_tm_url}  tm_sibling: {n_tm}  csv: {n_csv}  "
        f"seats=capacity: {n_seats}  qid: {n_qid}  wikidata_meta: {n_wd}  capacity: {n_cap}  "
        f"wikipedia: {n_wiki}  images: {n_img}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
