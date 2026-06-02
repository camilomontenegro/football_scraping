"""
Aplica data/stadium_overrides.json a dim_stadium (coords, image_url, wikidata_qid).

Uso:
    python -m scripts.export_stadium_override_template
    # Edita data/stadium_overrides.json (lat, lon, wikidata_qid, image_url)
    python -m scripts.apply_stadium_overrides --dry-run
    python -m scripts.apply_stadium_overrides
    python -m scripts.apply_stadium_overrides --fetch-images   # image desde QID si falta
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loaders.common import engine
from scrapers.wikidata_stadium_enricher import (
    STADIUM_OVERRIDES_PATH,
    _derive_timezone,
    _propagate_coords_to_team,
    resolve_stadium_image,
)

log = logging.getLogger(__name__)


def _resolve_team_id(conn, entry: dict) -> int | None:
    match_slug = entry.get("match")
    if match_slug:
        row = conn.execute(
            text("""
                SELECT t.canonical_id
                FROM dim_stadium s
                JOIN dim_team t ON t.canonical_id = s.canonical_team_id
                WHERE s.team_slug = :v
                LIMIT 1
            """),
            {"v": match_slug},
        ).fetchone()
        if row:
            return int(row[0])

    name = entry.get("team")
    if name:
        row = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = LOWER(:n) LIMIT 1"),
            {"n": name},
        ).fetchone()
        if row:
            return int(row[0])
        row = conn.execute(
            text("""
                SELECT canonical_id FROM dim_team
                WHERE LOWER(canonical_name) LIKE LOWER(:pat)
                LIMIT 1
            """),
            {"pat": f"%{name[:12]}%"},
        ).fetchone()
        if row:
            return int(row[0])
    return None


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Apply stadium_overrides.json to dim_stadium.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--fetch-images",
        action="store_true",
        help="Si hay wikidata_qid y falta image_url, consultar Wikidata.",
    )
    parser.add_argument("--path", type=Path, default=STADIUM_OVERRIDES_PATH)
    args = parser.parse_args()

    if not args.path.is_file():
        log.error("No existe %s — ejecuta export_stadium_override_template primero.", args.path)
        return 1

    payload = json.loads(args.path.read_text(encoding="utf-8"))
    entries = payload.get("overrides") or []
    updated = skipped = 0

    with engine.connect() as conn:
        for entry in entries:
            lat, lon = entry.get("latitude"), entry.get("longitude")
            image_url = entry.get("image_url")
            qid = entry.get("wikidata_qid")
            wiki = entry.get("wikipedia_url")

            if args.fetch_images and not image_url:
                wd = resolve_stadium_image(
                    qid=str(qid) if qid else None,
                    team=entry.get("team") or "",
                    stadium_name=entry.get("stadium_name") or "",
                )
                image_url = image_url or wd.get("image_url")
                wiki = wiki or wd.get("wikipedia_url")
                if wd.get("wikidata_qid"):
                    qid = wd.get("wikidata_qid")
                if lat is None and wd.get("latitude") is not None:
                    lat, lon = wd.get("latitude"), wd.get("longitude")

            if lat is None or lon is None:
                skipped += 1
                continue

            team_id = _resolve_team_id(conn, entry)
            if not team_id:
                log.warning("Sin dim_team para %r", entry.get("team") or entry.get("match"))
                skipped += 1
                continue

            data = {
                "latitude": float(lat),
                "longitude": float(lon),
                "timezone": _derive_timezone(float(lat), float(lon)),
            }
            if qid:
                data["wikidata_qid"] = qid
            if image_url:
                data["image_url"] = image_url
            if wiki:
                data["wikipedia_url"] = wiki

            cols = [k for k in data if data[k] not in (None, "")]
            if args.dry_run:
                log.info("dry-run team_id=%s %s cols=%s", team_id, entry.get("team"), cols)
                updated += 1
                continue

            with engine.begin() as tx:
                n = _propagate_coords_to_team(tx, team_id, data, cols)
                stadium_name = entry.get("stadium_name")
                if image_url or qid or wiki or stadium_name:
                    tx.execute(
                        text("""
                            UPDATE dim_stadium
                            SET stadium_name = COALESCE(:stadium_name, stadium_name),
                                image_url = COALESCE(:image_url, image_url),
                                wikidata_qid = COALESCE(:wikidata_qid, wikidata_qid),
                                wikipedia_url = COALESCE(:wikipedia_url, wikipedia_url),
                                country = COALESCE(:country, country),
                                updated_at = NOW()
                            WHERE canonical_team_id = :tid
                        """),
                        {
                            "tid": team_id,
                            "stadium_name": stadium_name,
                            "image_url": image_url,
                            "wikidata_qid": qid,
                            "wikipedia_url": wiki,
                            "country": entry.get("country"),
                        },
                    )
            log.info("OK %s (%s rows)", entry.get("team"), n)
            updated += 1

    print(f"Done. updated={updated} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
