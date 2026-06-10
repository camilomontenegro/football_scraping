"""
Rellena image_url en dim_stadium vía Wikidata (P18), en lotes para evitar 429.

Muchos QID en stadium_overrides.json son de clubes, no estadios: este script
sigue P115 → estadio y usa P18 del venue.

Uso:
    python -m scripts.fetch_stadium_images --analyze
    python -m scripts.fetch_stadium_images
    python -m scripts.fetch_stadium_images --only-synthetic
    python -m scripts.fetch_stadium_images --from-overrides
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
    batch_resolve_stadium_images,
    resolve_stadium_image,
)

log = logging.getLogger(__name__)

SQL_MISSING = text("""
    SELECT s.stadium_id,
           s.wikidata_qid,
           s.stadium_name,
           s.team_slug,
           COALESCE(t.canonical_name, s.team_slug) AS team
    FROM dim_stadium s
    LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
    WHERE (s.image_url IS NULL OR TRIM(s.image_url) = '')
      AND (
          s.wikidata_qid IS NOT NULL
          OR s.stadium_name IS NOT NULL
          OR t.canonical_name IS NOT NULL
      )
    ORDER BY s.stadium_id
""")


def analyze() -> None:
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        with_img = conn.execute(text(
            "SELECT COUNT(*) FROM dim_stadium WHERE image_url IS NOT NULL AND TRIM(image_url) <> ''",
        )).scalar()
        pending = conn.execute(text(f"SELECT COUNT(*) FROM ({SQL_MISSING.text}) q")).scalar()

    print(f"Estadios total:     {total}")
    print(f"Con image_url:      {with_img}")
    print(f"Pendientes:         {pending}")


def _rows_from_overrides() -> list[dict]:
    if not STADIUM_OVERRIDES_PATH.is_file():
        return []
    payload = json.loads(STADIUM_OVERRIDES_PATH.read_text(encoding="utf-8"))
    rows: list[dict] = []
    with engine.connect() as conn:
        for entry in payload.get("overrides") or []:
            slug = entry.get("match")
            if not slug:
                continue
            row = conn.execute(
                text("""
                    SELECT s.stadium_id, s.wikidata_qid, s.stadium_name, s.team_slug,
                           t.canonical_name AS team
                    FROM dim_stadium s
                    JOIN dim_team t ON t.canonical_id = s.canonical_team_id
                    WHERE s.team_slug = :slug
                    LIMIT 1
                """),
                {"slug": slug},
            ).mappings().fetchone()
            if row:
                rows.append(dict(row))
    return rows


def fetch_images(
    *,
    limit: int | None = None,
    only_synthetic: bool = False,
    from_overrides: bool = False,
    dry_run: bool = False,
) -> int:
    with engine.connect() as conn:
        if from_overrides:
            rows = _rows_from_overrides()
        else:
            sql = SQL_MISSING.text
            if only_synthetic:
                sql = sql.replace(
                    "WHERE (s.image_url",
                    "WHERE s.data_source = 'synthetic-geocode' AND (s.image_url",
                )
            if limit:
                sql += " LIMIT :limit"
            rows = [
                dict(r)
                for r in conn.execute(text(sql), {"limit": limit} if limit else {}).mappings()
            ]

    if not rows:
        print("No hay estadios pendientes de imagen.")
        return 0

    log.info("Resolviendo imágenes para %d estadio(s)...", len(rows))
    resolved = batch_resolve_stadium_images(rows)

    updated = 0
    for row in rows:
        sid = row["stadium_id"]
        data = resolved.get(sid)
        if not data or not data.get("image_url"):
            log.warning("Sin imagen: %s (qid=%s)", row.get("team"), row.get("wikidata_qid"))
            continue
        if dry_run:
            log.info("dry-run stadium_id=%s -> %s", sid, data["image_url"][:70])
            updated += 1
            continue
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE dim_stadium
                    SET image_url = :image_url,
                        wikidata_qid = COALESCE(:wikidata_qid, wikidata_qid),
                        wikipedia_url = COALESCE(:wikipedia_url, wikipedia_url),
                        updated_at = NOW()
                    WHERE stadium_id = :sid
                """),
                {
                    "sid": sid,
                    "image_url": data["image_url"],
                    "wikidata_qid": data.get("wikidata_qid"),
                    "wikipedia_url": data.get("wikipedia_url"),
                },
            )
        updated += 1

    print(f"Imágenes aplicadas: {updated}/{len(rows)}")
    return updated


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Fetch stadium images from Wikidata.")
    parser.add_argument("--analyze", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-synthetic", action="store_true")
    parser.add_argument("--from-overrides", action="store_true")
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        help="Seconds between Wikidata API calls (default 6.0).",
    )
    args = parser.parse_args()

    if args.delay is not None:
        import scrapers.wikidata_stadium_enricher as _enricher
        _enricher._MIN_REQUEST_INTERVAL = max(1.0, float(args.delay))
        log.info("Wikidata request interval set to %.1fs", _enricher._MIN_REQUEST_INTERVAL)

    if args.analyze:
        analyze()
        return 0

    fetch_images(
        limit=args.limit,
        only_synthetic=args.only_synthetic,
        from_overrides=args.from_overrides,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
