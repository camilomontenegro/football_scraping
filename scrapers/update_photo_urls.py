"""
scrapers/update_photo_urls.py
==============================
Actualiza dim_player.photo_url desde photo_urls.csv (compañeros / Cloudinary).

El CSV no va al repo: por defecto se lee desde el backup en Desktop.

Uso:
    python -m scrapers.update_photo_urls
    python -m scrapers.update_photo_urls --csv "C:\\ruta\\photo_urls.csv"
    python -m scrapers.update_photo_urls --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

DEFAULT_BACKUP_CSV = Path(
    r"C:/Users/ivanm/Desktop/football_scraping_backup/data/reference/photo_urls.csv"
)


def resolve_csv_path(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    if DEFAULT_BACKUP_CSV.exists():
        return DEFAULT_BACKUP_CSV
    legacy = Path(__file__).resolve().parent.parent / "db" / "migrations" / "photo_urls.csv"
    return legacy


def main() -> None:
    parser = argparse.ArgumentParser(description="Actualiza photo_url en dim_player desde CSV")
    parser.add_argument("--csv", type=Path, default=None, help="Ruta a photo_urls.csv")
    parser.add_argument("--dry-run", action="store_true", help="Sin escribir en la BD")
    args = parser.parse_args()

    csv_path = resolve_csv_path(args.csv)
    if not csv_path.exists():
        log.error("No se encuentra el archivo: %s", csv_path)
        sys.exit(1)

    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("photo_url") or "").strip()
            if not url:
                continue
            rows.append({"id": int(row["canonical_id"]), "url": url})

    log.info("CSV: %s", csv_path)
    log.info("URLs a importar: %d", len(rows))

    if args.dry_run:
        log.info("[DRY-RUN] No se escribirá nada en la BD")
        for r in rows[:5]:
            log.info("  canonical_id=%s → %s", r["id"], r["url"])
        log.info("  ...")
        return

    updated = 0
    with engine.begin() as conn:
        for r in rows:
            result = conn.execute(
                text(
                    "UPDATE dim_player SET photo_url = :url "
                    "WHERE canonical_id = :id"
                ),
                {"url": r["url"], "id": r["id"]},
            )
            updated += result.rowcount

    log.info("Completado: %d jugadores actualizados", updated)


if __name__ == "__main__":
    main()
