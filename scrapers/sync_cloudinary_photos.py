"""
scrapers/sync_cloudinary_photos.py
====================================
Sincroniza dim_player.photo_url con las fotos subidas a Cloudinary
por otros miembros del equipo.

Consulta la carpeta players/sin_eventos/ de Cloudinary, extrae el
canonical_id del nombre del archivo y actualiza dim_player.photo_url.

Uso:
    python scrapers/sync_cloudinary_photos.py
    python scrapers/sync_cloudinary_photos.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import cloudinary
import cloudinary.api
from dotenv import load_dotenv
from sqlalchemy import text

from loaders.common import engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

FOLDER = "players/sin_eventos"


def setup_cloudinary() -> None:
    load_dotenv()
    cloudinary.config(
        cloud_name=os.environ["CLOUDINARY_CLOUD_NAME"],
        api_key=os.environ["CLOUDINARY_API_KEY"],
        api_secret=os.environ["CLOUDINARY_API_SECRET"],
        secure=True,
    )


def get_all_cloudinary_resources() -> list[dict]:
    resources = []
    next_cursor = None
    while True:
        params = {"type": "upload", "prefix": FOLDER, "max_results": 500, "resource_type": "image"}
        if next_cursor:
            params["next_cursor"] = next_cursor
        result = cloudinary.api.resources(**params)
        resources.extend(result.get("resources", []))
        next_cursor = result.get("next_cursor")
        if not next_cursor:
            break
    return resources


def extract_canonical_id(public_id: str) -> int | None:
    filename = public_id.split("/")[-1]
    try:
        return int(filename.split("_")[0])
    except (ValueError, IndexError):
        return None


def main():
    parser = argparse.ArgumentParser(description="Sync photo_url desde Cloudinary players/sin_eventos a la BD")
    parser.add_argument("--dry-run", action="store_true", help="Muestra qué actualizaría sin escribir en la BD")
    args = parser.parse_args()

    setup_cloudinary()

    log.info(f"Consultando Cloudinary en {FOLDER} ...")
    resources = get_all_cloudinary_resources()
    log.info(f"Encontrados {len(resources)} archivos")

    updated, skipped, not_found = 0, 0, 0

    for resource in resources:
        canonical_id = extract_canonical_id(resource["public_id"])
        if canonical_id is None:
            continue

        if args.dry_run:
            log.info(f"  [DRY-RUN] canonical_id={canonical_id} → {resource['secure_url']}")
            updated += 1
            continue

        with engine.begin() as conn:
            result = conn.execute(
                text("UPDATE dim_player SET photo_url = :url WHERE canonical_id = :id AND photo_url IS NULL"),
                {"url": resource["secure_url"], "id": canonical_id},
            )
            if result.rowcount > 0:
                updated += 1
            else:
                skipped += 1

    log.info(f"\nCompletado: {updated} actualizadas, {skipped} ya tenían foto o no existen")


if __name__ == "__main__":
    main()
