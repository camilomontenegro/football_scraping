"""
Sube una imagen de estadio a Cloudinary desde URL (p. ej. Wikimedia Commons)
y enlaza dim_stadium_master.

Uso:
    python -m scripts.upload_stadium_image_from_url --stadium-id 440 --url "https://..."
    python -m scripts.upload_stadium_image_from_url --stadium-id 662 --wikidata Q10272863
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

from loaders.common import engine
from scripts.sync_stadium_images_cloudinary import _slug, _team_tags, _transformed_url

log = logging.getLogger(__name__)


def _cloudinary_config() -> None:
    import cloudinary

    cloud = os.getenv("CLOUDINARY_CLOUD_NAME", "").strip()
    key = os.getenv("CLOUDINARY_API_KEY", "").strip()
    secret = os.getenv("CLOUDINARY_API_SECRET", "").strip()
    if not all([cloud, key, secret]):
        raise ValueError("Faltan CLOUDINARY_* en .env")
    cloudinary.config(cloud_name=cloud, api_key=key, api_secret=secret)


def _commons_from_wikidata(qid: str) -> str | None:
    headers = {"User-Agent": "football-scraping/1.0 (stadium-image-upload)"}
    r = requests.get(
        f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
        timeout=30,
        headers=headers,
    )
    r.raise_for_status()
    ent = r.json()["entities"][qid]
    for claim in ent.get("claims", {}).get("P18", []):
        filename = claim["mainsnak"]["datavalue"]["value"]
        return "https://commons.wikimedia.org/wiki/Special:FilePath/" + quote(filename)
    return None


def upload(
    stadium_id: int,
    image_url: str,
    *,
    dry_run: bool = False,
    prefix: str = "Estadios",
) -> None:
    with engine.begin() as conn:
        row = conn.execute(
            text("""
                SELECT stadium_id, canonical_name, country, cloudinary_public_id
                FROM dim_stadium_master WHERE stadium_id = :sid
            """),
            {"sid": stadium_id},
        ).mappings().one()

        name = row["canonical_name"]
        country = (row["country"] or "unknown").strip()
        country_slug = _slug(country) or country.replace(" ", "-")
        public_id = f"{prefix}/{country_slug}/{_slug(name)}"

        if dry_run:
            log.info("[DRY] sid=%s %r → %s from %s", stadium_id, name, public_id, image_url)
            return

        _cloudinary_config()
        import cloudinary.uploader

        tags = _team_tags(conn, stadium_id)
        cloudinary.uploader.upload(
            image_url,
            public_id=public_id,
            overwrite=True,
            resource_type="image",
            tags=tags,
        )
        url = _transformed_url(public_id)
        conn.execute(
            text("""
                UPDATE dim_stadium_master
                SET cloudinary_public_id = :pid, image_url = :url
                WHERE stadium_id = :sid
            """),
            {"sid": stadium_id, "pid": public_id, "url": url},
        )
    log.info("Subido sid=%s public_id=%s", stadium_id, public_id)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--stadium-id", type=int, required=True)
    p.add_argument("--url")
    p.add_argument("--wikidata")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--prefix", default="Estadios")
    args = p.parse_args()

    url = args.url
    if args.wikidata:
        qid = args.wikidata if args.wikidata.startswith("Q") else f"Q{args.wikidata}"
        url = _commons_from_wikidata(qid)
        if not url:
            raise SystemExit(f"Sin P18 en Wikidata {qid}")
        log.info("Commons: %s", url)

    if not url:
        raise SystemExit("Indica --url o --wikidata")

    upload(args.stadium_id, url, dry_run=args.dry_run, prefix=args.prefix)
    print("OK")


if __name__ == "__main__":
    main()
