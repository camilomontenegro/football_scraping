"""
scrapers/player_photos_scraper.py
===================================
Descarga fotos de jugadores desde Transfermarkt, las sube a Cloudinary
organizadas por competición y guarda la URL en dim_player.photo_url.

Estructura en Cloudinary:
    players/la_liga/{canonical_id}_{slug_nombre}
    players/champions_league/{canonical_id}_{slug_nombre}
    ...

Uso:
    python scrapers/player_photos_scraper.py --competition "La Liga"
    python scrapers/player_photos_scraper.py --competition "Champions League"
    python scrapers/player_photos_scraper.py --competition "La Liga" --limit 50
    python scrapers/player_photos_scraper.py --id 35662
    python scrapers/player_photos_scraper.py --list-competitions
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import cloudinary
import cloudinary.uploader
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from loaders.common import engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

TRANSFERMARKT_PROFILE_URL = "https://www.transfermarkt.es/player/profil/spieler/{id}"

HEADERS_TM = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.transfermarkt.es/",
}

DELAY_BETWEEN_REQUESTS = 1.5  # segundos entre peticiones (TM es más estricto)


def setup_cloudinary() -> None:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    cloudinary.config(
        cloud_name=os.environ["CLOUDINARY_CLOUD_NAME"],
        api_key=os.environ["CLOUDINARY_API_KEY"],
        api_secret=os.environ["CLOUDINARY_API_SECRET"],
        secure=True,
    )


def slugify(name: str) -> str:
    name = name.lower().strip()
    name = re.sub(r"[áàäâ]", "a", name)
    name = re.sub(r"[éèëê]", "e", name)
    name = re.sub(r"[íìïî]", "i", name)
    name = re.sub(r"[óòöô]", "o", name)
    name = re.sub(r"[úùüû]", "u", name)
    name = re.sub(r"[ñ]", "n", name)
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def request_with_retry(url: str, retries: int = 3) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS_TM, timeout=15)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            log.warning("[HTTP %d] intento %d/%d — %s", e.response.status_code, attempt + 1, retries, url)
        except requests.exceptions.ConnectionError:
            log.warning("[CONNECTION ERROR] intento %d/%d — %s", attempt + 1, retries, url)
        except requests.exceptions.Timeout:
            log.warning("[TIMEOUT] intento %d/%d — %s", attempt + 1, retries, url)
        except Exception as e:
            log.warning("[ERROR] intento %d/%d — %s: %s", attempt + 1, retries, type(e).__name__, e)
        time.sleep(2 ** (attempt + 1))
    log.error("[FALLIDO] Se agotaron los %d reintentos para %s", retries, url)
    return None


def fetch_tm_image_bytes(tm_id: int) -> bytes | None:
    """Visita la página del jugador en Transfermarkt, extrae la URL real de la foto y la descarga."""
    page_url = TRANSFERMARKT_PROFILE_URL.format(id=tm_id)
    resp = request_with_retry(page_url)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    img_tag = soup.find("img", class_="data-header__profile-image")
    if not img_tag:
        log.warning(f"  TM sin foto en página para id={tm_id}")
        return None
    img_url = img_tag.get("src") or img_tag.get("data-src")
    if not img_url or "silhouette" in img_url:
        return None
    img_resp = request_with_retry(img_url)
    if not img_resp:
        return None
    if img_resp.headers.get("Content-Type", "").startswith("image"):
        return img_resp.content
    return None


def upload_to_cloudinary(image_bytes: bytes, public_id: str, folder: str) -> str | None:
    try:
        result = cloudinary.uploader.upload(
            image_bytes,
            public_id=public_id,
            folder=folder,
            overwrite=True,
            resource_type="image",
        )
        return result["secure_url"]
    except Exception as e:
        log.warning(f"Error subiendo a Cloudinary: {e}")
        return None


def list_competitions() -> list[tuple]:
    query = """
        SELECT dc.canonical_name, COUNT(DISTINCT fe.player_id) as jugadores
        FROM fact_events fe
        JOIN dim_match dm ON fe.match_id = dm.match_id
        JOIN dim_competition dc ON dm.competition_id = dc.canonical_id
        GROUP BY dc.canonical_name
        ORDER BY jugadores DESC
    """
    with engine.connect() as conn:
        return conn.execute(text(query)).fetchall()


def get_players_by_competition(competition: str, limit: int | None) -> list[tuple]:
    query = """
        SELECT DISTINCT dp.canonical_id, dp.canonical_name, dp.id_transfermarkt
        FROM dim_player dp
        JOIN fact_events fe ON fe.player_id = dp.canonical_id
        JOIN dim_match dm ON fe.match_id = dm.match_id
        JOIN dim_competition dc ON dm.competition_id = dc.canonical_id
        WHERE dc.canonical_name = :competition
          AND dp.photo_url IS NULL
          AND dp.id_transfermarkt IS NOT NULL
        ORDER BY dp.canonical_id
    """
    if limit:
        query += f" LIMIT {limit}"
    with engine.connect() as conn:
        return conn.execute(text(query), {"competition": competition}).fetchall()


def get_single_player(player_id: int) -> list[tuple]:
    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE canonical_id = :id AND id_transfermarkt IS NOT NULL
    """
    with engine.connect() as conn:
        return conn.execute(text(query), {"id": player_id}).fetchall()


def update_photo_url(canonical_id: int, url: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE dim_player SET photo_url = :url WHERE canonical_id = :id"),
            {"url": url, "id": canonical_id},
        )


def process_player(canonical_id: int, canonical_name: str,
                   id_transfermarkt: int, cloudinary_folder: str) -> bool:
    image_bytes = fetch_tm_image_bytes(id_transfermarkt)
    if not image_bytes:
        log.warning(f"  Sin imagen: {canonical_name} (id={canonical_id})")
        return False

    public_id = f"{canonical_id}_{slugify(canonical_name)}"
    cloudinary_url = upload_to_cloudinary(image_bytes, public_id, cloudinary_folder)
    if not cloudinary_url:
        return False

    update_photo_url(canonical_id, cloudinary_url)
    log.info(f"  OK {canonical_name} → {cloudinary_url}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Scraper de fotos de jugadores por competición")
    parser.add_argument("--competition", type=str, default=None, help='Nombre de la competición, ej: "La Liga"')
    parser.add_argument("--limit", type=int, default=None, help="Máximo de jugadores a procesar")
    parser.add_argument("--id", type=int, default=None, dest="player_id", help="Procesar solo un jugador por canonical_id")
    parser.add_argument("--list-competitions", action="store_true", help="Listar competiciones disponibles y salir")
    args = parser.parse_args()

    if args.list_competitions:
        comps = list_competitions()
        print("\nCompeticiones disponibles:")
        for name, count in comps:
            print(f"  {count:>5} jugadores  →  {name}")
        return

    setup_cloudinary()

    if args.player_id:
        players = get_single_player(args.player_id)
        cloudinary_folder = "players/otros"
    elif args.competition:
        players = get_players_by_competition(args.competition, args.limit)
        cloudinary_folder = f"players/{slugify(args.competition)}"
    else:
        parser.error("Indica --competition, --id o --list-competitions")
        return

    total = len(players)
    log.info(f"Jugadores a procesar: {total} → carpeta Cloudinary: {cloudinary_folder}")

    ok, failed = 0, 0
    for i, (canonical_id, canonical_name, id_transfermarkt) in enumerate(players, 1):
        log.info(f"[{i}/{total}] {canonical_name}")
        success = process_player(canonical_id, canonical_name, id_transfermarkt, cloudinary_folder)
        if success:
            ok += 1
        else:
            failed += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)

    log.info(f"\nCompletado: {ok} subidas, {failed} fallidas de {total} jugadores")


if __name__ == "__main__":
    main()
