"""
scrapers/player_photos_sin_eventos_scraper.py
===============================================
Descarga fotos de jugadores que NO aparecen en fact_events (plantillas,
reservas, etc.), las sube a Cloudinary en players/sin_eventos/ y guarda
la URL en dim_player.photo_url.

Uso:
    python scrapers/player_photos_sin_eventos_scraper.py
    python scrapers/player_photos_sin_eventos_scraper.py --limit 100
    python scrapers/player_photos_sin_eventos_scraper.py --id 35662
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
CLOUDINARY_FOLDER = "players/sin_eventos"

HEADERS_TM = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://www.transfermarkt.es/",
}

DELAY_BETWEEN_REQUESTS = 1.5


def setup_cloudinary() -> None:
    """
    Configura el cliente de Cloudinary con las credenciales del .env.
    Debe llamarse antes de cualquier operación de subida.
    Lee CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY y CLOUDINARY_API_SECRET.
    """

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
    """
    Convierte un nombre de jugador a un slug válido para Cloudinary.
    Elimina tildes, caracteres especiales y sustituye espacios por guiones bajos.

    Ejemplo:
        slugify("Éder Militão") → "eder_militao"
        slugify("Arda Güler")   → "arda_guler"

    Parámetros:
        name (str): nombre del jugador

    Devuelve:
        str con el slug normalizado
    """
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
    """
    Hace una petición GET con reintentos y backoff exponencial.
    Reintenta ante errores HTTP, de conexión o timeout.

    Parámetros:
        url     (str): URL a descargar
        retries (int): número máximo de intentos (por defecto 3)

    Devuelve:
        requests.Response si tiene éxito, None si agota los reintentos
    """
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
    """
    Accede al perfil de Transfermarkt del jugador y descarga la imagen en memoria.
    Hace dos peticiones: una a la página del perfil y otra a la URL de la imagen.
    Si la imagen es una silueta genérica devuelve None.

    URL perfil:
        https://www.transfermarkt.es/player/profil/spieler/{tm_id}

    Parámetros:
        tm_id (int): ID del jugador en Transfermarkt, ej: 401530

    Devuelve:
        bytes con el contenido de la imagen, o None si no se encuentra
    """
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

def upload_to_cloudinary(image_bytes: bytes, public_id: str) -> str | None:
    """
    Sube una imagen en bytes a Cloudinary y devuelve la URL segura.
    Las imágenes se guardan en la carpeta definida en CLOUDINARY_FOLDER.
    Si ya existe una imagen con el mismo public_id la sobreescribe.

    Ejemplo de URL devuelta:
        https://res.cloudinary.com/{cloud}/image/upload/players/sin_eventos/1_lionel_messi.jpg

    Parámetros:
        image_bytes (bytes): contenido binario de la imagen
        public_id   (str):   identificador único en Cloudinary, ej: "1_lionel_messi"

    Devuelve:
        str con la URL segura de Cloudinary, o None si falla la subida
    """
    try:
        result = cloudinary.uploader.upload(
            image_bytes,
            public_id=public_id,
            folder=CLOUDINARY_FOLDER,
            overwrite=True,
            resource_type="image",
        )
        return result["secure_url"]
    except Exception as e:
        log.warning(f"Error subiendo a Cloudinary: {e}")
        return None


def get_players_sin_eventos(limit: int | None) -> list[tuple]:
    """
    Obtiene de dim_player los jugadores que no tienen eventos registrados
    y que aún no tienen foto en Cloudinary (photo_url IS NULL).
    Requiere que id_transfermarkt no sea NULL para poder acceder al perfil.

    Parámetros:
        limit (int | None): número máximo de jugadores a devolver.
                            None devuelve todos.

    Devuelve:
        list[tuple]: lista de (canonical_id, canonical_name, id_transfermarkt)

    """
    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE photo_url IS NULL
          AND id_transfermarkt IS NOT NULL
          AND canonical_id NOT IN (SELECT DISTINCT player_id FROM fact_events)
        ORDER BY canonical_id
    """
    if limit:
        query += f" LIMIT {limit}"
    with engine.connect() as conn:
        return conn.execute(text(query)).fetchall()


def get_single_player(player_id: int) -> list[tuple]:
    """
    Obtiene un jugador concreto de dim_player por su canonical_id.
    Útil para procesar o re-procesar un jugador específico.

    Parámetros:
        player_id (int): canonical_id del jugador en dim_player

    Devuelve:
        list[tuple]: lista con un elemento (canonical_id, canonical_name, id_transfermarkt)
                     o lista vacía si no se encuentra
    """

    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE canonical_id = :id AND id_transfermarkt IS NOT NULL
    """
    with engine.connect() as conn:
        return conn.execute(text(query), {"id": player_id}).fetchall()


def update_photo_url(canonical_id: int, url: str) -> None:
    """
    Actualiza el campo photo_url en dim_player con la URL de Cloudinary.
    Hace commit inmediato — si el proceso se interrumpe las fotos ya
    subidas no se pierden.

    Parámetros:
        canonical_id (int): canonical_id del jugador en dim_player
        url          (str): URL segura de Cloudinary
    """
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE dim_player SET photo_url = :url WHERE canonical_id = :id"),
            {"url": url, "id": canonical_id},
        )


def process_player(canonical_id: int, canonical_name: str, id_transfermarkt: int) -> bool:
    """
    Orquesta el proceso completo para un jugador:
        1. Descarga la imagen de Transfermarkt
        2. Sube a Cloudinary
        3. Guarda la URL en dim_player

    Parámetros:
        canonical_id     (int): canonical_id del jugador en dim_player
        canonical_name   (str): nombre del jugador para el log y el public_id
        id_transfermarkt (int): ID del jugador en Transfermarkt

    Devuelve:
        True si el proceso completó correctamente, False si falló en algún paso
    """
    image_bytes = fetch_tm_image_bytes(id_transfermarkt)
    if not image_bytes:
        log.warning(f"  Sin imagen: {canonical_name} (id={canonical_id})")
        return False

    public_id = f"{canonical_id}_{slugify(canonical_name)}"
    cloudinary_url = upload_to_cloudinary(image_bytes, public_id)
    if not cloudinary_url:
        return False

    update_photo_url(canonical_id, cloudinary_url)
    log.info(f"  OK {canonical_name} → {cloudinary_url}")
    return True


def main():
    """
    Punto de entrada del scraper de fotos.

    Argumentos:
        --limit N  : procesa solo los primeros N jugadores sin foto
        --id N     : procesa un jugador concreto por canonical_id

    Uso:
        python -m scrapers.player_photos_sin_eventos_scraper
        python -m scrapers.player_photos_sin_eventos_scraper --limit 100
        python -m scrapers.player_photos_sin_eventos_scraper --id 583
    """
    
    parser = argparse.ArgumentParser(description="Scraper de fotos de jugadores sin eventos")
    parser.add_argument("--limit", type=int, default=None, help="Máximo de jugadores a procesar")
    parser.add_argument("--id", type=int, default=None, dest="player_id", help="Procesar un jugador por canonical_id")
    args = parser.parse_args()

    setup_cloudinary()

    if args.player_id:
        players = get_single_player(args.player_id)
    else:
        players = get_players_sin_eventos(args.limit)

    total = len(players)
    log.info(f"Jugadores sin eventos a procesar: {total} → carpeta Cloudinary: {CLOUDINARY_FOLDER}")

    ok, failed = 0, 0
    for i, (canonical_id, canonical_name, id_transfermarkt) in enumerate(players, 1):
        log.info(f"[{i}/{total}] {canonical_name}")
        success = process_player(canonical_id, canonical_name, id_transfermarkt)
        if success:
            ok += 1
        else:
            failed += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)

    log.info(f"\nCompletado: {ok} subidas, {failed} fallidas de {total} jugadores")


if __name__ == "__main__":
    main()
