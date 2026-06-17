
"""
scrapers/transfer_value_scraper.py
====================================
Descarga el historial de valor de mercado y fichajes de cada jugador desde
la API interna de Transfermarkt y genera CSVs con todos los datos.
 
Guarda los JSON en disco para uso futuro y mantiene una caché para evitar
reprocesar jugadores ya descargados.
 
Estructura de archivos generados:
    data/raw/players/market_value/{id_transfermarkt}.json  ← JSON valor de mercado por jugador
    data/raw/players/transfers/{id_transfermarkt}.json       ← JSON fichajes por jugador
    data/raw/players/market_value.csv                      ← CSV con todos los hitos de valor
    data/raw/players/transfers.csv                           ← CSV con todos los fichajes
    data/.cache/transfer_value_scraper.json                  ← caché de progreso
    logs/transfer_value_scraper.log                          ← log de ejecución
 
Uso:
    # Descargar todo (market value + transfers)
    python -m scrapers.transfer_value_scraper
 
    # Descargar solo los primeros 50 jugadores
    python -m scrapers.transfer_value_scraper --limit 50
 
    # Descargar un jugador concreto por canonical_id
    python -m scrapers.transfer_value_scraper --id 583
 
    # Forzar re-descarga aunque ya estén en caché
    python -m scrapers.transfer_value_scraper --force
 
    # Solo descargar market value (saltar transfers)
    python -m scrapers.transfer_value_scraper --skip-transfers
 
    # Solo descargar transfers (saltar market value)
    python -m scrapers.transfer_value_scraper --skip-mv
 
    # Solo regenerar CSVs desde los JSON ya descargados (sin peticiones)
    python -m scrapers.transfer_value_scraper --transform-only

     # Regenerar solo market_value.csv (sin peticiones)
    python -m scrapers.transfer_value_scraper --transform-only --skip-transfers

    # Regenerar solo transfers.csv (sin peticiones)
    python -m scrapers.transfer_value_scraper --transform-only --skip-mv
 
    # Simular ejecución sin hacer peticiones ni escribir nada
    python -m scrapers.transfer_value_scraper --dry-run
"""
 
from __future__ import annotations
 
import argparse
import json
import logging
import random
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional
 
import pandas as pd
import requests
from sqlalchemy import text
 
from loaders.common import engine
from utils.season_utils import normalize_season
 
# ── Logger ────────────────────────────────────────────────────────────────────
 
# instancia del logger para este módulo
log = logging.getLogger(__name__)
 
# ── Constantes ────────────────────────────────────────────────────────────────
 
DELAY_MIN   = 2.0   # pausa mínima entre peticiones (segundos)
DELAY_MAX   = 4.0   # pausa máxima entre peticiones (segundos)
MAX_RETRIES = 3     # número máximo de reintentos por petición
 
# número de jugadores procesados antes de hacer flush de la caché a disco
# evita perder todo el progreso si el proceso se interrumpe
COMMIT_BATCH = 50
 
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}
 
# Raíz del proyecto — sube dos niveles desde scrapers/transfer_value_scraper.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent
 
# URL de la API interna de Transfermarkt para el historial de valor de mercado
# {id} es el placeholder que se sustituye por el id_transfermarkt del jugador
# Ejemplo: https://www.transfermarkt.es/ceapi/marketValueDevelopment/graph/640428
TM_MV_URL = "https://www.transfermarkt.es/ceapi/marketValueDevelopment/graph/{id}"
 
# URL de la API interna de Transfermarkt para el historial de fichajes
# Ejemplo: https://www.transfermarkt.es/ceapi/transferHistory/list/640428
TM_TRANSFERS_URL = "https://www.transfermarkt.es/ceapi/transferHistory/list/{id}"
 
# Rutas de salida — market value
RAW_MV_DIR   = PROJECT_ROOT / "data" / "raw" / "players" / "market_value"
MV_CSV_PATH  = PROJECT_ROOT / "data" / "clean" / "market_value" / "market_value.csv"
 
# Rutas de salida — transfers
RAW_TRANSFERS_DIR   = PROJECT_ROOT / "data" / "raw" / "players" / "transfers"
TRANSFERS_CSV_PATH  = PROJECT_ROOT / "data" / "clean" / "tranfers" / "transfers.csv"
 
# Ruta de la caché de progreso
CACHE_PATH = PROJECT_ROOT / "data" / ".cache" / "transfer_value_scraper.json"

 
# ── Logging ───────────────────────────────────────────────────────────────────
 
def _setup_logging() -> None:
    """
    Configura el logging para escribir en consola y en archivo.
    Crea la carpeta logs/ si no existe y genera transfer_value_scraper.log.
 
    Ejemplo de salida en disco:
        logs/transfer_value_scraper.log
    """
    log_path = PROJECT_ROOT / "logs" / "transfer_value_scraper.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
 
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)
 
 
# ── Caché ─────────────────────────────────────────────────────────────────────
 
def _load_cache() -> dict:
    """
    Carga la caché de progreso desde disco.
    La caché registra qué jugadores ya han sido procesados para evitar
    repetir peticiones en ejecuciones posteriores.
    Tiene flags separados para market value y transfers.
 
    Ejemplo de estructura:
        {
            "640428": {
                "canonical_name": "Eduardo Camavinga",
                "mv_scraped": true,
                "transfers_scraped": true,
                "last_scraped": "2026-06-03T12:00:00"
            }
        }
 
    Devuelve:
        dict con la caché, o dict vacío si no existe el archivo
    """
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Error cargando caché: %s — se inicia caché vacía", e)
        return {}
 
 
def _save_cache(cache: dict) -> None:
    """
    Guarda la caché de progreso en disco.
    Se llama cada COMMIT_BATCH jugadores y al finalizar.
 
    Parámetros:
        cache (dict): diccionario con el estado de progreso por jugador
    """
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
 
 
# ── Helpers ───────────────────────────────────────────────────────────────────
 
def parse_date(date_str: str) -> Optional[date]:
    """
    Convierte una cadena de fecha en un objeto date de Python.
    En Transfermarkt las fechas pueden aparecer en distintos formatos.
 
    Ejemplos:
        parse_date("03/06/2019") → date(2019, 6, 3)   ← formato dd/mm/yyyy
        parse_date("2021-08-31") → date(2021, 8, 31)  ← formato yyyy-mm-dd (dateUnformatted)
        parse_date("-")          → None
        parse_date("")           → None
 
    Parámetros:
        date_str (str): cadena de fecha
 
    Devuelve:
        date con la fecha, o None si la cadena está vacía o no tiene formato reconocido
    """
    if not date_str or date_str.strip() in ("-", ""):
        return None
 
    # normaliza separadores a '/' para unificar formatos
    date_str = date_str.strip().replace(".", "/").replace("-", "/")
 
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
 
    return None
 
 
def request_with_retry(url: str, retries: int = MAX_RETRIES) -> requests.Response | None:
    """
    Hace una petición GET con reintentos y backoff exponencial.
    Reintenta ante errores HTTP, de conexión o timeout.
 
    Backoff exponencial: espera 2s, 4s, 8s entre reintentos.
 
    Parámetros:
        url     (str): URL a descargar
        retries (int): número máximo de intentos (por defecto MAX_RETRIES)
 
    Devuelve:
        requests.Response si tiene éxito, None si agota los reintentos
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
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
 
        # espera exponencial antes del siguiente intento: 2s, 4s, 8s...
        time.sleep(2 ** (attempt + 1))
 
    log.error("[FALLIDO] Se agotaron los %d reintentos para %s", retries, url)
    return None
 
 
def _append_to_csv(records: list[dict], path: Path) -> None:
    """
    Añade una lista de registros a un CSV de forma incremental.
    Si el archivo no existe lo crea con cabecera.
    Si ya existe añade las filas sin repetir la cabecera.
 
    Parámetros:
        records (list[dict]): lista de diccionarios a guardar
        path    (Path):       ruta del archivo CSV destino
    """
    if not records:
        return
    df = pd.DataFrame(records)
    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, mode="a", index=False, header=write_header,encoding="utf-8-sig")
    log.info("%d registros guardados en %s", len(records), path.name)


def _extract_team_id_from_wappen(wappen: str) -> int | None:
    """
    Extrae el id_transfermarkt del equipo desde la URL del escudo (wappen).
    El ID está embebido en la URL con el patrón /wappen/profil/{id}.

    Ejemplos:
        "https://tmssl.akamaized.net//images/wappen/profil/418.png"           → 418
        "https://tmssl.akamaized.net//images/wappen/profil/418_1729684474.png" → 418
        ""  → None

    Parámetros:
        wappen (str): URL del escudo del equipo

    Devuelve:
        int con el id_transfermarkt del equipo, o None si no se encuentra
    """
    if not wappen:
        return None
    match_object = re.search(r"/wappen/profil/(\d+)", wappen)
    return int(match_object.group(1)) if match_object else None
 
def _extract_team_id_from_href(href: str) -> int | None:
    """
    Extrae el id_transfermarkt de un equipo desde el href del JSON de transfers.
    El ID está embebido en la URL con el patrón /verein/{id}.
 
    Ejemplo:
        "/stade-rennes/transfers/verein/273/saison_id/2021" → 273
        "/real-madrid/transfers/verein/418/saison_id/2021"  → 418
 
    Parámetros:
        href (str): href del equipo en el JSON de transfers
 
    Devuelve:
        int con el id_transfermarkt del equipo, o None si no se encuentra
    """
    if not href:
        return None
    match_object = re.search(r"/verein/(\d+)", href)
    return int(match_object.group(1)) if match_object else None


def _extract_euros_from_html(texto: str) -> Optional[int]:
    """
    Extrae el valor numérico en euros de un string que puede contener HTML embebido.
    Tranfermarkt usa este formato para cesiones con coste y fines de cesión con coste:
        "Coste de cesión:<br /><i class="normaler-text">100 mil €</i>"
        "Fin de cesión<br /><i class="normaler-text">900 mil €</i>"

    Primero elimina las etiquetas HTML y luego busca el patrón numérico.

    Ejemplos confirmados en los JSON:
        "Coste de cesión:<br /><i ...>100 mil €</i>"  → 100000
        "Coste de cesión:<br /><i ...>2,70 mill. €</i>" → 2700000
        "Fin de cesión<br /><i ...>900 mil €</i>"      → 900000

    Parámetros:
        texto (str): string con posible HTML embebido

    Devuelve:
        int con el valor en euros, o None si no se encuentra
    """
    # elimina etiquetas HTML: <br />, <i class="...">, </i>
    sin_html = re.sub(r"<[^>]+>", "", texto)

    # busca número seguido de unidad: "100 mil €" o "2,70 mill. €"
    coincidencia = re.search(r"([\d,.]+)\s*(mill|mio|mil|k)", sin_html)
    if not coincidencia:
        return None

    # convierte el número al formato decimal estándar
    # formato europeo: punto = separador de miles, coma = decimal
    # "2,70" → elimina puntos de miles → "2,70" → coma por punto → "2.70" → 2.7
    numero_str = coincidencia.group(1).replace(".", "").replace(",", ".")
    try:
        numero = float(numero_str)
    except ValueError:
        return None

    unidad = coincidencia.group(2).lower()
    if unidad in ("mill", "mio"):
        return int(numero * 1_000_000)
    if unidad in ("mil", "k"):
        return int(numero * 1_000)

    return None


def parse_fee(fee_raw: Optional[str]) -> tuple[Optional[int], str]:
    """
    Parsea el campo 'fee' del JSON de transfers de Transfermarkt y devuelve
    el coste en euros y el tipo de operación.

    Patrones confirmados en los datos reales (data/raw/players/transfers/):
        "-"                                              → (None, "unknown")
        "?"                                              → (None, "unknown")
        "Libre"                                          → (0, "free")
        "Cesión"                                         → (None, "loan")
        "Coste de cesión:<br /><i ...>100 mil €</i>"    → (100000, "loan")
        "Fin de cesión"                                  → (None, "end_of_loan")
        "Fin de cesión<br /><i ...>900 mil €</i>"       → (900000, "end_of_loan")
        "1,50 mill. €"                                   → (1500000, "transfer")
        "300 mil €"                                      → (300000, "transfer")
        "draft"                                          → (None, "unknown")

    NOTA: el formato numérico es europeo (coma decimal, punto de miles)
    porque la URL usada es transfermarkt.es.

    Parámetros:
        fee_raw (str | None): texto original del campo fee del JSON

    Devuelve:
        tuple: (fee_euros, transfer_type)
            fee_euros     (int | None): coste en euros, None si no aplica, 0 si es libre
            transfer_type (str):        'transfer', 'loan', 'end_of_loan', 'free', 'unknown'
    """
    if not fee_raw:
        return None, "unknown"

    # limpia el string — minúsculas y sin espacios extremos
    # elimina tildes para comparar sin problemas de encoding
    # "Cesión" → "cesion", "Fin de cesión" → "fin de cesion"
    clean = (fee_raw.strip().lower()
             .replace("á", "a").replace("é", "e")
             .replace("í", "i").replace("ó", "o").replace("ú", "u"))

    # sin datos o desconocido — confirmados: "-", "?", "draft"
    if clean in ("-", "?", "draft", ""):
        return None, "unknown"

    # fichaje libre — coste 0
    # confirmado: "Libre"
    if "libre" in clean or "free" in clean:
        return 0, "free"

    # fin de cesión — puede incluir coste con HTML embebido
    # confirmados: "Fin de cesión", "Fin de cesión<br /><i ...>900 mil €</i>"
    if "fin de ces" in clean:
        coste = _extract_euros_from_html(fee_raw)
        return coste, "end_of_loan"

    # cesión con coste — HTML embebido con el precio
    # confirmado: "Coste de cesión:<br /><i class="normaler-text">100 mil €</i>"
    if "coste de ces" in clean:
        coste = _extract_euros_from_html(fee_raw)
        return coste, "loan"

    # cesión sin coste
    # confirmado: "Cesión"
    if "cesion" in clean:
        return None, "loan"

    # traspaso con coste numérico — formatos confirmados:
    # "1,50 mill. €", "300 mil €", "88 mil €", "1 mil €"
    coste = _extract_euros_from_html(fee_raw)
    if coste is not None:
        return coste, "transfer"

    # fallback — no encaja en ningún patrón conocido
    return None, "unknown"

# ── Consultas a la base de datos ──────────────────────────────────────────────
 
def get_players(limit: int | None) -> list[tuple]:
    """
    Obtiene la lista de jugadores con id_transfermarkt no nulo desde dim_player.
    A partir de los id_transfermarkt se construye la URL para obtener el historial
    de valor de mercado y fichajes de cada jugador.
 
    Parámetros:
        limit (int | None): número máximo de jugadores a devolver.
                            None devuelve todos.
 
    Devuelve:
        list[tuple]: lista de tuplas (canonical_id, canonical_name, id_transfermarkt)
    """
    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE id_transfermarkt IS NOT NULL
        ORDER BY canonical_id
    """
    if limit:
        query += f" LIMIT {limit}"
 
    try:
        with engine.connect() as conn:
            return conn.execute(text(query)).fetchall()
    except Exception as e:
        log.error("Error al obtener jugadores de la BD: %s", e)
        return []
 
 
def get_single_player(player_id: int) -> list[tuple]:
    """
    Obtiene un jugador concreto de dim_player por su canonical_id.
    Útil para procesar o re-procesar un jugador específico sin lanzar
    el proceso completo.
 
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
    try:
        with engine.connect() as conn:
            return conn.execute(text(query), {"id": player_id}).fetchall()
    except Exception as e:
        log.error("Error al obtener jugador con canonical_id %d: %s", player_id, e)
        return []
 
def get_team_map() -> dict:
    """
    Obtiene un diccionario de equipos desde dim_team indexado por id_transfermarkt.
    Se usa para añadir el canonical_id y canonical_name del equipo al CSV de transfers
    cuando el equipo ya existe en la BD con id_transfermarkt conocido.

    Nota: no todos los equipos en dim_team tienen id_transfermarkt — solo se
    incluyen los que sí lo tienen. El loader resolverá el resto.

    Ejemplo de estructura devuelta:
        {
            273: {"canonical_id": 45, "canonical_name": "Stade Rennais"},
            418: {"canonical_id": 12, "canonical_name": "Real Madrid"},
        }

    Devuelve:
        dict: {id_transfermarkt (int): {"canonical_id": int, "canonical_name": str}}
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_transfermarkt, canonical_id, canonical_name
                FROM dim_team
                WHERE id_transfermarkt IS NOT NULL
            """)).fetchall()
        return {row[0]: {"canonical_id": row[1], "canonical_name": row[2]} for row in rows}
    
    except Exception as e:
        log.error("Error al obtener mapa de equipos: %s", e)
        return {}
 
 
# ── Descarga y extracción — Market Value ──────────────────────────────────────
 
def fetch_and_save_mv_json(id_transfermarkt: int) -> dict | None:
    """
    Descarga el historial de valor de mercado de un jugador desde la API de Transfermarkt
    y guarda el JSON en disco para uso futuro sin necesidad de volver a descargarlo.
 
    URL de la API:
        https://www.transfermarkt.es/ceapi/marketValueDevelopment/graph/{id_transfermarkt}
 
    Ejemplo de salida en disco:
        data/raw/players/transfer_value/640428.json
 
    Parámetros:
        id_transfermarkt (int): ID del jugador en Transfermarkt, ej: 640428
 
    Devuelve:
        dict con los datos del JSON, o None si falla la petición
    """
    url = TM_MV_URL.format(id=id_transfermarkt)
    response = request_with_retry(url)
    if not response:
        return None
 
    # guarda el texto de la respuesta directamente en disco sin parse + serialización
    json_path = RAW_MV_DIR / f"{id_transfermarkt}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(response.text, encoding="utf-8")
 
    return response.json()
 
 
def extract_mv_to_csv(
        canonical_id: int,
        canonical_name: str,
        id_transfermarkt: int, 
        data: dict,
        team_map:dict
    ) -> None:
    """
    Extrae los hitos de valor de mercado del JSON y los añade al CSV transfer_value.csv.
    Por cada entrada en data["list"] genera un registro con el valor numérico (campo "y"),
    la fecha del hito normalizada, el equipo en ese momento y la edad del jugador.
 
    El valor numérico se extrae de "y" (entero en euros) y no de "mw" (string formateado).
 
    Ejemplo de entrada (un elemento de data["list"]):
        {
            "x": 1559512800000,           ← timestamp Unix en ms (no se usa)
            "y": 4000000,                 ← valor en euros ← SE USA ESTE
            "mw": "4,00 mill. €",         ← valor formateado (no se usa)
            "datum_mw": "03/06/2019",     ← fecha del hito
            "verein": "Stade Rennais FC", ← equipo en ese momento
            "age": "16"                   ← edad del jugador
        }
 
    Ejemplo de salida (una fila del CSV):
        canonical_id, canonical_name, id_transfermarkt, market_value, value_date, club_name, age
        1234, Eduardo Camavinga, 640428, 4000000, 2019-06-03, Stade Rennais FC, 16
 
    Parámetros:
        canonical_id     (int):  canonical_id del jugador en dim_player
        canonical_name   (str):  nombre del jugador
        id_transfermarkt (int):  ID del jugador en Transfermarkt
        data             (dict): JSON descargado de la API
        team_map         (dict): Mapeo de IDs de equipos en Transfermarkt a sus canonical_ids
    """
    records = []
    last_id_tm_club = None  # propaga el id_tm_club cuando wappen viene vacío

    for entry in data.get("list", []):
        # extrae el id_tm_club del wappen — solo viene en el primer hito de cada equipo
        id_tm_club = _extract_team_id_from_wappen(entry.get("wappen", ""))

        if id_tm_club:
            # nuevo equipo — actualiza el último id_tm_club visto
            last_id_tm_club = id_tm_club
        else:
            # wappen vacío — propaga el id_tm_club del hito anterior
            id_tm_club = last_id_tm_club
        
        # busca el canonical_id del club si el club tiene id_transfermarkt conocido 
        club_canonical_id = team_map.get(id_tm_club, {}).get("canonical_id") if id_tm_club else None

        records.append({
            "canonical_id":      canonical_id,
            "canonical_name":    canonical_name,
            "id_transfermarkt":  id_transfermarkt,
            "market_value":      entry["y"],
            "value_date":        parse_date(entry["datum_mw"]),
            "club_name":         entry["verein"],
            "age":               entry["age"],
            "id_tm_club":        id_tm_club,
            "club_canonical_id": club_canonical_id,
        })
    _append_to_csv(records, MV_CSV_PATH)
 
 
# ── Descarga y extracción — Transfers ─────────────────────────────────────────
 
def fetch_and_save_transfers_json(id_transfermarkt: int) -> dict | None:
    """
    Descarga el historial de fichajes de un jugador desde la API de Transfermarkt
    y guarda el JSON en disco para uso futuro sin necesidad de volver a descargarlo.
 
    URL de la API:
        https://www.transfermarkt.es/ceapi/transferHistory/list/{id_transfermarkt}
 
    Ejemplo de salida en disco:
        data/raw/players/transfers/640428.json
 
    Parámetros:
        id_transfermarkt (int): ID del jugador en Transfermarkt, ej: 640428
 
    Devuelve:
        dict con los datos del JSON, o None si falla la petición
    """
    url = TM_TRANSFERS_URL.format(id=id_transfermarkt)
    response = request_with_retry(url)
    if not response:
        return None
 
    json_path = RAW_TRANSFERS_DIR / f"{id_transfermarkt}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(response.text, encoding="utf-8")
 
    return response.json()
 
 
def extract_transfers_to_csv(
    canonical_id: int,
    canonical_name: str,
    id_transfermarkt: int,
    data: dict,
    team_map: dict,
) -> None:
    """
    Extrae el historial de fichajes del JSON y los añade al CSV transfers.csv.
    Por cada entrada en data["transfers"] genera un registro con la temporada
    normalizada, fecha exacta del fichaje, equipos origen y destino con sus IDs,
    datos económicos del traspaso y tipo de operación.

    La temporada se normaliza a formato 'YYYY/YYYY' usando normalize_season.
    La fecha se normaliza a objeto date usando parse_date.
    El id_transfermarkt del equipo se extrae del href usando _extract_team_id_from_href.
    El canonical_id del equipo se obtiene del team_map si existe en dim_team.
    El coste y tipo de operación se parsean de fee_raw usando parse_fee.

    Ejemplo de entrada (un elemento de data["transfers"]):
        {
            "season": "21/22",
            "dateUnformatted": "2021-08-31",
            "from": {"clubName": "Stade Rennes", "href": "/stade-rennes/transfers/verein/273/..."},
            "to":   {"clubName": "Real Madrid",  "href": "/real-madrid/transfers/verein/418/..."},
            "fee": "31,00 mill. €"
        }

    Ejemplo de salida (una fila del CSV):
        canonical_id, canonical_name, id_transfermarkt, season, transfer_date,
        from_team_name, from_team_id_tm, from_team_canonical_id, from_team_canonical_name,
        to_team_name, to_team_id_tm, to_team_canonical_id, to_team_canonical_name,
        fee_raw, fee_euros, transfer_type, is_loan, fee_currency

    Parámetros:
        canonical_id     (int):  canonical_id del jugador en dim_player
        canonical_name   (str):  nombre del jugador
        id_transfermarkt (int):  ID del jugador en Transfermarkt
        data             (dict): JSON descargado de la API
        team_map         (dict): {id_transfermarkt: {"canonical_id": int, "canonical_name": str}}
    """
    transfers = data.get("transfers", [])
    if not transfers:
        log.debug("Sin fichajes para jugador tm_id=%d", id_transfermarkt)
        return

    records = []
    for t in transfers:
        if not isinstance(t, dict):
            continue

        # temporada normalizada a formato YYYY/YYYY
        season = normalize_season(t.get("season"))

        # fecha exacta del fichaje — viene en formato ISO yyyy-mm-dd
        transfer_date = parse_date(t.get("dateUnformatted") or t.get("date"))

        # equipo origen
        from_obj            = t.get("from") or {}
        from_name           = from_obj.get("clubName")
        from_href           = from_obj.get("href", "")
        from_id_tm          = _extract_team_id_from_href(from_href)
        from_canonical_id   = team_map.get(from_id_tm, {}).get("canonical_id")   if from_id_tm else None
        from_canonical_name = team_map.get(from_id_tm, {}).get("canonical_name") if from_id_tm else None

        # equipo destino
        to_obj            = t.get("to") or {}
        to_name           = to_obj.get("clubName")
        to_href           = to_obj.get("href", "")
        to_id_tm          = _extract_team_id_from_href(to_href)
        to_canonical_id   = team_map.get(to_id_tm, {}).get("canonical_id")   if to_id_tm else None
        to_canonical_name = team_map.get(to_id_tm, {}).get("canonical_name") if to_id_tm else None

        # coste del traspaso — texto original del JSON
        fee_raw = t.get("fee")

        # parsea el coste en euros y el tipo de operación desde fee_raw
        # parse_fee maneja todos los formatos confirmados en los datos reales:
        # "31,00 mill. €" → (31000000, "transfer")
        # "Libre"         → (0, "free")
        # "Cesión"        → (None, "loan")
        # "Coste de cesión:<br /><i ...>100 mil €</i>" → (100000, "loan")
        # "Fin de cesión" → (None, "end_of_loan")
        # "-", "?"        → (None, "unknown")
        fee_euros, transfer_type = parse_fee(fee_raw)

        # is_loan se infiere del transfer_type — loan y end_of_loan son cesiones
        is_loan = transfer_type in ("loan", "end_of_loan")

        if not from_name and not to_name:
            continue

        records.append({
            "canonical_id":             canonical_id,
            "canonical_name":           canonical_name,
            "id_transfermarkt":         id_transfermarkt,
            "season":                   season,
            "transfer_date":            transfer_date,
            "from_team_name":           from_name,
            "from_team_id_tm":          from_id_tm,
            "from_team_canonical_id":   from_canonical_id,
            "from_team_canonical_name": from_canonical_name,
            "to_team_name":             to_name,
            "to_team_id_tm":            to_id_tm,
            "to_team_canonical_id":     to_canonical_id,
            "to_team_canonical_name":   to_canonical_name,
            "fee_raw":                  fee_raw,
            "fee_euros":                fee_euros,
            "transfer_type":            transfer_type,
            "is_loan":                  is_loan,
            "fee_currency":             "€" if fee_euros is not None else None,
        })

    _append_to_csv(records, TRANSFERS_CSV_PATH)
 
# ── Regeneración de CSVs desde JSON ──────────────────────────────────────────
 
def build_mv_csv_from_raw() -> None:
    """
    Regenera el CSV market_value.csv leyendo todos los JSON ya descargados en disco.
    Útil cuando el CSV se ha corrompido o se quiere cambiar el formato sin
    volver a hacer peticiones a Transfermarkt.

    Lee todos los archivos en data/raw/players/market_value/*.json
    Carga el team_map desde la BD para resolver canonical_id de equipos.

    Uso:
        python -m scrapers.transfer_value_scraper --transform-only
    """
    players_map = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_transfermarkt, canonical_id, canonical_name
                FROM dim_player
                WHERE id_transfermarkt IS NOT NULL
            """)).fetchall()
        players_map = {row[0]: (row[1], row[2]) for row in rows}
    except Exception as e:
        log.warning("No se pudo cargar el mapeo de jugadores: %s", e)

    if not RAW_MV_DIR.exists():
        log.warning("No existe la carpeta %s — no hay JSON que procesar", RAW_MV_DIR)
        return

    if MV_CSV_PATH.exists():
        MV_CSV_PATH.unlink()
        log.info("CSV eliminado para regeneración: %s", MV_CSV_PATH)

    # carga el team_map para resolver canonical_id de equipos
    team_map = get_team_map()

    json_files = sorted(RAW_MV_DIR.glob("*.json"))
    log.info("Regenerando market value CSV desde %d archivos JSON...", len(json_files))

    for json_path in json_files:
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", json_path.name, e)
            continue

        id_transfermarkt = int(json_path.stem)
        # obtiene canonical_id y canonical_name desde el mapeo de la BD
        canonical_id, canonical_name = players_map.get(id_transfermarkt, (None, None))
        extract_mv_to_csv(canonical_id, canonical_name, id_transfermarkt, data, team_map)

    log.info("Market value CSV regenerado: %s", MV_CSV_PATH)
    
 
def build_transfers_csv_from_raw() -> None:
    """
    Regenera el CSV transfers.csv leyendo todos los JSON ya descargados en disco.
    Útil cuando el CSV se ha corrompido o se quiere cambiar el formato sin
    volver a hacer peticiones a Transfermarkt.
 
    Lee todos los archivos en data/raw/players/transfers/*.json
    Carga el team_map desde la BD para resolver canonical_id de equipos.
 
    Uso:
        python -m scrapers.transfer_value_scraper --transform-only
    """

    players_map = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_transfermarkt, canonical_id, canonical_name
                FROM dim_player
                WHERE id_transfermarkt IS NOT NULL
            """)).fetchall()
        players_map = {row[0]: (row[1], row[2]) for row in rows}
    except Exception as e:
        log.warning("No se pudo cargar el mapeo de jugadores: %s", e)
    


    if not RAW_TRANSFERS_DIR.exists():
        log.warning("No existe la carpeta %s — no hay JSON que procesar", RAW_TRANSFERS_DIR)
        return
 
    if TRANSFERS_CSV_PATH.exists():
        TRANSFERS_CSV_PATH.unlink()
        log.info("CSV eliminado para regeneración: %s", TRANSFERS_CSV_PATH)
 
    # carga el team_map para resolver canonical_id de equipos
    team_map = get_team_map()
    json_files = sorted(RAW_TRANSFERS_DIR.glob("*.json"))
    log.info("Regenerando transfers CSV desde %d archivos JSON...", len(json_files))
 
    for json_path in json_files:
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", json_path.name, e)
            continue
 
        id_transfermarkt = int(json_path.stem)
        # obtiene canonical_id y canonical_name desde el mapeo de la BD
        canonical_id, canonical_name = players_map.get(id_transfermarkt, (None, None))
       

        extract_transfers_to_csv(canonical_id, canonical_name, id_transfermarkt, data, team_map)
 
    log.info("Transfers CSV regenerado: %s", TRANSFERS_CSV_PATH)
 
 
# ── Orquestador ───────────────────────────────────────────────────────────────
 
def scrape_players(
    players: list[tuple],
    cache: dict,
    force: bool = False,
    dry_run: bool = False,
    skip_mv: bool = False,
    skip_transfers: bool = False,
) -> dict:
    """
    Orquesta la descarga del historial de valor de mercado y fichajes
    para una lista de jugadores.
 
    Gestión de caché:
        - Flags separados para market value (mv_scraped) y transfers (transfers_scraped)
        - Si el jugador ya tiene el flag activo y force=False → se salta ese proceso
        - Si force=True → se re-descarga aunque esté en caché
        - Cada COMMIT_BATCH jugadores guarda la caché a disco por si se interrumpe
 
    Gestión de dry_run:
        - Si dry_run=True → solo loguea lo que haría sin hacer peticiones ni escribir
 
    Parámetros:
        players        (list[tuple]): lista de (canonical_id, canonical_name, id_transfermarkt)
        cache          (dict):        caché de progreso cargada desde disco
        force          (bool):        si True re-descarga aunque esté en caché
        dry_run        (bool):        si True no hace peticiones ni escribe nada
        skip_mv        (bool):        si True omite la descarga de market value
        skip_transfers (bool):        si True omite la descarga de transfers
 
    Devuelve:
        dict con estadísticas separadas para mv y transfers
    """
    total   = len(players)
    stats = {
        "total":              total,
        "mv_ok":              0,
        "mv_failed":          0,
        "mv_skipped":         0,
        "transfers_ok":       0,
        "transfers_failed":   0,
        "transfers_skipped":  0,
    }
    processed_since_flush = 0
 
    # carga el team_map una sola vez para todos los jugadores
    team_map = get_team_map() if not (skip_mv and skip_transfers) else {}
    log.info("Jugadores a procesar: %d (force=%s, dry_run=%s, skip_mv=%s, skip_transfers=%s)",
             total, force, dry_run, skip_mv, skip_transfers)
 
    try:
        for i, (canonical_id, canonical_name, id_transfermarkt) in enumerate(players, 1):
            cache_key    = str(id_transfermarkt)
            cached       = cache.get(cache_key, {})
            mv_done      = cached.get("mv_scraped") and not force
            transfers_done = cached.get("transfers_scraped") and not force
 
            log.info("[%d/%d] %s (tm_id=%d)", i, total, canonical_name, id_transfermarkt)
 
            # ── Market Value ──
            if not skip_mv:
                if mv_done:
                    log.debug("  MV ya en caché — saltando")
                    stats["mv_skipped"] += 1
                elif dry_run:
                    log.info("  [dry-run] se descargaría MV: %s", TM_MV_URL.format(id=id_transfermarkt))
                else:
                    data = fetch_and_save_mv_json(id_transfermarkt)
                    if data:
                        extract_mv_to_csv(canonical_id, canonical_name, id_transfermarkt, data,team_map)
                        stats["mv_ok"] += 1
                        cached["mv_scraped"] = True
                    else:
                        stats["mv_failed"] += 1
                        log.warning("  Falló la descarga de MV para %s", canonical_name)
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
            # ── Transfers ──
            if not skip_transfers:
                if transfers_done:
                    log.debug("  Transfers ya en caché — saltando")
                    stats["transfers_skipped"] += 1
                elif dry_run:
                    log.info("  [dry-run] se descargarían transfers: %s",
                             TM_TRANSFERS_URL.format(id=id_transfermarkt))
                else:
                    data = fetch_and_save_transfers_json(id_transfermarkt)
                    if data:
                        extract_transfers_to_csv(
                            canonical_id, canonical_name, id_transfermarkt, data, team_map
                        )
                        stats["transfers_ok"] += 1
                        cached["transfers_scraped"] = True
                    else:
                        stats["transfers_failed"] += 1
                        log.warning("  Falló la descarga de transfers para %s", canonical_name)
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
 
            if not dry_run:
                # actualiza la caché con el estado del jugador
                cached["canonical_name"] = canonical_name
                cached["last_scraped"]   = datetime.now().isoformat()
                cache[cache_key]         = cached
                processed_since_flush   += 1
 
                # guarda la caché a disco cada COMMIT_BATCH jugadores
                if processed_since_flush >= COMMIT_BATCH:
                    _save_cache(cache)
                    log.info("  >> Caché guardada (%d jugadores procesados)",
                             stats["mv_ok"] + stats["transfers_ok"])
                    processed_since_flush = 0
 
    except KeyboardInterrupt:
        # guarda el progreso si el usuario interrumpe con Ctrl+C
        log.warning("Interrumpido por el usuario — guardando caché...")
        if not dry_run:
            _save_cache(cache)
 
    # flush final de la caché
    if not dry_run and processed_since_flush > 0:
        _save_cache(cache)
 
    return stats
 
 
# ── Punto de entrada ──────────────────────────────────────────────────────────
 
def main() -> None:
    """
    Punto de entrada del scraper de valor de mercado y fichajes.
 
    Argumentos:
        --limit N          : procesa solo los primeros N jugadores
        --id N             : procesa un jugador concreto por canonical_id
        --force            : re-descarga aunque el jugador ya esté en caché
        --skip-mv          : omite la descarga de market value
        --skip-transfers   : omite la descarga de fichajes
        --transform-only   : solo regenera los CSVs desde los JSON ya descargados
        --dry-run          : simula la ejecución sin hacer peticiones ni escribir
 
    El modo dry-run sirve para comprobar cuántos jugadores se van a procesar,
    cuáles están en caché y cuáles no, sin gastar tiempo ni arriesgar bloqueos.
 
    Uso:
        python -m scrapers.transfer_value_scraper
        python -m scrapers.transfer_value_scraper --limit 50
        python -m scrapers.transfer_value_scraper --id 583
        python -m scrapers.transfer_value_scraper --force
        python -m scrapers.transfer_value_scraper --skip-transfers
        python -m scrapers.transfer_value_scraper --skip-mv
        python -m scrapers.transfer_value_scraper --transform-only
        python -m scrapers.transfer_value_scraper --dry-run
    """
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
    )
    _setup_logging()
 
    parser = argparse.ArgumentParser(
        description="Scraper de historial de valor de mercado y fichajes (Transfermarkt)"
    )
    parser.add_argument("--limit",           type=int,  default=None, help="Máximo de jugadores a procesar")
    parser.add_argument("--id",              type=int,  default=None, dest="player_id", help="Procesar un jugador por canonical_id")
    parser.add_argument("--force",           action="store_true", help="Re-descargar aunque esté en caché")
    parser.add_argument("--skip-mv",         action="store_true", help="Omitir descarga de market value")
    parser.add_argument("--skip-transfers",  action="store_true", help="Omitir descarga de fichajes")
    parser.add_argument("--transform-only",  action="store_true", help="Solo regenerar CSVs desde JSON existentes")
    parser.add_argument("--dry-run",         action="store_true", help="Simular sin hacer peticiones ni escribir")
    args = parser.parse_args()
 
    # modo transform-only: regenera los CSVs sin hacer peticiones
    if args.transform_only:
        log.info("Modo transform-only — regenerando CSVs desde JSON existentes")
        if not args.skip_mv:
            build_mv_csv_from_raw()
        if not args.skip_transfers:
            build_transfers_csv_from_raw()
        return
 
    # carga la caché — si force=True se ignora iniciando una caché vacía
    cache = {} if args.force else _load_cache()
    if args.force:
        log.info("--force activado — se ignorará la caché")
 
    # obtiene los jugadores a procesar
    if args.player_id:
        players = get_single_player(args.player_id)
    else:
        players = get_players(args.limit)
 
    if not players:
        log.error("No se encontraron jugadores — verifica la conexión a la BD")
        return
 
    # lanza el proceso
    stats = scrape_players(
        players, cache,
        force=args.force,
        dry_run=args.dry_run,
        skip_mv=args.skip_mv,
        skip_transfers=args.skip_transfers,
    )
 
    # resumen final
    print(f"\n{'=' * 50}")
    print(f"  Total jugadores:           {stats['total']}")
    print(f"  Market Value descargados:  {stats['mv_ok']}")
    print(f"  Market Value fallidos:     {stats['mv_failed']}")
    print(f"  Market Value en caché:     {stats['mv_skipped']}")
    print(f"  Transfers descargados:     {stats['transfers_ok']}")
    print(f"  Transfers fallidos:        {stats['transfers_failed']}")
    print(f"  Transfers en caché:        {stats['transfers_skipped']}")
    print(f"  CSV market value:          {MV_CSV_PATH}")
    print(f"  CSV transfers:             {TRANSFERS_CSV_PATH}")
    print(f"{'=' * 50}")
 
 
if __name__ == "__main__":
    main()