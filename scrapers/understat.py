"""
scrapers/understat.py
=====================
Wrapper síncrono sobre la API pública de Understat.
No usa librería de terceros: hace requests directos al endpoint JSON
incrustado en el HTML (la misma técnica que understatapi).

Funciones puras — sin acceso a DB.
"""
from __future__ import annotations

import json
import logging
import re
import time

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://understat.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": BASE_URL,
}

# Mapa de nombres de ligas en Understat
LEAGUE_MAP = {
    "laliga": "La_Liga",
    "la_liga": "La_Liga",
    "premier": "EPL",
    "epl": "EPL",
    "bundesliga": "Bundesliga",
    "serie_a": "Serie_A",
    "ligue_1": "Ligue_1",
}


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def _request(url: str, retries: int = 3, delay: float = 2.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except Exception as exc:
            log.warning("Intento %d/%d fallido para %s: %s", attempt + 1, retries, url, exc)
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


def _extract_json_var(html: str, var_name: str) -> list | dict | None:
    """
    Understat incrusta datos como:
        var shotsData = JSON.parse( '\x7B ...' );
    Extrae y devuelve el objeto Python equivalente.
    """
    # Regex flexible para capturar el contenido de JSON.parse
    pattern = rf"var\s+{var_name}\s*=\s*JSON\.parse\(\s*'(.+?)'\s*\)"
    m = re.search(pattern, html, re.DOTALL)
    if not m:
        return None
        
    raw_escaped = m.group(1)
    try:
        # Decodificar escapes hex (\xHH) y otros
        decoded = raw_escaped.encode("utf-8").decode("unicode_escape")
        return json.loads(decoded)
    except Exception as exc:
        log.warning("Error decodificando %s: %s", var_name, exc)
        return None


# ─────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────

def get_league_matches(league: str, season: str) -> list[dict]:
    """
    Devuelve lista de partidos de una liga/temporada.
    `league`: nombre Understat, e.g. 'La_Liga'
    `season`: año inicio, e.g. '2020'
    """
    league_key = LEAGUE_MAP.get(league.lower(), league)
    url = f"{BASE_URL}/league/{league_key}/{season}"
    
    # Intentar obtener via JSON.parse primero (más rápido)
    r = _request(url)
    if r:
        data = _extract_json_var(r.text, "datesData")
        if data:
            return data if isinstance(data, list) else []

    # Fallback: Selenium para raspar los links de la tabla si el JSON no está
    log.info("Buscando partidos via Selenium para %s %s...", league, season)
    from scrapers.sofascore import create_driver
    from selenium.webdriver.common.by import By
    import time

    driver = create_driver()
    try:
        driver.get(url)
        time.sleep(3)
        # Buscar todos los links que contengan /match/
        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/match/')]")
        matches = []
        seen_ids = set()
        for link in links:
            href = link.get_attribute("href")
            match_id = re.search(r"/match/(\d+)", href)
            if match_id:
                mid = match_id.group(1)
                if mid not in seen_ids:
                    # Estructura mínima compatible
                    matches.append({"id": mid})
                    seen_ids.add(mid)
        return matches
    finally:
        driver.quit()


def get_match_shots(match_id: str | int, retries: int = 3, use_selenium: bool = False) -> list[dict]:
    """
    Devuelve lista de shots para un partido.
    Combina home (h) y away (a).
    """
    url = f"{BASE_URL}/match/{match_id}"
    html = ""

    if use_selenium:
        from scrapers.sofascore import create_driver
        driver = create_driver()
        try:
            driver.get(url)
            import time
            time.sleep(2)
            html = driver.page_source
        finally:
            driver.quit()
    else:
        r = _request(url, retries=retries)
        if r:
            html = r.text

    if not html:
        log.warning("Sin HTML para match %s", match_id)
        return []

    shots_data = _extract_json_var(html, "shotsData")
    if not shots_data or not isinstance(shots_data, dict):
        log.warning("No se encontró shotsData para match %s", match_id)
        if not use_selenium:
            return get_match_shots(match_id, retries, use_selenium=True)
        return []

    shots = shots_data.get("h", []) + shots_data.get("a", [])
    return shots
