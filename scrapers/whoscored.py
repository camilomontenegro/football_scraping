"""
scrapers/whoscored.py
=====================
Scraper de WhoScored usando Selenium (Chrome).

WhoScored bloquea headless Chrome de forma fiable, por eso se usa Chrome
en modo NO headless (visible). Requiere Chrome instalado.

Extrae el objeto `matchCentreData` incrustado en el HTML de la página
de partido (Match Centre).

Funciones puras — sin acceso a DB.
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

log = logging.getLogger(__name__)

BASE_URL = "https://www.whoscored.com"


# ─────────────────────────────────────────────────────
# DRIVER
# ─────────────────────────────────────────────────────

def build_driver(headless: bool = False) -> webdriver.Chrome:
    """
    Crea un driver de Chrome.
    headless=False por defecto — WhoScored bloquea headless Chrome.
    """
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    # Enmascarar navigator.webdriver
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"},
    )
    return driver


# ─────────────────────────────────────────────────────
# EXTRACCIÓN
# ─────────────────────────────────────────────────────

def _extract_match_centre_data(page_source: str) -> Optional[dict]:
    """
    Extrae el blob JSON `matchCentreData` del source HTML de WhoScored.
    WhoScored lo incrusta como:
        matchCentreData = {...};
    """
    for pattern in [
        r"matchCentreData\s*=\s*(\{.+?\});\s*\n",
        r"matchCentreData\s*=\s*(\{.+?\})\s*;",
    ]:
        m = re.search(pattern, page_source, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError as exc:
                log.warning("Error parseando matchCentreData: %s", exc)
                return None
    return None


def fetch_match_data(driver: webdriver.Chrome, match_id: int) -> Optional[dict]:
    """
    Navega al Match Centre de WhoScored y extrae los datos del partido.
    Retorna el dict `matchCentreData` o None si no se encuentra.
    """
    url = f"{BASE_URL}/Matches/{match_id}/Live"
    log.info("GET %s", url)
    driver.get(url)

    try:
        WebDriverWait(driver, 25).until(
            lambda d: "matchCentreData" in d.page_source or "matchHeader" in d.page_source
        )
    except Exception:
        log.warning("Timeout esperando matchCentreData para match %d", match_id)

    return _extract_match_centre_data(driver.page_source)
