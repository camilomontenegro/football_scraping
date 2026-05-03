"""
whoscored_scraper.py
====================
Scraper de WhoScored usando Selenium + BeautifulSoup.
Extrae eventos de partidos (pases, tiros, presiones, duelos...)
con coordenadas X,Y del objeto matchCentreData.

Guarda 4 CSVs en data/raw/whoscored/<competicion>/:
    - whoscored_events_<competicion>.csv
    - whoscored_matches_<competicion>.csv
    - whoscored_players_<competicion>.csv
    - whoscored_teams_<competicion>.csv

IMPORTANTE: WhoScored tiene protección anti-bot.
Si falla, prueba a poner HEADLESS = False para ver el navegador.
"""

import json
import os
import re
import time
import random
import logging
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from scripts.competitions import get_competition

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────

# URLs exactas de fixtures de WhoScored por competición y temporada
WHOSCORED_URLS = {
    "LaLiga": {
        "2020/2021": "https://es.whoscored.com/regions/206/tournaments/4/seasons/8321/stages/18851/fixtures/espa%C3%B1a-laliga-2020-2021",
        "2021/2022": "https://es.whoscored.com/regions/206/tournaments/4/seasons/8681/stages/19895/fixtures/espa%C3%B1a-laliga-2021-2022",
        "2022/2023": "https://es.whoscored.com/regions/206/tournaments/4/seasons/9149/stages/21073/fixtures/espa%C3%B1a-laliga-2022-2023",
        "2023/2024": "https://es.whoscored.com/regions/206/tournaments/4/seasons/9682/stages/22176/fixtures/espa%C3%B1a-laliga-2023-2024",
        "2024/2025": "https://es.whoscored.com/regions/206/tournaments/4/seasons/10317/stages/23401/fixtures/espa%C3%B1a-laliga-2024-2025",
        "2025/2026": "https://es.whoscored.com/regions/206/tournaments/4/seasons/10803/stages/24622/fixtures/espa%C3%B1a-laliga-2025-2026",
    },
    "UEFA Champions League": {
        "2020/2021": "https://es.whoscored.com/regions/250/tournaments/12/seasons/8177/stages/19130/fixtures/europe-champions-league-2020-2021",
        "2021/2022": "https://es.whoscored.com/regions/250/tournaments/12/seasons/8623/stages/20265/fixtures/europe-champions-league-2021-2022",
        "2022/2023": "https://es.whoscored.com/regions/250/tournaments/12/seasons/9086/stages/20969/fixtures/europe-champions-league-2022-2023",
        "2023/2024": "https://es.whoscored.com/regions/250/tournaments/12/seasons/9664/stages/22686/fixtures/europe-champions-league-2023-2024",
        "2024/2025": "https://es.whoscored.com/regions/250/tournaments/12/seasons/10456/stages/24083/fixtures/europe-champions-league-2024-2025",
        "2025/2026": "https://es.whoscored.com/regions/250/tournaments/12/seasons/10903/stages/24797/fixtures/europe-champions-league-2025-2026",
    }
}

DELAY_MIN = 3.0
DELAY_MAX = 6.0
HEADLESS = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "whoscored"


# ── DRIVER ─────────────────────────────────────────────────────────────────────

def create_driver() -> webdriver.Chrome:
    """Crea un driver de Chrome con configuración anti-detección."""
    options = Options()

    if HEADLESS:
        options.add_argument('--headless=new')

    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Bloquear imágenes y CSS para mayor rapidez
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheet": 2,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument(
        'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/136.0.0.0 Safari/537.36'
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.execute_cdp_cmd(
        'Page.addScriptToEvaluateOnNewDocument',
        {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'}
    )
    return driver


def random_sleep():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def accept_cookies(driver: webdriver.Chrome):
    try:
        cookie_btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Aceptar todo') or contains(text(), 'Accept all')]")
            )
        )
        cookie_btn.click()
        log.info("  Cookies aceptadas ✓")
        time.sleep(2)
    except Exception:
        pass


# ── BASE DE DATOS (INCREMENTAL) ──────────────────────────────────────────────

def get_existing_whoscored_match_ids() -> set:
    """Devuelve un conjunto con los IDs de WhoScored que ya están en la base de datos."""
    try:
        from sqlalchemy import text
        from loaders.common import engine
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id_whoscored FROM dim_match WHERE id_whoscored IS NOT NULL")).fetchall()
            return {str(r[0]) for r in rows}
    except Exception as e:
        log.warning("No se pudo consultar la BD para IDs existentes (¿DB apagada?): %s", e)
        return set()


# ── SCRAPING ───────────────────────────────────────────────────────────────────

def get_season_matches(driver: webdriver.Chrome, season_name: str, url: str) -> List[Dict]:
    """Obtiene la lista de IDs de partidos navegando por el calendario."""
    log.info("  Obteniendo partidos de temporada %s...", season_name)
    all_match_ids = set()

    try:
        driver.get(url)
        time.sleep(10)
        accept_cookies(driver)

        MONTHS_TO_NAVIGATE = 10  # Suficiente para Liga (Ago-May) o Champions (Sep-Jun)

        for month_idx in range(MONTHS_TO_NAVIGATE):
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(2)
        
            script_js = r'''
            var ids = [];
            var links = document.querySelectorAll('a[href*="/matches/"]');
            links.forEach(function(l) {
                var m = l.href.match(/\/matches\/(\d+)/i);
                if (m) ids.push(m[1]);
            });
            return [...new Set(ids)];
            '''
            ids = driver.execute_script(script_js)

            if ids:
                all_match_ids.update(ids)
                log.info("    Mes %d: %d partidos (total: %d)", month_idx + 1, len(ids), len(all_match_ids))
                
            try:
                prev_btn = driver.find_element(By.ID, "dayChangeBtn-prev")
                driver.execute_script("arguments[0].click();", prev_btn)
                time.sleep(4)
            except Exception:
                break

        matches = [{'whoscored_match_id': mid, 'season': season_name} for mid in all_match_ids]
        log.info("   ✓ %d partidos totales extraídos para %s", len(matches), season_name)
        return matches
    
    except Exception as e:
        log.error("  Error en temporada %s: %s", season_name, e)
        return []


def get_match_data(driver: webdriver.Chrome, match_id: str, season_name: str) -> dict:
    """Obtiene los datos de un partido desde matchCentreData."""
    url = f"https://es.whoscored.com/matches/{match_id}/live"

    try:
        driver.get(url)
        random_sleep()
        accept_cookies(driver)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        script = soup.find('script', string=re.compile('matchCentreData'))
        if not script:
            return {}

        pattern = r'matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n'
        m = re.search(pattern, script.string, re.DOTALL)
        if not m:
            return {}

        data = json.loads(m.group(1))
        data['whoscored_match_id'] = match_id
        data['season'] = season_name
        return data

    except Exception as e:
        log.error("  Error extrayendo matchCentreData para partido %s: %s", match_id, e)
        return {}


# ── TRANSFORMACIÓN ─────────────────────────────────────────────────────────────

def extract_events(match_data: dict) -> list[dict]:
    match_id = match_data.get('whoscored_match_id')
    season   = match_data.get('season')
    events   = match_data.get('events', [])

    result = []
    for e in events:
        try:
            x, y = e.get('x'), e.get('y')
            end_x, end_y = e.get('endX'), e.get('endY')

            result.append({
                'whoscored_match_id':  match_id,
                'whoscored_event_id':  e.get('id'),
                'whoscored_player_id': e.get('playerId'),
                'whoscored_team_id':   e.get('teamId'),
                'player_name':         e.get('playerName'),
                'event_type':          e.get('type', {}).get('displayName') if isinstance(e.get('type'), dict) else e.get('type'),
                'period':              e.get('period', {}).get('displayName') if isinstance(e.get('period'), dict) else e.get('period'),
                'minute':              e.get('minute'),
                'second':              e.get('second'),
                'x':                   round(float(x) / 100, 4) if x is not None else None,
                'y':                   round(float(y) / 100, 4) if y is not None else None,
                'end_x':               round(float(end_x) / 100, 4) if end_x is not None else None,
                'end_y':               round(float(end_y) / 100, 4) if end_y is not None else None,
                'outcome':             e.get('outcomeType', {}).get('displayName') if isinstance(e.get('outcomeType'), dict) else e.get('outcomeType'),
                'season':              season,
                'source':              'whoscored',
            })
        except Exception:
            continue
    return result


def extract_players_from_match(match_data: dict) -> list[dict]:
    players = []
    for side in ('home', 'away'):
        team_data = match_data.get(side, {})
        team_id   = team_data.get('teamId')
        team_name = team_data.get('name')
        for p in team_data.get('players', []):
            players.append({
                'whoscored_player_id': p.get('playerId'),
                'player_name':         p.get('name'),
                'whoscored_team_id':   team_id,
                'team_name':           team_name,
                'position':            p.get('position'),
                'shirt_number':        p.get('shirtNo'),
            })
    return players


def extract_teams_from_match(match_data: dict) -> list[dict]:
    teams = []
    for side in ('home', 'away'):
        team_data = match_data.get(side, {})
        if team_data.get('teamId'):
            teams.append({
                'whoscored_team_id': team_data.get('teamId'),
                'team_name':         team_data.get('name'),
            })
    return teams


# ── ORQUESTADOR PRINCIPAL ──────────────────────────────────────────────────────

def scrape_whoscored(competition: str, season: str = None, from_date: str = None, match_ids: list = None):
    """
    Ejecuta el scraper de WhoScored.
    
    Args:
        competition: Nombre de la competición (ej: "La Liga")
        season:      Temporada a scrapear (ej: "2024/2025"). Si None, scrapea todas.
        from_date:   (Ignorado por eficiencia, en su lugar verificamos la BD)
        match_ids:   Lista de IDs específicos a descargar (opcional).
    """
    # 1. Obtener URLs de la competición
    comp_config = get_competition(competition)
    comp_name = comp_config["name"] if comp_config else competition
    urls_dict = WHOSCORED_URLS.get(comp_name)
    
    if not urls_dict:
        log.error("WhoScored no tiene URLs configuradas para '%s'", comp_name)
        return

    seasons_to_scrape = {season: urls_dict[season]} if season and season in urls_dict else urls_dict

    # 2. Configurar directorio de salida
    comp_slug = comp_name.lower().replace(" ", "-")
    
    # Si se especifica temporada, crear subcarpeta estandarizada
    if season:
        folder_season = season.replace("/", "_")
        out_dir = BASE_OUTPUT_DIR / comp_slug / f"season={folder_season}"
    else:
        out_dir = BASE_OUTPUT_DIR / comp_slug
        
    os.makedirs(out_dir, exist_ok=True)

    all_matches, all_events, all_players, all_teams = [], [], [], []
    driver = create_driver()

    try:
        log.info("Iniciando navegador para WhoScored...")
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        # 3. Obtener partidos que ya tenemos en BD (incremental)
        existing_ids = get_existing_whoscored_match_ids()
        if existing_ids:
            log.info("[UPDATE] %d partidos de WhoScored ya están en BD. Se omitirán.", len(existing_ids))

        for season_name, url in seasons_to_scrape.items():
            log.info("\n📅 Temporada %s", season_name)

            if match_ids:
                matches = [{'whoscored_match_id': str(mid), 'season': season_name} for mid in match_ids]
            else:
                matches = get_season_matches(driver, season_name, url)

            if not matches:
                continue

            all_matches.extend(matches)
            
            # 4. Procesar solo los nuevos
            for i, match in enumerate(matches, 1):
                mid = str(match['whoscored_match_id'])
                
                # ¡Magia incremental! Si ya existe, saltamos la descarga de Selenium (que es lenta)
                if mid in existing_ids and not match_ids:
                    log.info("  [%d/%d] Partido %s ya en BD -> OMITIDO", i, len(matches), mid)
                    continue

                log.info("  [%d/%d] Partido %s -> DESCARGANDO", i, len(matches), mid)

                match_data = get_match_data(driver, mid, season_name)
                if not match_data or 'events' not in match_data:
                    log.warning("    No se encontraron eventos")
                    continue

                events = extract_events(match_data)
                all_events.extend(events)
                all_players.extend(extract_players_from_match(match_data))
                all_teams.extend(extract_teams_from_match(match_data))

                log.info("    -> %d eventos guardados", len(events))

            log.info("  ✓ Temporada %s completa", season_name)

    except Exception as e:
        log.error("Error fatal en WhoScored: %s", e)
    finally:
        driver.quit()
        log.info("Driver cerrado.")

    # 5. Guardar CSVs
    if not all_matches:
        log.warning("No se extrajeron datos de WhoScored.")
        return

    df_matches = pd.DataFrame(all_matches)
    df_events = pd.DataFrame(all_events)
    df_players = pd.DataFrame(all_players).drop_duplicates(subset=['whoscored_player_id']) if all_players else pd.DataFrame()
    df_teams = pd.DataFrame(all_teams).drop_duplicates(subset=['whoscored_team_id']) if all_teams else pd.DataFrame()

    df_matches.to_csv(out_dir / f"whoscored_matches.csv", index=False)
    if not df_events.empty:
        df_events.to_csv(out_dir / f"whoscored_events.csv", index=False)
    if not df_players.empty:
        df_players.to_csv(out_dir / f"whoscored_players.csv", index=False)
    if not df_teams.empty:
        df_teams.to_csv(out_dir / f"whoscored_teams.csv", index=False)

    log.info("\n✅ Scraping WhoScored finalizado")
    log.info("  Partidos encontrados: %d", len(df_matches))
    log.info("  Eventos nuevos:       %d", len(df_events))
    log.info("  Archivos en: %s", out_dir)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scraper de WhoScored")
    parser.add_argument("--competition", "-c", type=str, default="La Liga", help="Competición (ej: La Liga, UEFA Champions League)")
    parser.add_argument("--season", "-s", type=str, default=None, help="Temporada (ej: 2024/2025). Si no, todas.")
    args = parser.parse_args()
    
    scrape_whoscored(competition=args.competition, season=args.season)
