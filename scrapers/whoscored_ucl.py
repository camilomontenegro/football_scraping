"""
whoscored_scraper.py
====================
Scraper de WhoScored usando Selenium + BeautifulSoup.
Extrae eventos de partidos (pases, tiros, presiones, duelos...)
con coordenadas X,Y del objeto matchCentreData.

La Champions League â€” temporadas 2020/21 hasta 2025/26 â€” todos los equipos.

Guarda 4 CSVs en data/raw/whoscored/:
    - whoscored_events_ucl.csv   â†’ todos los eventos con coordenadas
    - whoscored_matches_ucl.csv  â†’ partidos con IDs
    - whoscored_players_ucl.csv  â†’ jugadores con IDs de WhoScored
    - whoscored_teams_ucl.csv    â†’ equipos con IDs de WhoScored

IMPORTANTE: WhoScored tiene protecciÃ³n anti-bot.
Si falla, prueba a poner HEADLESS = False para ver el navegador.
"""

import json
import os
import re
import time
import random
import logging

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# CONFIGURACIÃóN #

# URLs exactas de fixtures de La Champions por temporada 
SEASON_URLS = {
    "2020/21": "https://www.whoscored.com/regions/250/tournaments/12/seasons/8177/stages/19130/fixtures/europe-champions-league-2020-2021",
    "2021/22": "https://www.whoscored.com/regions/250/tournaments/12/seasons/8623/stages/20265/fixtures/europe-champions-league-2021-2022",
    "2022/23": "https://www.whoscored.com/regions/250/tournaments/12/seasons/9086/stages/20969/fixtures/europe-champions-league-2022-2023",
    "2023/24": "https://www.whoscored.com/regions/250/tournaments/12/seasons/9664/stages/22686/fixtures/europe-champions-league-2023-2024",
    "2024/25": "https://www.whoscored.com/regions/250/tournaments/12/seasons/10456/stages/24083/fixtures/europe-champions-league-2024-2025",
    "2025/26": "https://www.whoscored.com/regions/250/tournaments/12/seasons/10903/stages/24797/fixtures/europe-champions-league-2025-2026",
}

# Pausa entre requests para evitar bloqueos
DELAY_MIN = 3.0
DELAY_MAX = 6.0

# Poner False si WhoScored bloquea el scraper (podr­a ser necesario para ver el navegador y resolver captchas manualmente)
HEADLESS = False

from pathlib import Path
 
# Define la ruta de salida para los CSVs (ajusta según tu estructura de carpetas) PARA CUALQUIER ORDENADOR QUE EJECUTE ESTE CÓDIGO, SE GUARDARÁN LOS CSVs EN data/raw/ucl/whoscored/
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent  # sube 3 niveles: ucl/ → scrapers/ → proyecto/
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "ucl" / "whoscored"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# DRIVER (función create_driver hace todo el setup)

def create_driver() -> webdriver.Chrome:
    """Crea un driver de Chrome con configuraciÃ³n anti-detecciÃ³n."""
    options = Options()

    if HEADLESS:
        options.add_argument('--headless=new')

    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
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
    """Pausa aleatoria para evitar bloqueos."""
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def accept_cookies(driver: webdriver.Chrome):
    """Acepta el popup de cookies si aparece."""
    try:
        cookie_btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Aceptar todo') or contains(text(), 'Accept all')]")
            )
        )
        cookie_btn.click()
        log.info("  Cookies aceptadas âœ“")
        time.sleep(2)
    except Exception:
        log.info("  Sin popup de cookies")


# OBTENER PARTIDOS DE LA TEMPORADA (Funcion get_season_matches)#
def get_season_matches(driver: webdriver.Chrome, season_name: str, url: str) -> list[dict]:
    """
    Obtiene la lista de IDs de partidos de La Champions para una temporada.
    """
    log.info("  Obteniendo partidos de temporada %s...", season_name)
    all_match_ids = set()

    try:
        driver.get(url)
        time.sleep(10)  # espera a que cargue el JS
        accept_cookies(driver) # Acepta cookies si aparece

        

        # Número de meses a navegar hacia atrás (Champions dura ~9 meses)
        MONTHS_TO_NAVIGATE = 9

        for month_idx in range(MONTHS_TO_NAVIGATE):
            driver.execute_script("window.scrollBy(0, 500);") # scroll dentro del bucle para que cargue bien cada mes
            time.sleep(2)
        
            # Extraer IDs del mes actual
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
                log.info("    Mes %d: %d partidos encontrados (total: %d)",
                         month_idx + 1, len(ids), len(all_match_ids))
                
            # Navegar al mes anterior
            try:
                prev_btn = driver.find_element(By.ID, "dayChangeBtn-prev")
                driver.execute_script("arguments[0].click();", prev_btn)
                log.info("    Click en mes anterior OK")
                time.sleep(4)
            except Exception as e:
                log.info("    No se pudo hacer click: %s", e)
                break

        matches = [{'whoscored_match_id': mid, 'season': season_name} for mid in all_match_ids]
        log.info("   %d partidos totales para %s", len(matches), season_name)
        return matches
    
    except Exception as e:
        log.error("  Error en temporada %s: %s", season_name, e)
        return []



# OBTENER EVENTOS DE UN PARTIDO (Funcion get_match_data)#
def get_match_data(driver: webdriver.Chrome, match_id: str, season_name: str) -> dict:
    """
    Obtiene los datos de un partido desde matchCentreData.
    WhoScored incrusta todos los datos en un objeto JS dentro del HTML.
    """
    # Usa la URL en minÃºsculas que es la que funciona
    url = f"https://es.whoscored.com/matches/{match_id}/live"

    try:
        driver.get(url)
        random_sleep()

        # Acepta cookies si aparece
        accept_cookies(driver)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Busca el script que contiene matchCentreData
        script = soup.find('script', string=re.compile('matchCentreData'))
        if not script:
            log.warning("  No se encontrÃ³ matchCentreData para partido %s", match_id)
            return {}

        # Extrae el JSON del objeto matchCentreData
        pattern = r'matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n'
        m = re.search(pattern, script.string, re.DOTALL)
        if not m:
            log.warning("  No se pudo extraer matchCentreData para partido %s", match_id)
            return {}

        data = json.loads(m.group(1))
        data['whoscored_match_id'] = match_id
        data['season'] = season_name
        return data

    except Exception as e:
        log.error("  Error en partido %s: %s", match_id, e)
        return {}


# TRANSFORMACIónN DE DATOS (Funciones extract_events, extract_players_from_match, extract_teams_from_match)#

def extract_events(match_data: dict) -> list[dict]:
    """Extrae eventos con coordenadas normalizadas a 0-1."""
    match_id = match_data.get('whoscored_match_id')
    season   = match_data.get('season')
    events   = match_data.get('events', [])

    result = []
    for e in events:
        try:
            x = e.get('x')
            y = e.get('y')
            end_x = e.get('endX')
            end_y = e.get('endY')

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
        except Exception as ex:
            log.warning("  Error procesando evento: %s", ex)
            continue

    return result


def extract_players_from_match(match_data: dict) -> list[dict]:
    """Extrae jugadores de ambos equipos."""
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
    """Extrae equipos del partido."""
    teams = []
    for side in ('home', 'away'):
        team_data = match_data.get(side, {})
        if team_data.get('teamId'):
            teams.append({
                'whoscored_team_id': team_data.get('teamId'),
                'team_name':         team_data.get('name'),
            })
    return teams


# ORQUESTADOR PRINCIPAL (función scrape_whoscored)#

def scrape_whoscored():
    """Orquestador principal. Recorre todas las temporadas y partidos."""
    all_matches = []
    all_events  = []
    all_players = []
    all_teams   = []

    driver = create_driver()

    try:
        log.info("Iniciando navegador...")
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        for season_name, url in SEASON_URLS.items():
            log.info("\nðŸ“… Temporada %s", season_name)

            matches = get_season_matches(driver, season_name, url)
            if not matches:
                continue

            all_matches.extend(matches)

            for i, match in enumerate(matches, 1):
                mid = match['whoscored_match_id']
                log.info("  [%d/%d] Partido %s", i, len(matches), mid)

                match_data = get_match_data(driver, mid, season_name)
                if not match_data or 'events' not in match_data:
                    continue

                all_events.extend(extract_events(match_data))
                all_players.extend(extract_players_from_match(match_data))
                all_teams.extend(extract_teams_from_match(match_data))

                if i % 10 == 0:
                    log.info("  â†’ %d/%d partidos | eventos: %d",
                             i, len(matches), len(all_events))

            log.info("  âœ“ Temporada %s completa", season_name)

    except Exception as e:
        log.error("Error fatal: %s", e)
    finally:
        driver.quit()
        log.info("Driver cerrado.")

    df_players = pd.DataFrame(all_players)
    df_teams   = pd.DataFrame(all_teams)

    return (
        pd.DataFrame(all_matches),
        pd.DataFrame(all_events),
        df_players.drop_duplicates(subset=['whoscored_player_id']) if not df_players.empty else df_players,
        df_teams.drop_duplicates(subset=['whoscored_team_id']) if not df_teams.empty else df_teams,
    )


# MAIN 

def main():
    print("=" * 55)
    print(f"  WhoScored scraper — Champions League 2020/21 → 2025/26")
    print("=" * 55)

    df_matches, df_events, df_players, df_teams = scrape_whoscored()

    if df_matches.empty:
        print("\nâš  No se obtuvieron datos.")
        return

    matches_path = os.path.join(OUTPUT_DIR, "whoscored_matches_ucl.csv")
    events_path  = os.path.join(OUTPUT_DIR, "whoscored_events_ucl.csv")
    players_path = os.path.join(OUTPUT_DIR, "whoscored_players_ucl.csv")
    teams_path   = os.path.join(OUTPUT_DIR, "whoscored_teams_ucl.csv")

    df_matches.to_csv(matches_path, index=False)
    df_events.to_csv( events_path,  index=False)
    df_players.to_csv(players_path, index=False)
    df_teams.to_csv(  teams_path,   index=False)

    print(f"\nâœ… Scraping finalizado")
    print(f"  Partidos: {len(df_matches)}")
    print(f"  Eventos:  {len(df_events)}")
    print(f"  Jugadores:{len(df_players)}")
    print(f"  Equipos:  {len(df_teams)}")
    print(f"\nðŸ“ Archivos en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
