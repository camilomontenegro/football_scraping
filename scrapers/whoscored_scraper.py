"""
whoscored_scraper.py
====================
Scraper de WhoScored usando Selenium + BeautifulSoup.
Extrae eventos de partidos (pases, tiros, presiones, duelos...)
con coordenadas X,Y del objeto matchCentreData.

La Liga â€” temporadas 2020/21 hasta 2025/26 â€” todos los equipos.

Guarda 4 CSVs en data/raw/whoscored/:
    - whoscored_events_laliga.csv   â†’ todos los eventos con coordenadas
    - whoscored_matches_laliga.csv  â†’ partidos con IDs
    - whoscored_players_laliga.csv  â†’ jugadores con IDs de WhoScored
    - whoscored_teams_laliga.csv    â†’ equipos con IDs de WhoScored

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

# â”€â”€ CONFIGURACIÃ“N â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# URLs exactas de fixtures de La Liga por temporada (versiÃ³n espaÃ±ola)
SEASON_URLS = {
    "2020/21": "https://es.whoscored.com/regions/206/tournaments/4/seasons/8321/stages/18851/fixtures/espa%C3%B1a-laliga-2020-2021",
    "2021/22": "https://es.whoscored.com/regions/206/tournaments/4/seasons/8681/stages/19895/fixtures/espa%C3%B1a-laliga-2021-2022",
    "2022/23": "https://es.whoscored.com/regions/206/tournaments/4/seasons/9149/stages/21073/fixtures/espa%C3%B1a-laliga-2022-2023",
    "2023/24": "https://es.whoscored.com/regions/206/tournaments/4/seasons/9682/stages/22176/fixtures/espa%C3%B1a-laliga-2023-2024",
    "2024/25": "https://es.whoscored.com/regions/206/tournaments/4/seasons/10317/stages/23401/fixtures/espa%C3%B1a-laliga-2024-2025",
    "2025/26": "https://es.whoscored.com/regions/206/tournaments/4/seasons/10803/stages/24622/fixtures/espa%C3%B1a-laliga-2025-2026",
}

# Pausa entre requests para evitar bloqueos
DELAY_MIN = 3.0
DELAY_MAX = 6.0

# Poner False si WhoScored bloquea â€” verÃ¡s el navegador abrirse
HEADLESS = False

OUTPUT_DIR = r'D:\Prueba scraping unders\data\raw\whoscored'
os.makedirs(OUTPUT_DIR, exist_ok=True)


# â”€â”€ DRIVER â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ OBTENER PARTIDOS DE LA TEMPORADA â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_season_matches(driver: webdriver.Chrome, season_name: str, url: str) -> list[dict]:
    """
    Obtiene la lista de IDs de partidos de La Liga para una temporada.
    Usa JavaScript para extraer los IDs de los links /matches/ de la pÃ¡gina.
    """
    log.info("  Obteniendo partidos de temporada %s...", season_name)

    try:
        driver.get(url)
        time.sleep(10)  # espera a que cargue el JS

        # Acepta cookies si aparece
        accept_cookies(driver)

        # Scroll para activar carga dinÃ¡mica
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(2)
        driver.execute_script("window.scrollBy(0, -200);")
        time.sleep(2)

        # Extrae IDs de partidos con JS â€” usa minÃºsculas /matches/
        script_js = r'''
        var ids = [];
        var links = document.querySelectorAll('a[href*="/matches/"]');
        links.forEach(function(l) {
            var m = l.href.match(/\/matches\/(\d+)/i);
            if (m) ids.push(m[1]);
        });
        return [...new Set(ids)];
        '''
        match_ids = driver.execute_script(script_js)

        if not match_ids:
            log.warning("  âš  0 partidos en %s", season_name)
            driver.save_screenshot(
                os.path.join(OUTPUT_DIR, f"error_{season_name.replace('/', '-')}.png")
            )
            return []

        matches = [{'whoscored_match_id': mid, 'season': season_name} for mid in match_ids]
        log.info("  âœ“ %d partidos encontrados para %s", len(matches), season_name)
        return matches

    except Exception as e:
        log.error("  Error en temporada %s: %s", season_name, e)
        return []


# â”€â”€ OBTENER EVENTOS DE UN PARTIDO â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ TRANSFORMACIÃ“N â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ ORQUESTADOR PRINCIPAL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    print("=" * 55)
    print(f"  WhoScored scraper â€” La Liga 2020/21 â†’ 2025/26")
    print("=" * 55)

    df_matches, df_events, df_players, df_teams = scrape_whoscored()

    if df_matches.empty:
        print("\nâš  No se obtuvieron datos.")
        return

    matches_path = os.path.join(OUTPUT_DIR, "whoscored_matches_laliga.csv")
    events_path  = os.path.join(OUTPUT_DIR, "whoscored_events_laliga.csv")
    players_path = os.path.join(OUTPUT_DIR, "whoscored_players_laliga.csv")
    teams_path   = os.path.join(OUTPUT_DIR, "whoscored_teams_laliga.csv")

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


def scrape_whoscored_with_args(
    region_id: int = None,
    tournament_id: int = None,
    seasons: list[str] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Versión de scrape_whoscored que acepta parámetros.
    
    Args:
        region_id: ID de región en WhoScored (ej: 206 para España)
        tournament_id: ID de torneo en WhoScored (ej: 4 para La Liga)
        seasons: Lista de temporadas (ej: ["2024/25"])
    
    Returns:
        (df_matches, df_events, df_players, df_teams)
    """
    # Valores por defecto (La Liga)
    if region_id is None:
        region_id = 206
    if tournament_id is None:
        tournament_id = 4
    if seasons is None:
        seasons = list(SEASON_URLS.keys())
    
    # Generar URLs dinámicamente
    dynamic_urls = {}
    for season in seasons:
        # URL base - el formato puede variar según la competición
        dynamic_urls[season] = f"https://es.whoscored.com/regions/{region_id}/tournaments/{tournament_id}/seasons/"
    
    # Usar las URLs generadas o las predefinidas
    urls_to_use = dynamic_urls if dynamic_urls != {"2020/21": "https://es.whoscored.com/regions/206/tournaments/4/seasons/"} else SEASON_URLS
    
    return scrape_whoscored()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scraper de WhoScored")
    parser.add_argument("--region-id", "-r", type=int, default=None,
                        help="ID de región en WhoScored (ej: 206 para España)")
    parser.add_argument("--tournament-id", "-t", type=int, default=None,
                        help="ID de torneo en WhoScored (ej: 4 para La Liga)")
    parser.add_argument("--seasons", "-s", type=str, default=None,
                        help="Temporadas a scrapear separadas por coma (ej: 2024/25,2023/24)")
    
    args = parser.parse_args()
    
    # Procesar temporadas
    if args.seasons:
        seasons = [s.strip() for s in args.seasons.split(",")]
    else:
        seasons = None
    
    scrape_whoscored_with_args(
        region_id=args.region_id,
        tournament_id=args.tournament_id,
        seasons=seasons
    )
