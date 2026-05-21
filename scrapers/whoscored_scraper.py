"""
whoscored_scraper.py
====================
Scraper genérico de WhoScored usando Selenium + BeautifulSoup.

Es compatible con cualquier liga definida en `scripts/competitions.py`
(LaLiga, Bundesliga, Premier League, Serie A, Ligue 1, …) siempre que se
proporcionen los IDs de temporada/stage en `WHOSCORED_STAGES`.

Estrategia de paginacion (universal):
  1. Carga la URL de fixtures.
  2. Abre el datepicker (#toggleCalendar).
  3. Cambia a vista de años (clic en button[class*='buttonOff']).
  4. Selecciona el AÑO mas antiguo seleccionable
     (ultimo td.datePicker_selectable del yearsTbody).
  5. Selecciona el MES mas antiguo seleccionable
     (primer td.datePicker_selectable del monthsTbody).
  6. Acumula IDs visibles y avanza con #dayChangeBtn-next por toda la
     temporada hasta que ya no haya partidos nuevos.

Mitigaciones anti-bot:
  - Reinicio del driver cada DRIVER_RESTART_EVERY partidos.
  - Pausa larga (LONG_PAUSE_SECONDS) ante FAIL_STREAK_LIMIT fallos seguidos.
  - Delays altos entre peticiones (DELAY_MIN..DELAY_MAX).
"""

import json
import os
import re
import sys
import time
import random
import logging
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# Permitir import desde scripts.competitions
sys.path.append(str(Path(__file__).resolve().parent.parent))
from scripts.competitions import get_competition  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

# -- CONFIGURACION ----------------------------------------------------

# Mapa (competition_name, season) -> {"season_id": int, "stages": [int, ...]}
# La URL canónica se construye a partir de la configuración de
# `competitions.py` (region_id, tournament_id, slug, season_format).
#
# `stages` es una lista para soportar torneos con varias fases (e.g.
# fase de grupos + final stage en una Copa del Mundo). Para una liga
# doméstica con una sola stage, basta con [stage_id].
#
# Cómo añadir una temporada/competición nueva:
#   1) Asegúrate de que la competición existe en scripts/competitions.py
#      con `whoscored.region_id`, `whoscored.tournament_id` y `whoscored.slug`.
#      Si la season en URL es un único año (e.g. "2026") añade
#      `whoscored.season_format = "single"`.
#   2) Añade la entrada (competition, season) -> {season_id, stages} abajo.
WHOSCORED_STAGES: dict[tuple[str, str], dict] = {
    # ── La Liga ──────────────────────────────────────────────────────
    ("La Liga", "2020/21"): {"season_id":  8321, "stages": [18851]},
    ("La Liga", "2021/22"): {"season_id":  8681, "stages": [19895]},
    ("La Liga", "2022/23"): {"season_id":  9149, "stages": [21073]},
    ("La Liga", "2023/24"): {"season_id":  9682, "stages": [22176]},
    ("La Liga", "2024/25"): {"season_id": 10317, "stages": [23401]},
    ("La Liga", "2025/26"): {"season_id": 10803, "stages": [24622]},

    # ── Bundesliga ───────────────────────────────────────────────────
    ("Bundesliga", "2025/26"): {"season_id": 10720, "stages": [24478]},

    # ── FIFA World Cup ────────────────────────────────────────────────
    # Mundial 2026: 12 grupos (A-L) + Final Stage (eliminatorias).
    # Stage IDs extraídos del HTML oficial de WhoScored 2026.
    ("FIFA World Cup", "2026"): {
        "season_id": 10498,
        "stages": [
            23753,  # Grp. A
            23754,  # Grp. B
            23755,  # Grp. C
            23756,  # Grp. D
            23757,  # Grp. E
            23758,  # Grp. F
            23759,  # Grp. G
            23760,  # Grp. H
            23761,  # Grp. I
            23762,  # Grp. J
            23763,  # Grp. K
            23764,  # Grp. L
            23752,  # Final Stage
        ],
    },
    ("FIFA World Cup", "2022"): {"season_id": 8213, "stages": []},
    ("FIFA World Cup", "2018"): {"season_id": 5967, "stages": []},
    ("FIFA World Cup", "2014"): {"season_id": 3768, "stages": []},
    # Para temporadas con `stages: []` el scraper construye igualmente
    # las URLs candidatas pero sin saber stage_id. Hay que rellenarlas
    # cuando se inspeccione el selector de stages de cada año.
}


def _format_season_url_part(season: str, season_format: str = "range") -> str:
    """Convierte la season al fragmento que aparece al final de la URL.

    `range`  -> "2025/26" o "2025/2026" -> "2025-2026"
    `single` -> "2026"               -> "2026"
    """
    s = season.strip()
    if season_format == "single":
        # Aceptamos "2026", "2026/27" (cogemos el primero) o "26" -> "2026".
        first = s.split("/", 1)[0].strip()
        if first.isdigit() and len(first) == 2:
            first = "20" + first
        return first
    if "/" in s:
        a, b = s.split("/", 1)
        a = a.strip()
        b = b.strip()
        if len(a) == 2 and a.isdigit():
            a = "20" + a
        if len(b) == 2 and b.isdigit():
            b = "20" + b
        return f"{a}-{b}"
    return s


def build_season_urls(competition_name: str, season: str) -> list[str]:
    """Construye TODAS las URLs de fixtures para (competition, season).

    Para ligas domésticas devuelve una sola URL.
    Para torneos con varias stages (Mundial, EURO, Nations League…)
    devuelve una URL por cada stage_id registrada.
    """
    comp = get_competition(competition_name)
    if not comp:
        log.error("Competición desconocida: %s", competition_name)
        return []
    ws = comp.get("sources", {}).get("whoscored") or {}
    region_id = ws.get("region_id")
    tournament_id = ws.get("tournament_id")
    slug = ws.get("slug")
    season_format = ws.get("season_format", "range")
    if not all([region_id, tournament_id, slug]):
        log.error(
            "Configuración WhoScored incompleta para %s (region/tournament/slug)",
            competition_name,
        )
        return []

    key = (competition_name, season)
    if key not in WHOSCORED_STAGES:
        log.error(
            "No hay (season_id, stages) registrados para %s %s. "
            "Añádelos a WHOSCORED_STAGES.",
            competition_name, season,
        )
        return []
    cfg = WHOSCORED_STAGES[key]
    season_id = cfg["season_id"]
    stage_ids = cfg.get("stages") or []
    if not stage_ids:
        log.warning(
            "WHOSCORED_STAGES no tiene stage_ids registradas para %s %s. "
            "Añádelas para poder scrapear este torneo.",
            competition_name, season,
        )
        return []

    season_part = _format_season_url_part(season, season_format)
    base = (
        f"https://es.whoscored.com/regions/{region_id}/tournaments/{tournament_id}"
        f"/seasons/{season_id}/stages/{{stage}}/fixtures/{slug}-{season_part}"
    )
    return [base.format(stage=sid) for sid in stage_ids]


# ── Compat: mantener `build_season_url` para callers antiguos ────────
def build_season_url(competition_name: str, season: str) -> str | None:
    """Devuelve la primera URL (compat). Usa `build_season_urls` para
    obtener todas las stages de un torneo internacional."""
    urls = build_season_urls(competition_name, season)
    return urls[0] if urls else None


def get_seasons_for_competition(competition_name: str) -> list[str]:
    """Devuelve las temporadas registradas para una competición."""
    return sorted(
        {s for (c, s) in WHOSCORED_STAGES.keys() if c == competition_name}
    )


def _slug_for_filename(competition_name: str) -> str:
    """Slug seguro para nombres de archivo (laliga, bundesliga, etc.)."""
    comp = get_competition(competition_name) or {}
    name = comp.get("name") or competition_name
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_") or "competition"

# Delays altos para no parecer bot (antes 4-8, ahora 6-12)
DELAY_MIN = 6.0
DELAY_MAX = 12.0
HEADLESS = False

# Reintentos para get_match_data ante bloqueos anti-bot
MATCH_RETRIES = 3
MATCH_RETRY_BACKOFF = (15, 30, 60)

# Mitigaciones anti-bot adicionales
DRIVER_RESTART_EVERY = 100      # cerrar/abrir Chrome cada N partidos
FAIL_STREAK_LIMIT = 5            # tras N fallos seguidos, pausa larga
LONG_PAUSE_SECONDS = 600         # 10 minutos de pausa cuando se sospecha bloqueo
RESTART_PAUSE_SECONDS = 30       # pausa al reiniciar el driver

# Tope de pulsaciones de next al avanzar dia/semana
MAX_NEXT_STEPS = 250

# Si el toggle deja de cambiar tras N intentos, asumimos fin de temporada
TOGGLE_STALE_LIMIT = 3

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = str(PROJECT_ROOT / "data" / "raw" / "whoscored")


# -- DRIVER -----------------------------------------------------------

def create_driver() -> webdriver.Chrome:
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
        options=options,
    )
    driver.execute_cdp_cmd(
        'Page.addScriptToEvaluateOnNewDocument',
        {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'}
    )
    return driver


def restart_driver(old_driver) -> webdriver.Chrome:
    """Cierra el driver actual y crea uno nuevo. Galletas y huellas se reinician."""
    log.info("[ANTI-BOT] Reiniciando driver — cerrando Chrome...")
    try:
        old_driver.quit()
    except Exception:
        pass
    log.info("[ANTI-BOT] Esperando %ds antes de abrir uno nuevo...", RESTART_PAUSE_SECONDS)
    time.sleep(RESTART_PAUSE_SECONDS)
    new_driver = create_driver()
    log.info("[ANTI-BOT] Nuevo Chrome abierto. Aterrizando en home y aceptando cookies...")
    new_driver.get("https://es.whoscored.com")
    time.sleep(5)
    accept_cookies(new_driver)
    return new_driver


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
        log.info("  Cookies aceptadas")
        time.sleep(2)
    except Exception:
        pass


# -- DATEPICKER -------------------------------------------------------

def _open_datepicker(driver: webdriver.Chrome) -> bool:
    """Click en #toggleCalendar para abrir el datepicker."""
    try:
        el = driver.find_element(By.CSS_SELECTOR, "#toggleCalendar")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.3)
        try:
            el.click()
        except Exception:
            driver.execute_script("arguments[0].click();", el)
        time.sleep(1.5)
        return True
    except Exception as e:
        log.warning("  No se pudo abrir el datepicker: %s", e)
        return False


def _switch_to_year_view(driver: webdriver.Chrome) -> bool:
    """Click en el boton del año para cambiar a vista de selector de años."""
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "button[class*='buttonOff']")
        try:
            btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn)
        time.sleep(1)
        rows = driver.find_elements(By.CSS_SELECTOR,
                                    "tbody[class*='yearsTbody'] td.datePicker_selectable")
        if rows:
            return True
        log.warning("  Tras clic en boton de año, no apareció yearsTbody")
        return False
    except Exception as e:
        log.warning("  No se pudo cambiar a vista de año: %s", e)
        return False


def _select_oldest_year(driver: webdriver.Chrome):
    try:
        cells = driver.find_elements(By.CSS_SELECTOR,
                                     "tbody[class*='yearsTbody'] td.datePicker_selectable")
        cells = [c for c in cells if c.is_displayed()]
        if not cells:
            return None
        oldest = cells[-1]
        year_text = (oldest.text or "").strip()
        try:
            oldest.click()
        except Exception:
            driver.execute_script("arguments[0].click();", oldest)
        time.sleep(1.5)
        try:
            return int(year_text)
        except ValueError:
            return None
    except Exception as e:
        log.warning("  No se pudo seleccionar año mas antiguo: %s", e)
        return None


def _select_first_selectable_month(driver: webdriver.Chrome):
    try:
        cells = driver.find_elements(By.CSS_SELECTOR,
                                     "tbody[class*='monthsTbody'] td.datePicker_selectable")
        cells = [c for c in cells if c.is_displayed()]
        if not cells:
            return None
        first = cells[0]
        month_text = (first.text or "").strip()
        try:
            first.click()
        except Exception:
            driver.execute_script("arguments[0].click();", first)
        time.sleep(2)
        return month_text or "?"
    except Exception as e:
        log.warning("  No se pudo seleccionar mes mas antiguo: %s", e)
        return None


def _jump_to_season_start(driver: webdriver.Chrome) -> bool:
    if not _open_datepicker(driver):
        return False

    if not _switch_to_year_view(driver):
        log.info("  Vista de año no disponible; intento mes en pantalla actual")
    else:
        year = _select_oldest_year(driver)
        if year is None:
            log.warning("  No se pudo escoger año; abortando datepicker")
            return False
        log.info("  Datepicker -> año mas antiguo: %d", year)
        time.sleep(0.7)

    month = _select_first_selectable_month(driver)
    if month is None:
        log.warning("  No se pudo escoger mes; abortando datepicker")
        return False
    log.info("  Datepicker -> primer mes con partidos: %s", month)
    return True


def _click_next_week(driver: webdriver.Chrome) -> bool:
    try:
        btn = driver.find_element(By.CSS_SELECTOR, "#dayChangeBtn-next")
        if not btn.is_displayed():
            return False
        klass = btn.get_attribute("class") or ""
        if "disabled" in klass.lower():
            return False
        try:
            btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn)
        return True
    except Exception:
        return False


def _read_toggle_text(driver: webdriver.Chrome) -> str:
    try:
        el = driver.find_element(By.CSS_SELECTOR, "#toggleCalendar .toggleDatePicker")
        return (el.text or "").strip()
    except Exception:
        try:
            el = driver.find_element(By.CSS_SELECTOR, "#toggleCalendar")
            return (el.text or "").strip()
        except Exception:
            return ""


# -- OBTENER PARTIDOS DE LA TEMPORADA ---------------------------------

JS_GET_MATCH_IDS = r'''
var ids = [];
var links = document.querySelectorAll('a[href*="/matches/"]');
links.forEach(function(l) {
    var m = l.href.match(/\/matches\/(\d+)/i);
    if (m) ids.push(m[1]);
});
return [...new Set(ids)];
'''


def get_season_matches(driver: webdriver.Chrome, season_name: str, url: str) -> list[dict]:
    log.info("  Obteniendo partidos de temporada %s...", season_name)
    try:
        driver.get(url)
        time.sleep(10)
        accept_cookies(driver)

        all_ids: set[str] = set()

        if _jump_to_season_start(driver):
            log.info("  Posicionado al inicio de temporada via datepicker")
            time.sleep(random.uniform(2.5, 4.0))
        else:
            log.warning("  Datepicker no disponible; recogiendo solo lo visible")

        last_toggle = _read_toggle_text(driver)
        ids = driver.execute_script(JS_GET_MATCH_IDS) or []
        all_ids.update(ids)
        log.info("  - Inicio ('%s'): %d partidos visibles (acumulado=%d)",
                 last_toggle, len(ids), len(all_ids))

        stale_streak = 0
        for step in range(MAX_NEXT_STEPS):
            if not _click_next_week(driver):
                log.info("  - Boton 'next' no encontrado/deshabilitado. Fin.")
                break
            time.sleep(random.uniform(2.0, 3.5))

            new_toggle = _read_toggle_text(driver)
            if new_toggle and new_toggle == last_toggle:
                stale_streak += 1
                log.info("  - Sem +%d: rango sin cambios (streak=%d/%d)",
                         step + 1, stale_streak, TOGGLE_STALE_LIMIT)
                if stale_streak >= TOGGLE_STALE_LIMIT:
                    log.info("  - %d intentos sin avanzar -> fin de temporada",
                             TOGGLE_STALE_LIMIT)
                    break
                continue

            stale_streak = 0
            last_toggle = new_toggle

            ids = driver.execute_script(JS_GET_MATCH_IDS) or []
            new_ids = [x for x in ids if x not in all_ids]
            all_ids.update(ids)
            log.info("  - Sem +%d ('%s'): %d nuevos (acumulado=%d)",
                     step + 1, new_toggle, len(new_ids), len(all_ids))

        if not all_ids:
            log.warning("  0 partidos en %s", season_name)
            try:
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                driver.save_screenshot(
                    os.path.join(OUTPUT_DIR,
                                 f"error_{season_name.replace('/', '-')}.png")
                )
            except Exception:
                pass
            return []

        matches = [{'whoscored_match_id': mid, 'season': season_name}
                   for mid in sorted(all_ids)]
        log.info("  TOTAL %d partidos encontrados para %s", len(matches), season_name)
        return matches

    except Exception as e:
        log.error("  Error en temporada %s: %s", season_name, e)
        return []


# -- OBTENER EVENTOS DE UN PARTIDO ------------------------------------

def _extract_match_centre(html: str):
    soup = BeautifulSoup(html, 'html.parser')
    script = soup.find('script', string=re.compile('matchCentreData'))
    if not script:
        return None
    pattern = r'matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n'
    m = re.search(pattern, script.string, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None


def _extract_match_date(match_data: dict) -> str | None:
    """Devuelve la fecha del partido en formato 'YYYY-MM-DD'.

    WhoScored almacena la fecha en `matchCentreData` bajo varias claves
    según el momento del partido. Probamos en orden de fiabilidad.
    """
    if not isinstance(match_data, dict):
        return None
    # Claves típicas que pueden traer la fecha como string
    for k in ("startDate", "startTime", "kickOffDate", "kickoffDate",
              "matchDate", "matchDateString"):
        v = match_data.get(k)
        if not v:
            continue
        s = str(v)
        # WhoScored suele dar '2025-08-22T18:30:00' o '20250822T183000'
        m = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", s)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def _looks_blocked(html: str) -> bool:
    indicators = [
        "cf-browser-verification",
        "Just a moment",
        "Attention Required",
        "challenge-platform",
        "Verifying you are human",
    ]
    return any(ind in html for ind in indicators)


def get_match_data(driver: webdriver.Chrome, match_id: str, season_name: str) -> dict:
    url = f"https://es.whoscored.com/matches/{match_id}/live"
    for attempt in range(1, MATCH_RETRIES + 1):
        try:
            driver.get(url)
            random_sleep()
            accept_cookies(driver)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            html = driver.page_source

            if _looks_blocked(html):
                wait = MATCH_RETRY_BACKOFF[min(attempt - 1, len(MATCH_RETRY_BACKOFF) - 1)]
                log.warning("  Bloqueo detectado en %s. Reintento %d/%d tras %ds.",
                            match_id, attempt, MATCH_RETRIES, wait)
                time.sleep(wait)
                continue

            data = _extract_match_centre(html)
            if data is None:
                wait = MATCH_RETRY_BACKOFF[min(attempt - 1, len(MATCH_RETRY_BACKOFF) - 1)]
                log.warning("  matchCentreData no encontrado en %s "
                            "(reintento %d/%d tras %ds)",
                            match_id, attempt, MATCH_RETRIES, wait)
                time.sleep(wait)
                continue

            data['whoscored_match_id'] = match_id
            data['season'] = season_name
            # Extraer fecha del partido en formato YYYY-MM-DD para downstream
            data['match_date'] = _extract_match_date(data)
            return data
        except Exception as e:
            wait = MATCH_RETRY_BACKOFF[min(attempt - 1, len(MATCH_RETRY_BACKOFF) - 1)]
            log.warning("  Error en partido %s intento %d/%d: %s (espera %ds)",
                        match_id, attempt, MATCH_RETRIES, e, wait)
            time.sleep(wait)

    log.error("  Abandonando partido %s tras %d intentos.", match_id, MATCH_RETRIES)
    return {}


# -- TRANSFORMACION ---------------------------------------------------

def extract_events(match_data: dict) -> list[dict]:
    match_id = match_data.get('whoscored_match_id')
    season = match_data.get('season')
    events = match_data.get('events', [])
    result = []
    for e in events:
        try:
            x = e.get('x'); y = e.get('y')
            end_x = e.get('endX'); end_y = e.get('endY')
            etype = e.get('type', {}).get('displayName') if isinstance(e.get('type'), dict) else e.get('type')
            period = e.get('period', {}).get('displayName') if isinstance(e.get('period'), dict) else e.get('period')
            outcome = e.get('outcomeType', {}).get('displayName') if isinstance(e.get('outcomeType'), dict) else e.get('outcomeType')
            result.append({
                'whoscored_match_id':  match_id,
                'whoscored_event_id':  e.get('id'),
                'whoscored_player_id': e.get('playerId'),
                'whoscored_team_id':   e.get('teamId'),
                'player_name':         e.get('playerName'),
                'event_type':          etype,
                'period':              period,
                'minute':              e.get('minute'),
                'second':              e.get('second'),
                'x':                   round(float(x) / 100, 4) if x is not None else None,
                'y':                   round(float(y) / 100, 4) if y is not None else None,
                'end_x':               round(float(end_x) / 100, 4) if end_x is not None else None,
                'end_y':               round(float(end_y) / 100, 4) if end_y is not None else None,
                'outcome':             outcome,
                'season':              season,
                'source':              'whoscored',
            })
        except Exception:
            continue
    return result


def extract_players_from_match(match_data: dict) -> list[dict]:
    season = match_data.get('season')
    res = []
    for side in ('home', 'away'):
        team = match_data.get(side) or {}
        team_id = team.get('teamId')
        for p in team.get('players', []) or []:
            res.append({
                'whoscored_player_id': p.get('playerId'),
                'name':                p.get('name'),
                'whoscored_team_id':   team_id,
                'position':            p.get('position'),
                'shirt_no':            p.get('shirtNo'),
                'season':              season,
                'source':              'whoscored',
            })
    return res


def extract_teams_from_match(match_data: dict) -> list[dict]:
    season = match_data.get('season')
    res = []
    for side in ('home', 'away'):
        team = match_data.get(side) or {}
        if team.get('teamId'):
            res.append({
                'whoscored_team_id': team.get('teamId'),
                'name':              team.get('name'),
                'season':            season,
                'source':            'whoscored',
            })
    return res


# -- NORMALIZADOR DE TEMPORADA ----------------------------------------

def _normalize_season(season: str, season_format: str = "range") -> str:
    """Normaliza la season a la representación que usa WHOSCORED_STAGES.

    `range`  (por defecto): "21/22" -> "21/22", "2021" -> "21/22"
    `single` (torneos internacionales): "2026" -> "2026", "26" -> "2026"
    """
    s = season.strip()
    if season_format == "single":
        # Coger el primer año
        first = s.split("/", 1)[0].strip()
        if first.isdigit():
            n = int(first)
            if n < 100:
                n += 2000
            return str(n)
        return s
    if "/" in s:
        a, b = s.split("/", 1)
        a = a.strip()
        b = b.strip()[-2:]
        if len(a) == 2 and a.isdigit():
            a = "20" + a
        return f"{a}/{b}"
    if s.isdigit():
        n = int(s)
        if n < 100:
            n += 2000
        return f"{n}/{(n + 1) % 100:02d}"
    return s


def _competition_season_format(competition_name: str) -> str:
    """Formato de season en URL para una competición ('range' / 'single')."""
    comp = get_competition(competition_name) or {}
    return comp.get("sources", {}).get("whoscored", {}).get(
        "season_format", "range",
    )


# -- ORQUESTADOR ------------------------------------------------------

def scrape_whoscored(season=None, competition: str = "La Liga"):
    """Descarga partidos de la liga indicada.

    Args:
        season: Temporada concreta ("2025/26"). Si es None, descarga todas
            las temporadas configuradas para esa competición.
        competition: Nombre de la competición tal como aparece en
            scripts/competitions.py. Por defecto "La Liga" para mantener
            compatibilidad con código antiguo.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    available_for_comp = get_seasons_for_competition(competition)
    if not available_for_comp:
        log.error(
            "No hay temporadas registradas en WHOSCORED_STAGES para %s. "
            "Añade entradas (competition, season) -> (season_id, stage_id) "
            "en whoscored_scraper.py para empezar a scrapear esa liga.",
            competition,
        )
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    season_format = _competition_season_format(competition)

    if season:
        target = _normalize_season(season, season_format)
        if target not in available_for_comp:
            log.error(
                "Temporada '%s' (normalizada a '%s') no disponible para %s. "
                "Disponibles: %s. Abortando WhoScored.",
                season, target, competition, available_for_comp,
            )
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        seasons_targets = [target]
        log.info("WhoScored [%s]: descargando SOLO la temporada %s",
                 competition, target)
    else:
        seasons_targets = available_for_comp
        log.info("WhoScored [%s]: descargando TODAS las temporadas (%s)",
                 competition, seasons_targets)

    # Lista de (season_label, url). Para torneos internacionales con
    # varias stages habrá varias entradas con la misma season_label.
    seasons_to_run: list[tuple[str, str]] = []
    for s in seasons_targets:
        urls = build_season_urls(competition, s)
        if not urls:
            log.warning("  Saltando %s %s: no se pudo construir ninguna URL.",
                        competition, s)
            continue
        for u in urls:
            seasons_to_run.append((s, u))
        if len(urls) > 1:
            log.info(
                "  %s %s: %d stages (fase de grupos + eliminatorias)",
                competition, s, len(urls),
            )

    if not seasons_to_run:
        log.error("No hay URLs válidas para ejecutar.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

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

        for season_name, url in seasons_to_run:
            log.info("\n[SEASON] %s", season_name)
            matches = get_season_matches(driver, season_name, url)
            if not matches:
                continue
            all_matches.extend(matches)

            fail_streak = 0
            for i, match in enumerate(matches, 1):
                # Reinicio periodico del driver para limpiar fingerprint
                if i > 1 and (i - 1) % DRIVER_RESTART_EVERY == 0:
                    log.info("[ANTI-BOT] %d partidos procesados — reinicio preventivo del driver",
                             i - 1)
                    driver = restart_driver(driver)

                mid = match['whoscored_match_id']
                log.info("  [%d/%d] Partido %s", i, len(matches), mid)
                match_data = get_match_data(driver, mid, season_name)

                if not match_data or 'events' not in match_data:
                    fail_streak += 1
                    log.warning("  Fallo acumulado: %d/%d", fail_streak, FAIL_STREAK_LIMIT)
                    if fail_streak >= FAIL_STREAK_LIMIT:
                        log.warning("[ANTI-BOT] %d fallos seguidos — sospecha de bloqueo.",
                                    fail_streak)
                        log.warning("[ANTI-BOT] Pausa de %ds + reinicio de driver...",
                                    LONG_PAUSE_SECONDS)
                        time.sleep(LONG_PAUSE_SECONDS)
                        driver = restart_driver(driver)
                        fail_streak = 0
                    continue

                # Reset del contador al obtener un partido bueno
                fail_streak = 0

                # Propagar la fecha del partido al match dict del CSV
                m_date = match_data.get('match_date')
                if m_date:
                    match['match_date'] = m_date

                all_events.extend(extract_events(match_data))
                all_players.extend(extract_players_from_match(match_data))
                all_teams.extend(extract_teams_from_match(match_data))
                if i % 10 == 0:
                    log.info("  -> %d/%d partidos | eventos: %d",
                             i, len(matches), len(all_events))

            log.info("  Temporada %s completa", season_name)

    except Exception as e:
        log.error("Error fatal: %s", e)
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        log.info("Driver cerrado.")

    df_matches = pd.DataFrame(all_matches)
    df_events  = pd.DataFrame(all_events)
    df_players = pd.DataFrame(all_players)
    df_teams   = pd.DataFrame(all_teams)
    if not df_players.empty:
        df_players = df_players.drop_duplicates(subset=['whoscored_player_id'])
    if not df_teams.empty:
        df_teams = df_teams.drop_duplicates(subset=['whoscored_team_id'])

    if not df_matches.empty:
        slug = _slug_for_filename(competition)
        # Etiquetamos cada DataFrame con la competición para downstream
        for df in (df_matches, df_events, df_players, df_teams):
            if not df.empty:
                df["competition"] = competition

        matches_path = os.path.join(OUTPUT_DIR, f"whoscored_matches_{slug}.csv")
        events_path  = os.path.join(OUTPUT_DIR, f"whoscored_events_{slug}.csv")
        players_path = os.path.join(OUTPUT_DIR, f"whoscored_players_{slug}.csv")
        teams_path   = os.path.join(OUTPUT_DIR, f"whoscored_teams_{slug}.csv")
        df_matches.to_csv(matches_path, index=False)
        df_events.to_csv(events_path,   index=False)
        df_players.to_csv(players_path, index=False)
        df_teams.to_csv(teams_path,     index=False)
        log.info("[OK] CSVs guardados en %s", OUTPUT_DIR)
        log.info("    matches=%d events=%d players=%d teams=%d",
                 len(df_matches), len(df_events), len(df_players), len(df_teams))
    else:
        log.warning("[!] No se obtuvieron datos - no se han escrito CSVs.")

    return (df_matches, df_events, df_players, df_teams)


# -- MAIN -------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="WhoScored scraper genérico")
    parser.add_argument(
        "--competition", "-c", default="La Liga",
        help="Nombre de la competición (ej. 'La Liga', 'Bundesliga').",
    )
    parser.add_argument(
        "--season", "-s", default=None,
        help="Temporada concreta (ej. '2025/26'). Si se omite, todas las disponibles.",
    )
    args = parser.parse_args()

    print("=" * 55)
    print(f"  WhoScored scraper - {args.competition}")
    print("=" * 55)
    df_matches, df_events, df_players, df_teams = scrape_whoscored(
        season=args.season, competition=args.competition,
    )
    if df_matches.empty:
        print("\n[!] No se obtuvieron datos.")
        return
    print("\n[OK] Scraping finalizado")
    print(f"  Partidos: {len(df_matches)}")
    print(f"  Eventos:  {len(df_events)}")
    print(f"  Jugadores:{len(df_players)}")
    print(f"  Equipos:  {len(df_teams)}")
    print(f"\n  Archivos en: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
