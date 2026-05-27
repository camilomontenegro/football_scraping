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
#      Regenerar: python scripts/discover_whoscored_stages.py
WHOSCORED_STAGES: dict[tuple[str, str], dict] = {
    # -- La Liga --
    ("La Liga", "2020/21"): {"season_id": 8321, "stages": [18851]},
    ("La Liga", "2021/22"): {"season_id": 8681, "stages": [19895]},
    ("La Liga", "2022/23"): {"season_id": 9149, "stages": [21073]},
    ("La Liga", "2023/24"): {"season_id": 9682, "stages": [22176]},
    ("La Liga", "2024/25"): {"season_id": 10317, "stages": [23401]},
    ("La Liga", "2025/26"): {"season_id": 10803, "stages": [24622]},
    # -- Premier League --
    ("Premier League", "2020/21"): {"season_id": 8228, "stages": [18685]},
    ("Premier League", "2021/22"): {"season_id": 8618, "stages": [19793]},
    ("Premier League", "2022/23"): {"season_id": 9075, "stages": [20934]},
    ("Premier League", "2023/24"): {"season_id": 9618, "stages": [22076]},
    ("Premier League", "2024/25"): {"season_id": 10316, "stages": [23400]},
    ("Premier League", "2025/26"): {"season_id": 10743, "stages": [24533]},
    # -- Bundesliga --
    ("Bundesliga", "2020/21"): {"season_id": 8279, "stages": [18762]},
    ("Bundesliga", "2021/22"): {"season_id": 8667, "stages": [19862]},
    ("Bundesliga", "2022/23"): {"season_id": 9120, "stages": [21026]},
    ("Bundesliga", "2023/24"): {"season_id": 9649, "stages": [22128]},
    ("Bundesliga", "2024/25"): {"season_id": 10365, "stages": [23471]},
    ("Bundesliga", "2025/26"): {"season_id": 10720, "stages": [24478]},
    # -- Serie A --
    ("Serie A", "2020/21"): {"season_id": 8330, "stages": [18873]},
    ("Serie A", "2021/22"): {"season_id": 8735, "stages": [19982]},
    ("Serie A", "2022/23"): {"season_id": 9159, "stages": [21087]},
    ("Serie A", "2023/24"): {"season_id": 9659, "stages": [22143]},
    ("Serie A", "2024/25"): {"season_id": 10375, "stages": [23490]},
    ("Serie A", "2025/26"): {"season_id": 10732, "stages": [24500]},
    # -- Ligue 1 --
    ("Ligue 1", "2020/21"): {"season_id": 8185, "stages": [18594]},
    ("Ligue 1", "2021/22"): {"season_id": 8671, "stages": [19866]},
    ("Ligue 1", "2022/23"): {"season_id": 9129, "stages": [21037]},
    ("Ligue 1", "2023/24"): {"season_id": 9635, "stages": [22105]},
    ("Ligue 1", "2024/25"): {"season_id": 10329, "stages": [23414]},
    ("Ligue 1", "2025/26"): {"season_id": 10792, "stages": [24609]},
    # -- Primeira Liga --
    ("Primeira Liga", "2020/21"): {"season_id": 8315, "stages": [18842]},
    ("Primeira Liga", "2021/22"): {"season_id": 8714, "stages": [19947]},
    ("Primeira Liga", "2022/23"): {"season_id": 9191, "stages": [21149]},
    ("Primeira Liga", "2023/24"): {"season_id": 9730, "stages": [22254]},
    ("Primeira Liga", "2024/25"): {"season_id": 10378, "stages": [23494]},
    ("Primeira Liga", "2025/26"): {"season_id": 10774, "stages": [24568]},
    # -- Eredivisie --
    ("Eredivisie", "2020/21"): {"season_id": 8187, "stages": [18596]},
    ("Eredivisie", "2021/22"): {"season_id": 8625, "stages": [19802]},
    ("Eredivisie", "2022/23"): {"season_id": 9112, "stages": [21021]},
    ("Eredivisie", "2023/24"): {"season_id": 9705, "stages": [22225]},
    ("Eredivisie", "2024/25"): {"season_id": 10321, "stages": [23405]},
    ("Eredivisie", "2025/26"): {"season_id": 10752, "stages": [24542]},
    # -- Champions League --
    ("Champions League", "2020/21"): {"season_id": 8177, "stages": [18972, 18973, 18974, 18975, 18976, 18977, 18978, 18979, 19130]},
    ("Champions League", "2021/22"): {"season_id": 8623, "stages": [20088, 20089, 20090, 20091, 20092, 20093, 20094, 20095, 20265]},
    ("Champions League", "2022/23"): {"season_id": 9086, "stages": [20961, 20962, 20963, 20964, 20965, 20966, 20967, 20968, 20969]},
    ("Champions League", "2023/24"): {"season_id": 9664, "stages": [22489, 22490, 22491, 22492, 22493, 22494, 22495, 22496, 22686]},
    ("Champions League", "2024/25"): {"season_id": 10456, "stages": [23663, 24083]},
    ("Champions League", "2025/26"): {"season_id": 10903, "stages": [24796, 24797]},
    # -- Europa League --
    ("Europa League", "2020/21"): {"season_id": 8178, "stages": [18981, 18982, 18983, 18984, 18985, 18986, 18987, 18988, 18989, 18990, 18991, 18992, 19164]},
    ("Europa League", "2021/22"): {"season_id": 8741, "stages": [20106, 20107, 20108, 20109, 20110, 20111, 20112, 20113, 20266]},
    ("Europa League", "2022/23"): {"season_id": 9087, "stages": [20971, 20972, 20973, 20974, 20975, 20976, 20977, 20978, 20979]},
    ("Europa League", "2023/24"): {"season_id": 9778, "stages": [22510, 22511, 22512, 22513, 22514, 22515, 22516, 22517, 22687]},
    ("Europa League", "2024/25"): {"season_id": 10458, "stages": [23665, 24084]},
    ("Europa League", "2025/26"): {"season_id": 10904, "stages": [24798, 24799]},
    # -- Europa Conference League --
    ("Europa Conference League", "2021/22"): {"season_id": 8696, "stages": [20105, 20114, 20115, 20116, 20117, 20118, 20119, 20120, 20267]},
    ("Europa Conference League", "2022/23"): {"season_id": 9109, "stages": [21010, 21011, 21012, 21013, 21014, 21015, 21016, 21017, 21018]},
    ("Europa Conference League", "2023/24"): {"season_id": 9672, "stages": [22502, 22503, 22504, 22505, 22506, 22507, 22508, 22509, 22688]},
    ("Europa Conference League", "2024/25"): {"season_id": 10462, "stages": [23668, 24006]},
    ("Europa Conference League", "2025/26"): {"season_id": 10905, "stages": [24800, 24801]},
    # -- FIFA World Cup --
    ("FIFA World Cup", "2014"): {"season_id": 3768, "stages": [7557, 7558, 7559, 7560, 7561, 7562, 7563, 7564, 7565, 7566, 7567, 7568, 7569]},
    ("FIFA World Cup", "2018"): {"season_id": 5967, "stages": [12751, 12752, 12753, 12754, 12755, 12756, 12757, 12758, 12759, 12760, 12761, 12762, 12763]},
    ("FIFA World Cup", "2022"): {"season_id": 8213, "stages": [18649, 18650, 18651, 18652, 18653, 18654, 18655, 18656, 18657]},
    ("FIFA World Cup", "2026"): {"season_id": 10498, "stages": [23752, 23753, 23754, 23755, 23756, 23757, 23758, 23759, 23760, 23761, 23762, 23763, 23764]},
    # -- European Championship --
    ("European Championship", "2020"): {"season_id": 7329, "stages": [16297, 16298, 16299, 16300, 16301, 16302, 16306]},
    ("European Championship", "2024"): {"season_id": 9299, "stages": [21399, 21400, 21401, 21402, 21403, 21404, 21415, 23157]},
    # -- Copa America --
    ("Copa America", "2021"): {"season_id": 8171, "stages": [18130, 18131, 18164]},
    ("Copa America", "2024"): {"season_id": 9910, "stages": [22767, 22768, 22769, 22770, 22868, 23386]},
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
# OUTPUT_DIR legacy. Las rutas reales vienen de utils.data_paths.
OUTPUT_DIR = str(PROJECT_ROOT / "data" / "raw" / "whoscored")

from utils.data_paths import save_clean_csv, normalize_season as _norm_season, raw_dir as _raw_dir  # noqa: E402


def _save_json(data, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


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


def get_season_matches(
    driver: webdriver.Chrome,
    season_name: str,
    url: str,
    stage_label: str | None = None,
) -> list[dict]:
    label = stage_label or season_name
    log.info("  Obteniendo partidos (%s)...", label)
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
            log.warning("  0 partidos en %s", label)
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
        log.info("  TOTAL %d partidos en %s", len(matches), label)
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


def _extract_attendance(html: str) -> int | None:
    """Extract attendance from WhoScored match page HTML.

    Tries multiple strategies:
      1. JSON data in JavaScript variables (matchHeader, require.config)
      2. HTML elements (match info section, dl/dd pairs)
      3. Regex fallback on raw page text
    """
    if not html:
        return None

    # Strategy 1: JSON in JavaScript — "attendance":12345 or "attendance": 12345
    m = re.search(r'"attendance"\s*:\s*(\d+)', html)
    if m:
        val = int(m.group(1))
        if val > 0:
            return val

    # Strategy 2: Parse HTML elements
    soup = BeautifulSoup(html, 'html.parser')

    # Look for <dt>/<dd> pairs (common WhoScored match info layout)
    for dt in soup.find_all('dt'):
        label = dt.get_text(strip=True).lower()
        if 'attendance' in label or 'asistencia' in label or 'espectadores' in label:
            dd = dt.find_next_sibling('dd')
            if dd:
                raw = dd.get_text(strip=True).replace(',', '').replace('.', '').replace(' ', '')
                digits = re.search(r'\d+', raw)
                if digits:
                    return int(digits.group())

    # Look for spans/divs with attendance-related class names
    for el in soup.find_all(['span', 'div'], class_=re.compile(r'attend|capacity', re.I)):
        raw = el.get_text(strip=True).replace(',', '').replace('.', '').replace(' ', '')
        digits = re.search(r'\d+', raw)
        if digits:
            val = int(digits.group())
            if val > 100:  # filter out noise
                return val

    # Strategy 3: Text-based regex on page
    text = soup.get_text()
    pattern = re.search(
        r'(?:Attendance|Asistencia|Espectadores)\s*[:\-]?\s*([\d,.\s]+)',
        text, re.IGNORECASE,
    )
    if pattern:
        raw = pattern.group(1).replace(',', '').replace('.', '').replace(' ', '')
        if raw.isdigit() and int(raw) > 100:
            return int(raw)

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
            # Extraer asistencia del HTML de la página
            data['attendance'] = _extract_attendance(html)
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


def extract_players_from_match(match_data: dict, competition: str | None = None) -> list[dict]:
    season = match_data.get('season')
    res = []
    for side in ('home', 'away'):
        team = match_data.get(side) or {}
        team_id = team.get('teamId')
        team_name = team.get('name')
        for p in team.get('players', []) or []:
            name = p.get('name')
            res.append({
                'whoscored_player_id': p.get('playerId'),
                'name':                name,
                'player_name':         name,
                'whoscored_team_id':   team_id,
                'team_name':           team_name,
                'position':            p.get('position'),
                'shirt_no':            p.get('shirtNo'),
                'competition':         competition,
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


def whoscored_season_available(competition_name: str, season: str) -> bool:
    """True si (competition, season) tiene season_id, stages y slug en config."""
    season_format = _competition_season_format(competition_name)
    target = _normalize_season(season, season_format)
    if target not in get_seasons_for_competition(competition_name):
        return False
    return bool(build_season_urls(competition_name, target))


# -- ORQUESTADOR ------------------------------------------------------

def _stage_label_from_url(url: str) -> str:
    m = re.search(r"/stages/(\d+)/", url, re.I)
    return f"stage {m.group(1)}" if m else "stage ?"


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

    all_matches: list[dict] = []
    all_events: list[dict] = []
    all_players: list[dict] = []
    all_teams: list[dict] = []

    driver = create_driver()
    try:
        log.info("Iniciando navegador...")
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        # Fase 1: descubrir partidos en TODAS las stages antes de descargar eventos.
        matches_by_id: dict[str, dict] = {}
        for season_name, url in seasons_to_run:
            stage_label = f"{season_name} ({_stage_label_from_url(url)})"
            log.info("\n[DISCOVERY] %s", stage_label)
            stage_matches = get_season_matches(
                driver, season_name, url, stage_label=stage_label,
            )
            new_ids = 0
            for row in stage_matches:
                mid = str(row["whoscored_match_id"])
                if mid not in matches_by_id:
                    matches_by_id[mid] = row
                    new_ids += 1
            log.info(
                "  %s: %d partidos (%d nuevos, total acumulado=%d)",
                stage_label, len(stage_matches), new_ids, len(matches_by_id),
            )

        all_matches = list(matches_by_id.values())
        log.info(
            "\n[DISCOVERY] Total único: %d partidos en %d stage(s)",
            len(all_matches), len(seasons_to_run),
        )

        # ── Guardar raw JSON: listado de partidos por temporada ──
        from collections import defaultdict
        _matches_by_season: dict[str, list] = defaultdict(list)
        for _m in all_matches:
            _matches_by_season[_m["season"]].append(_m)
        for _s, _s_matches in _matches_by_season.items():
            _sl = _norm_season(_s) or str(_s).replace("/", "_")
            _srd = _raw_dir(competition, _sl, "whoscored")
            _srd.mkdir(parents=True, exist_ok=True)
            _save_json(_s_matches, _srd / "matches.json")

        if not all_matches:
            log.warning("[!] No se encontraron partidos.")
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

        # Fase 2: descargar eventos de cada partido (todas las stages).
        fail_streak = 0
        for i, match in enumerate(all_matches, 1):
            if i > 1 and (i - 1) % DRIVER_RESTART_EVERY == 0:
                log.info(
                    "[ANTI-BOT] %d partidos procesados — reinicio preventivo del driver",
                    i - 1,
                )
                driver = restart_driver(driver)

            season_name = match["season"]
            mid = match["whoscored_match_id"]
            log.info("  [%d/%d] Partido %s", i, len(all_matches), mid)
            match_data = get_match_data(driver, mid, season_name)

            if not match_data or "events" not in match_data:
                fail_streak += 1
                log.warning("  Fallo acumulado: %d/%d", fail_streak, FAIL_STREAK_LIMIT)
                if fail_streak >= FAIL_STREAK_LIMIT:
                    log.warning(
                        "[ANTI-BOT] %d fallos seguidos — sospecha de bloqueo.",
                        fail_streak,
                    )
                    log.warning(
                        "[ANTI-BOT] Pausa de %ds + reinicio de driver...",
                        LONG_PAUSE_SECONDS,
                    )
                    time.sleep(LONG_PAUSE_SECONDS)
                    driver = restart_driver(driver)
                    fail_streak = 0
                continue

            fail_streak = 0

            # ── Guardar raw JSON del partido ──
            _sl = _norm_season(season_name) or str(season_name).replace("/", "_")
            _mrd = _raw_dir(competition, _sl, "whoscored") / "matches" / str(mid)
            _mrd.mkdir(parents=True, exist_ok=True)
            _save_json(match_data, _mrd / "match_data.json")

            m_date = match_data.get("match_date")
            if m_date:
                match["match_date"] = m_date

            m_att = match_data.get("attendance")
            if m_att:
                match["attendance"] = m_att

            all_events.extend(extract_events(match_data))
            all_players.extend(extract_players_from_match(match_data, competition=competition))
            all_teams.extend(extract_teams_from_match(match_data))
            if i % 10 == 0:
                log.info(
                    "  -> %d/%d partidos | eventos: %d",
                    i, len(all_matches), len(all_events),
                )

        log.info("  Descarga completa: %d partidos", len(all_matches))

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

    # Dedup por (id, season) — NUNCA por id solo: si un jugador o equipo
    # aparece en varias temporadas, cada una conserva su fila para que el
    # split per-season no pierda registros.
    if not df_players.empty and "season" in df_players.columns:
        df_players = df_players.drop_duplicates(subset=["whoscored_player_id", "season"])
    elif not df_players.empty:
        df_players = df_players.drop_duplicates(subset=["whoscored_player_id"])

    if not df_teams.empty and "season" in df_teams.columns:
        df_teams = df_teams.drop_duplicates(subset=["whoscored_team_id", "season"])
    elif not df_teams.empty:
        df_teams = df_teams.drop_duplicates(subset=["whoscored_team_id"])

    if df_matches.empty:
        log.warning("[!] No se obtuvieron datos - no se han escrito CSVs.")
        return (df_matches, df_events, df_players, df_teams)

    # Etiqueta competition en todas las tablas que tengan filas.
    for df in (df_matches, df_events, df_players, df_teams):
        if not df.empty:
            df["competition"] = competition

    # Layout canónico: split por temporada → data/clean/<comp>/<season>/whoscored/.
    # Sólo se procesan las temporadas que el usuario pidió en ESTE run
    # (`seasons_targets` armado más arriba). Esto garantiza que llamadas
    # sucesivas con --season distintos NUNCA reescriben carpetas ajenas.
    seasons_to_write: list[str] = []
    if "season" in df_matches.columns:
        seasons_to_write = [s for s in seasons_targets
                            if s in set(df_matches["season"].dropna().unique())]
    if not seasons_to_write:
        # Fallback defensivo: usa el arg directamente.
        seasons_to_write = [season] if season else seasons_targets

    log.info("WhoScored guardará %d temporada(s): %s",
             len(seasons_to_write), seasons_to_write)

    for s in seasons_to_write:
        season_label = _norm_season(s) or str(s).replace("/", "_")
        wrote_any = False
        for name, df in (
            ("matches", df_matches), ("events", df_events),
            ("players", df_players), ("teams", df_teams),
        ):
            if df.empty:
                continue
            if "season" not in df.columns:
                log.warning("WhoScored: '%s' sin columna season — se saltea para "
                            "evitar contaminar %s", name, season_label)
                continue
            df_slice = df[df["season"] == s].copy()
            if df_slice.empty:
                continue
            out = save_clean_csv(competition, season_label, "whoscored", name, df_slice)
            log.info("  · %s: %d filas → %s", name, len(df_slice), out)
            wrote_any = True
        if wrote_any:
            log.info("[OK] WhoScored %s %s — CSVs escritos",
                     competition, season_label)
        else:
            log.warning("[!] WhoScored %s %s — sin filas para esta temporada",
                        competition, season_label)

    log.info("    Totales: matches=%d events=%d players=%d teams=%d",
             len(df_matches), len(df_events), len(df_players), len(df_teams))

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
