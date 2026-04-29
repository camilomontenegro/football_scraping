"""
scrapers/whoscored_uel.py
==========================
Scraper de WhoScored para la Europa League.
Basado en whoscored_ucl.py — solo cambian los IDs de temporada,
las URLs de fixtures, el campo competition y OUTPUT_DIR.

IDs de referencia (competitions.py):
    WhoScored region_id    = 2      ← Europa (mismo que UCL)
    WhoScored tournament_id = 404   ← Europa League

Salida (data/raw/uel/whoscored/):
    season=2020_2021/
        matches_clean.csv        ← dim_match  (campos DB)
        events_clean.csv         ← fact_events (campos DB)
        teams.csv                ← dim_team   (campos DB)
        players.csv              ← dim_player (campos DB)
        raw/
            matches_raw.json
            match_<id>/
                events_raw.json

IMPORTANTE: WhoScored tiene protección anti-bot.
    - HEADLESS = False si el scraper es bloqueado
    - Los IDs de partido de SEASONS_DATA están vacíos por defecto → el scraper
      los descubrirá automáticamente navegando las páginas de fixtures.
      Cuando los tengas, pégalos en SEASONS_DATA para acelerar ejecuciones futuras.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── CONSTANTS ────────────────────────────────────────────────────────────────

# IDs de los partidos de cada temporada.
# Se dejan vacíos [] para que el scraper los descubra automáticamente.
# Una vez scrapeados, se pegan aquí para acelerar futuras ejecuciones.
SEASONS_DATA: dict[str, list[int]] = {
    "2020/21": [],  # añadir IDs cuando se conozcan
    "2021/22": [],
    "2022/23": [],
    "2023/24": [],
    "2024/25": [],
    "2025/26": [],
}

# URLs de las páginas de fixtures de WhoScored para la Europa League.
# tournament_id = 404 (competitions.py → "Europa League" → whoscored.tournament_id)
# Nota: las rutas de WhoScored siguen el patrón:
#   /regions/<region_id>/tournaments/<tournament_id>/seasons/<season_id>/stages/<stage_id>/fixtures/...
# Las season_id y stage_id varían cada año — verificar en la web de WhoScored
# si alguna URL deja de funcionar.
SEASON_URLS: dict[str, str] = {
    #"2020/21": "https://www.whoscored.com/regions/250/tournaments/30/seasons/8178/stages/19164/fixtures/europe-europa-league-2020-2021",
    #"2021/22": "https://www.whoscored.com/regions/250/tournaments/30/seasons/8741/stages/20266/fixtures/europe-europa-league-2021-2022",
    #"2022/23": "https://www.whoscored.com/regions/250/tournaments/30/seasons/9087/stages/20979/fixtures/europe-europa-league-2022-2023",
    "2023/24": "https://www.whoscored.com/regions/250/tournaments/30/seasons/9778/stages/22687/fixtures/europe-europa-league-2023-2024",
    "2024/25": "https://www.whoscored.com/regions/250/tournaments/30/seasons/10458/stages/23665/fixtures/europe-europa-league-2024-2025",
    "2025/26": "https://www.whoscored.com/regions/250/tournaments/30/seasons/10904/stages/24799/fixtures/europe-europa-league-2025-2026",
}

# Temporadas a scrapear (toma solo las que tienen URL activa en SEASON_URLS)
SEASON_NAMES: list[str] = list(SEASON_URLS.keys()) 

# Pausas entre requests (segundos) — WhoScored es sensible a velocidad
DELAY_MIN = 3.0
DELAY_MAX = 6.0

# La Europa League dura ~9 meses también (julio → mayo)
MONTHS_TO_NAVIGATE = 9

# False recomendado para poder resolver captchas manualmente
HEADLESS = False

OUTPUT_DIR   = Path(r"C:\Users\Noeli\football_scraping\data\raw\uel\whoscored")

# ── HELPERS ──────────────────────────────────────────────────────────────────

def create_driver() -> webdriver.Chrome:
    """Crea un Chrome con configuración anti-detección."""
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    driver.execute_cdp_cmd( # Oculta el flag de WebDriver para evitar detección
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'},
    )
    return driver


def random_sleep() -> None:
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

# Intenta aceptar cookies si aparece el banner (puede variar según región e idioma)
def accept_cookies(driver: webdriver.Chrome) -> None:
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(),'Aceptar todo') or contains(text(),'Accept all')]")
            )
        )
        btn.click()
        log.info("  Cookies aceptadas ✓")
        time.sleep(2)
    except Exception:
        pass

# Guarda un dict/list como JSON, creando carpetas si es necesario
def _save_json(data, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# Convierte etiquetas de temporada a nombres de carpeta consistentes
def _season_label_to_folder(label: str) -> str:
    """
    '2020/21' → 'season=2020_2021'
    Acepta también '2020/2021'.
    """
    parts = label.split("/")
    year2 = "20" + parts[1] if len(parts[1]) == 2 else parts[1]
    return f"season={parts[0]}_{year2}"

# Genera un ID de lote único basado en la fecha y hora actual
def _generate_batch_id() -> str:
    return "batch_" + datetime.now().strftime("%Y%m%d_%H%M%S")

# parsea fechas de WhoScored, que pueden venir en distintos formatos (timestamp, string con fecha, etc.)
def _parse_ws_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y-%m-%d")
    m = re.search(r"(\d{4}-\d{2}-\d{2})", str(raw))
    return m.group(1) if m else None


# ── FETCH FUNCTIONS ───────────────────────────────────────────────────────────

def get_season_match_ids(driver: webdriver.Chrome, season_label: str, url: str) -> list[str]:
    """
    Navega la página de fixtures y extrae todos los match IDs.
    Itera hacia atrás mes a mes (MONTHS_TO_NAVIGATE veces).
    """
    log.info("  Obteniendo partidos de temporada %s …", season_label)
    all_ids: set[str] = set()

    driver.get(url)
    time.sleep(10)
    accept_cookies(driver)

    _JS_EXTRACT_IDS = r"""
        var ids = [];
        document.querySelectorAll('a[href*="/matches/"]').forEach(function(l) {
            var m = l.href.match(/\/matches\/(\d+)/i);
            if (m) ids.push(m[1]);
        });
        return [...new Set(ids)];
    """

    for month_idx in range(MONTHS_TO_NAVIGATE):
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(2)

        ids: list[str] = driver.execute_script(_JS_EXTRACT_IDS) or []
        if ids:
            before = len(all_ids)
            all_ids.update(ids)
            log.info("    Mes %d: +%d nuevos (total: %d)",
                     month_idx + 1, len(all_ids) - before, len(all_ids))

        try:
            prev_btn = driver.find_element(By.ID, "dayChangeBtn-prev")
            driver.execute_script("arguments[0].click();", prev_btn)
            time.sleep(4)
        except Exception as e:
            log.info("    No se pudo navegar al mes anterior: %s", e)
            break

    log.info("  → %d partidos encontrados para %s", len(all_ids), season_label)
    return list(all_ids)


def get_match_data(driver: webdriver.Chrome, match_id: str, season_label: str) -> dict:
    """
    Carga la página de un partido y extrae matchCentreData.
    """
    url = f"https://es.whoscored.com/matches/{match_id}/live"
    try:
        driver.get(url)
        random_sleep()
        accept_cookies(driver)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        soup   = BeautifulSoup(driver.page_source, "html.parser")
        script = soup.find("script", string=re.compile("matchCentreData"))
        if not script:
            log.warning("  matchCentreData no encontrado para partido %s", match_id)
            return {}

        m = re.search(r"matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n", script.string, re.DOTALL)
        if not m:
            log.warning("  No se pudo parsear matchCentreData para partido %s", match_id)
            return {}

        data = json.loads(m.group(1))
        data["_whoscored_match_id"] = match_id
        data["_season_label"]       = season_label
        return data

    except Exception as e:
        log.error("  Error en partido %s: %s", match_id, e)
        return {}


# ── ORCHESTRATOR ──────────────────────────────────────────────────────────────

def scrape_whoscored_season(
    season_label: str,
    driver: webdriver.Chrome,
    batch_id: str,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Descarga todos los partidos de UNA temporada de Europa League.

    Returns:
        (matches_meta, all_events, all_players, all_teams)
    """
    season_folder = _season_label_to_folder(season_label)
    season_dir    = OUTPUT_DIR / season_folder
    raw_dir       = season_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Obtener IDs: precargados o scrapeando fixtures
    known_ids = [str(mid) for mid in SEASONS_DATA.get(season_label, [])]
    if known_ids:
        match_ids = known_ids
        log.info("  Usando %d IDs precargados para %s", len(match_ids), season_label)
    else:
        log.info("  Sin IDs precargados para %s → scrapeando fixtures …", season_label)
        url = SEASON_URLS.get(season_label)
        if not url:
            log.error("  No hay URL configurada para %s", season_label)
            return [], [], [], []
        match_ids = get_season_match_ids(driver, season_label, url)

    _save_json(
        [{"whoscored_match_id": mid, "season": season_label} for mid in match_ids],
        raw_dir / f"matches_batch_{batch_id}.json"
    )

    matches_meta: list[dict] = []
    all_events:   list[dict] = []
    all_players:  list[dict] = []
    all_teams:    list[dict] = []

    for i, mid in enumerate(match_ids, 1):
        log.info("  [%d/%d] Partido %s", i, len(match_ids), mid)

        match_data = get_match_data(driver, mid, season_label)
        if not match_data or "events" not in match_data:
            log.warning("    Sin datos para partido %s, saltando", mid)
            continue

        _save_json(match_data, raw_dir / f"match_{mid}" / "events_raw.json")

        matches_meta.append(_build_match_meta(match_data))
        all_events.extend(_extract_raw_events(match_data))
        all_players.extend(_extract_raw_players(match_data))
        all_teams.extend(_extract_raw_teams(match_data))

        if i % 10 == 0:
            log.info("    → %d/%d partidos | eventos acumulados: %d",
                     i, len(match_ids), len(all_events))

    log.info("  Temporada %s: %d partidos, %d eventos",
             season_label, len(matches_meta), len(all_events))
    return matches_meta, all_events, all_players, all_teams


# ── RAW EXTRACTORS ────────────────────────────────────────────────────────────

def _build_match_meta(match_data: dict) -> dict:
    home = match_data.get("home", {})
    away = match_data.get("away", {})
    raw_date   = match_data.get("startTime") or match_data.get("startDate") or ""
    match_date = _parse_ws_date(raw_date)
    score_str  = str(match_data.get("score", ""))
    return {
        "whoscored_match_id": match_data.get("_whoscored_match_id"),
        "season":             match_data.get("_season_label"),
        "match_date":         match_date,
        "home_team_id_ws":    home.get("teamId"),
        "home_team_name":     home.get("name"),
        "away_team_id_ws":    away.get("teamId"),
        "away_team_name":     away.get("name"),
        "home_score":         score_str.split(":")[0].strip() if ":" in score_str else None,
        "away_score":         score_str.split(":")[1].strip() if ":" in score_str else None,
        "data_source":        "whoscored",
    }


def _extract_raw_events(match_data: dict) -> list[dict]:
    mid    = match_data.get("_whoscored_match_id")
    season = match_data.get("_season_label")
    events = match_data.get("events", [])
    enriched = []
    for e in events:
        e["_whoscored_match_id"] = mid
        e["_season_label"]       = season
        enriched.append(e)
    return enriched


def _extract_raw_players(match_data: dict) -> list[dict]:
    players = []
    for side in ("home", "away"):
        team_data = match_data.get(side, {})
        team_id   = team_data.get("teamId")
        team_name = team_data.get("name")
        for p in team_data.get("players", []):
            players.append({
                "whoscored_player_id": p.get("playerId"),
                "player_name":         p.get("name"),
                "whoscored_team_id":   team_id,
                "team_name":           team_name,
                "position":            p.get("position"),
                "shirt_number":        p.get("shirtNo"),
            })
    return players


def _extract_raw_teams(match_data: dict) -> list[dict]:
    teams = []
    for side in ("home", "away"):
        team_data = match_data.get(side, {})
        if team_data.get("teamId"):
            teams.append({
                "whoscored_team_id": team_data.get("teamId"),
                "team_name":         team_data.get("name"),
            })
    return teams


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def transform_matches(matches_meta: list[dict]) -> pd.DataFrame:
    """Adapta metadatos de partidos a las columnas de dim_match."""
    rows = []
    for m in matches_meta:
        rows.append({
            "whoscored_match_id": m.get("whoscored_match_id"),
            "match_date":         m.get("match_date"),
            "season":             m.get("season"),
            "competition":        "Europa League",          # ← diferencia clave vs UCL
            "home_team_id_ws":    m.get("home_team_id_ws"),
            "home_team_name":     m.get("home_team_name"),
            "away_team_id_ws":    m.get("away_team_id_ws"),
            "away_team_name":     m.get("away_team_name"),
            "home_score":         m.get("home_score"),
            "away_score":         m.get("away_score"),
            "data_source":        "whoscored",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").astype("Int16")
        df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").astype("Int16")
    return df


def transform_events(events_raw: list[dict]) -> pd.DataFrame:
    """Adapta los eventos crudos a las columnas de fact_events."""
    rows = []
    for e in events_raw:
        try:
            x     = e.get("x")
            y     = e.get("y")
            end_x = e.get("endX")
            end_y = e.get("endY")
            rows.append({
                "whoscored_match_id":  e.get("_whoscored_match_id"),
                "whoscored_event_id":  e.get("id"),
                "whoscored_player_id": e.get("playerId"),
                "whoscored_team_id":   e.get("teamId"),
                "player_name":         e.get("playerName"),
                "event_type":          e.get("type", {}).get("displayName") if isinstance(e.get("type"), dict) else e.get("type"),
                "period":              e.get("period", {}).get("displayName") if isinstance(e.get("period"), dict) else e.get("period"),
                "minute":              e.get("minute"),
                "second":              e.get("second"),
                "x":                   round(float(x) / 100, 4) if x is not None else None,
                "y":                   round(float(y) / 100, 4) if y is not None else None,
                "end_x":               round(float(end_x) / 100, 4) if end_x is not None else None,
                "end_y":               round(float(end_y) / 100, 4) if end_y is not None else None,
                "outcome":             e.get("outcomeType", {}).get("displayName") if isinstance(e.get("outcomeType"), dict) else e.get("outcomeType"),
                "season":              e.get("_season_label"),
                "data_source":         "whoscored",
            })
        except Exception as ex:
            log.warning("  Error procesando evento: %s", ex)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
        df["second"] = pd.to_numeric(df["second"], errors="coerce").astype("Int16")
        df["x"]      = pd.to_numeric(df["x"],      errors="coerce").round(4)
        df["y"]      = pd.to_numeric(df["y"],      errors="coerce").round(4)
        df["end_x"]  = pd.to_numeric(df["end_x"],  errors="coerce").round(4)
        df["end_y"]  = pd.to_numeric(df["end_y"],  errors="coerce").round(4)
    return df


# ── DIM EXTRACTORS ────────────────────────────────────────────────────────────

def extract_teams(players_raw: list[dict]) -> pd.DataFrame:
    seen: dict[int, str] = {}
    for p in players_raw:
        tid = p.get("whoscored_team_id")
        if tid and tid not in seen:
            seen[tid] = p.get("team_name")
    df = pd.DataFrame(
        [{"whoscored_team_id": k, "canonical_name": v} for k, v in seen.items()]
    ).sort_values("whoscored_team_id").reset_index(drop=True)
    return df


def extract_players(players_raw: list[dict]) -> pd.DataFrame:
    seen: dict[int, dict] = {}
    for p in players_raw:
        pid = p.get("whoscored_player_id")
        if pid and pid not in seen:
            seen[pid] = {
                "whoscored_player_id": pid,
                "canonical_name":      p.get("player_name"),
                "position":            p.get("position"),
                "whoscored_team_id":   p.get("whoscored_team_id"),
            }
    df = pd.DataFrame(list(seen.values()))
    if not df.empty:
        df = df.sort_values("whoscored_player_id").reset_index(drop=True)
    return df


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    batch_id = _generate_batch_id()

    print("=" * 60)
    print(f"  WhoScored scraper — Europa League")
    print(f"  Temporadas: {SEASON_NAMES[0]} → {SEASON_NAMES[-1]}")
    print(f"  tournament_id WhoScored: 404")
    print(f"  Batch ID: {batch_id}") #opcional: usar batch_id para nombrar subcarpetas o archivos si quieres mantener versiones históricas
    print("=" * 60)

    driver = create_driver()

    try:
        driver.get("https://es.whoscored.com")
        time.sleep(5)
        accept_cookies(driver)

        for season_label in SEASON_NAMES:
            print(f"\n[SEASON] Descargando temporada {season_label} …")

            try:
                matches_meta, all_events, all_players_raw, all_teams_raw = (
                    scrape_whoscored_season(season_label, driver, batch_id)
                )
            except Exception as e:
                log.error("Temporada %s falló: %s", season_label, e)
                print(f"  Error en temporada {season_label}. Continuando …")
                continue

            if not matches_meta:
                print(f"  No se obtuvieron partidos para {season_label}")
                continue

            # Transformar
            df_matches = transform_matches(matches_meta)
            df_events  = transform_events(all_events)
            df_teams   = extract_teams(all_players_raw)
            df_players = extract_players(all_players_raw)

            # Guardar CSVs
            season_dir = OUTPUT_DIR / _season_label_to_folder(season_label)
            raw_dir = season_dir / "raw"
            season_dir.mkdir(parents=True, exist_ok=True)
            raw_dir.mkdir(parents=True, exist_ok=True)

            df_matches.to_csv(season_dir / "matches_clean.csv", index=False, encoding="utf-8-sig")
            df_events.to_csv( season_dir / "events_clean.csv",  index=False, encoding="utf-8-sig")
            df_teams.to_csv(  season_dir / "teams.csv",         index=False, encoding="utf-8-sig")
            df_players.to_csv(season_dir / "players.csv",       index=False, encoding="utf-8-sig")

            print(f"  Temporada {season_label}:")
            print(f"    Partidos: {len(df_matches)}")
            print(f"    Eventos:  {len(df_events)}")
            print(f"    Equipos:  {len(df_teams)}")
            print(f"    Jugadores:{len(df_players)}")
            print(f"  Archivos guardados en {season_dir}")

    finally:
        driver.quit()
        log.info("Driver cerrado.")

    print(f"\n Descarga de WhoScored UEL completada")
    print(f"   Directorio: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()