"""
scrapers/sofascore_scraper.py
==============================
Scraper unificado de SofaScore. Sigue el mismo patron que understat_scraper.py:

    Estructura:
        1. CONSTANTS       - configuracion del scraper
        2. HELPERS         - driver Selenium y peticion JSON
        3. FETCH           - funciones puras de obtencion de datos
        4. ORCHESTRATOR    - scrape_sofascore() acumula todo
        5. TRANSFORM       - adapta campos al esquema de la DB
        6. DIM EXTRACTORS  - extract_teams(), extract_players()
        7. MAIN            - scrape -> transform -> guardar en disco
        8. __main__ guard

    Salida (data/raw/sofascore/):
        season=<label>/
            matches_batch_<id>.json          <- lista cruda de partidos
            matches_clean.csv                <- dim_match (campos DB)
            teams.csv                        <- dim_team  (campos DB)
            players.csv                      <- dim_player (campos DB)
            match_<id>/batch_id=<id>/
                shots.json                   <- tiros crudos
                events.json                  <- incidentes crudos
                lineups.json                 <- alineaciones crudas
                shots_clean.csv              <- fact_shots (campos DB)
                events_clean.csv             <- fact_events (campos DB)

    Los loaders/ son los unicos que escriben en la DB.
"""

from __future__ import annotations

import json
import logging
import os
import re
import csv
import sys
import time
import requests
try:
    from curl_cffi import requests as tls_requests
except ImportError:  # dependencia opcional; queda documentada en requirements.txt
    tls_requests = None
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

log = logging.getLogger(__name__)


class SofaScoreBlockedError(RuntimeError):
    """Raised when SofaScore returns anti-bot challenge/forbidden JSON."""

#CONSTANTS
TOURNAMENT_ID = 8                          # La Liga en SofaScore
SEASON_NAMES  = ["LaLiga 20/21", "LaLiga 21/22", "LaLiga 22/23", "LaLiga 23/24", "LaLiga 24/25", "LaLiga 25/26"]  # temporadas a scrapear
DELAY_SEC     = 1.2                        # pausa entre peticiones; SofaScore penaliza ráfagas rápidas
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
SOFASCORE_API = "https://api.sofascore.com/api/v1"
SOFASCORE_WEB = "https://www.sofascore.com/"
SOFASCORE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Origin": "https://www.sofascore.com",
    "Referer": SOFASCORE_WEB,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}
OUTPUT_DIR    = PROJECT_ROOT / "data" / "raw" / "sofascore"
# Note: mkdir() is called inside scrape_sofascore() to avoid side-effects on import


# HELPERS 

def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Crea un Chrome controlado por Selenium.

    Se usa como fallback cuando el cliente HTTP recibe `challenge`. En entornos
    locales suele funcionar mejor iniciar sesión/cookies desde la web pública
    antes de abrir endpoints de API directamente.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1365,900")
    options.add_argument(f"--user-agent={SOFASCORE_HEADERS['User-Agent']}")
    options.page_load_strategy = "eager"

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def create_http_session():
    """Crea una sesión HTTP con cabeceras de navegador para SofaScore.

    Si está instalado `curl_cffi`, se usa con impersonación de Chrome porque
    SofaScore puede bloquear clientes por fingerprint TLS aunque los headers
    sean correctos. Si no está disponible, se usa `requests` estándar.
    """
    if tls_requests is not None:
        session = tls_requests.Session(impersonate="chrome136")
    else:
        session = requests.Session()
    session.headers.update(SOFASCORE_HEADERS)
    proxy = os.getenv("SOFASCORE_PROXY") or os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY")
    if proxy:
        try:
            session.proxies.update({"http": proxy, "https": proxy})
        except AttributeError:
            session.proxies = {"http": proxy, "https": proxy}
    # Calienta cookies de la web pública. Si falla, no aborta: la API puede
    # seguir respondiendo sin cookies en algunos entornos.
    try:
        session.get(SOFASCORE_WEB, timeout=12)
    except Exception as e:
        log.debug("No se pudieron precargar cookies de SofaScore: %s", e)
    return session


def _is_http_session(client) -> bool:
    if isinstance(client, requests.Session):
        return True
    if tls_requests is not None and isinstance(client, tls_requests.Session):
        return True
    return False


def _validate_sofascore_payload(data: dict, url: str) -> dict:
    """Valida errores de API y normaliza el mensaje de bloqueo."""
    error = data.get("error") if isinstance(data, dict) else None
    if error:
        reason = error.get("reason") or error.get("code") or "unknown"
        raise SofaScoreBlockedError(f"SofaScore bloquea {url}: {reason}")
    return data


def _get_json_http(session: requests.Session, url: str) -> dict:
    """Obtiene JSON vía requests, con reintentos cortos para 429/5xx."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code in {403, 401, 429}:
                raise SofaScoreBlockedError(
                    f"SofaScore bloquea {url}: HTTP {resp.status_code}"
                )
            if resp.status_code >= 500 and attempt < 2:
                time.sleep(DELAY_SEC * (attempt + 2))
                continue
            resp.raise_for_status()
            return _validate_sofascore_payload(resp.json(), url)
        except SofaScoreBlockedError:
            raise
        except (Exception, ValueError) as e:
            last_exc = e
            if attempt < 2:
                time.sleep(DELAY_SEC * (attempt + 1))
                continue
    raise RuntimeError(f"No se pudo leer JSON de SofaScore en {url}: {last_exc}")


def _get_json_selenium(driver: webdriver.Chrome, url: str, timeout: float = 5) -> dict:
    """Navega a una URL de la API de SofaScore con Selenium y devuelve JSON."""
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(DELAY_SEC)
    body = driver.find_element("tag name", "body").text.strip()
    try:
        return _validate_sofascore_payload(json.loads(body), url)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"SofaScore no devolvió JSON válido en {url}: {body[:200]}") from e


def get_json(client, url: str) -> dict:
    """Devuelve JSON desde SofaScore usando requests o Selenium."""
    if _is_http_session(client):
        return _get_json_http(client, url)
    return _get_json_selenium(client, url)


# â”€â”€ FETCH FUNCTIONS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _season_lookup_keys(season_name: str) -> set[str]:
    keys = {str(season_name or "").strip()}
    if re.match(r"^\d{4}/\d{4}$", str(season_name or "")):
        start_year, end_year = season_name.split("/")
        keys.add(f"{start_year[-2:]}/{end_year[-2:]}")
        keys.add(f"{start_year}/{end_year[-2:]}")
    return {k.lower() for k in keys if k}


def get_season_id(driver: webdriver.Chrome, tournament_id: int, season_name: str) -> tuple[Optional[int], Optional[str]]:
    """Devuelve (season_id, season_label) para un nombre de temporada dado.

    Consulta el endpoint de temporadas del torneo y busca la que
    contenga season_name en su nombre.
    """
    data = get_json(
        driver,
        f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons",
    )
    possible_names = _season_lookup_keys(season_name)

    for s in data.get("seasons", []):
        season_label = str(s.get("name", ""))
        season_year = str(s.get("year", ""))
        haystack = f"{season_label} {season_year}".lower()
        if any(name in haystack for name in possible_names):
            return s["id"], season_label
    return None, None


def get_reference_season_id(
    competition_name: str,
    tournament_id: int,
    season_name: str,
) -> tuple[Optional[int], Optional[str]]:
    """Resolve SofaScore season_id from the local master reference table."""
    ref_path = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
    if not ref_path.exists():
        return None, None
    with ref_path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (
                row.get("source") == "sofascore"
                and row.get("competition") == competition_name
                and row.get("season") == season_name
                and str(row.get("competition_id")) == str(tournament_id)
                and row.get("season_id")
            ):
                return int(row["season_id"]), row.get("season") or season_name
    return None, None


def get_matches(driver: webdriver.Chrome, tournament_id: int, season_id: int) -> list[dict]:
    """Devuelve todos los partidos de una temporada paginando el endpoint.

    El endpoint devuelve hasta ~20 partidos por pÃ¡gina.
    Navega hacia atrÃ¡s hasta agotar las pÃ¡ginas.
    """
    events = []
    page   = 0
    while True:
        url  = (
            f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}"
            f"/season/{season_id}/events/last/{page}"
        )
        data  = get_json(driver, url)
        batch = data.get("events", [])
        if not batch:
            break
        events.extend(batch)
        if not data.get("hasNextPage"):
            break
        page += 1
    return events


def _get_match_date(match: dict) -> "date | None":
    """Extrae la fecha de un partido de SofaScore."""
    from datetime import date
    # El campo puede ser "startDate" o "timestamp"
    start_date = match.get("startDate") or match.get("start_date")
    if start_date:
        # Formato: "2025-05-25" o similar
        return date.fromisoformat(start_date[:10])
    timestamp = match.get("timestamp") or match.get("startTime")
    if timestamp:
        # Unix timestamp
        return date.fromtimestamp(timestamp)
    return None


def get_match_shots(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo del mapa de tiros de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/shotmap")


def get_match_events(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de los incidentes de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/incidents")


def get_match_lineups(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de las alineaciones de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/lineups")


def get_scraped_sofascore_match_ids() -> set[int]:
    """Obtiene los id_sofascore de los partidos que ya tienen eventos en la BBDD."""
    try:
        from sqlalchemy import text
        from loaders.common import engine
        query = """
            SELECT DISTINCT m.id_sofascore
            FROM dim_match m
            JOIN fact_events e ON m.match_id = e.match_id
            WHERE m.id_sofascore IS NOT NULL
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
            return {int(r[0]) for r in rows}
    except Exception as e:
        log.warning("No se pudo consultar BBDD para cache de SofaScore: %s", e)
        return set()


# ── ORCHESTRATOR ──────────────────────────────────────────────────────────────

def scrape_sofascore(
    season_name: str = None,
    tournament_id: int = TOURNAMENT_ID,
    competition_name: str = "Bundesliga",
    from_date: str = None,
    full_refresh: bool = False,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Orquestador principal."""
    print(f"  [INFO] Iniciando scrape_sofascore para {competition_name} ({season_name or 'actual'})...")
    
    if season_name is None:
        season_name = SEASON_NAMES[0]
    
    from_date_obj = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        print(f"  [FILTER] Descargando solo partidos desde: {from_date}")
    
    print("  [INFO] Iniciando sesión HTTP con SofaScore...")
    
    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # crear directorio al scrapear, no al importar

    client = create_http_session()
    driver = None
    all_shots:    list[dict] = []
    all_events:   list[dict] = []
    all_lineups:  list[dict] = []

    try:
        season_id, season_label = get_reference_season_id(
            competition_name, tournament_id, season_name,
        )
        if season_id:
            print(f"  [INFO] SofaScore season_id resuelto desde tabla maestra: {season_id}")
        else:
            season_id, season_label = get_season_id(client, tournament_id, season_name)
        if season_id is None:
            raise ValueError(f"Temporada '{season_name}' no encontrada en SofaScore")

        print(f"\n[SEASON] Temporada: {season_label}  (id={season_id})")
        try:
            matches = get_matches(client, tournament_id, season_id)
        except SofaScoreBlockedError as e:
            print(f"  [WARN] Cliente HTTP bloqueado por SofaScore: {e}")
            print("  [INFO] Reintentando con Chrome visible para resolver cookies/challenge...")
            driver = create_driver(headless=False)
            driver.get(SOFASCORE_WEB)
            time.sleep(5)
            client = driver
            try:
                matches = get_matches(client, tournament_id, season_id)
            except SofaScoreBlockedError as selenium_error:
                raise SofaScoreBlockedError(
                    "SofaScore sigue devolviendo challenge/403 incluso con Chrome. "
                    "Esto normalmente indica bloqueo del origen/IP. Ejecuta desde una IP residencial "
                    "o define SOFASCORE_PROXY con un proxy residencial/sticky válido. "
                    f"Detalle original: {selenium_error}"
                ) from selenium_error
        print(f"  [+] {len(matches)} partidos encontrados")

        # Configurar carpeta base con nombre de la competicion
        from wizard.competitions import get_competition
        comp_config = get_competition(competition_name)
        comp_slug = "la-liga"  # default
        if competition_name:
            comp_slug = competition_name.lower().replace(" ", "-")
        elif tournament_id:
            # Intentar encontrar la competición por tournament_id
            from scripts.competitions import COMPETITIONS
            for key, config in COMPETITIONS.items():
                if config.get("sources", {}).get("sofascore", {}).get("id") == tournament_id:
                    comp_slug = key.lower().replace(" ", "-")
                    break

        # Convertir season "2024/2025" a "2024_2025"
        if season_name:
            folder_season = season_name.replace("/", "_")
        else:
            folder_season = season_label.replace("/", "_").replace(" ", "_")
        
        season_dir = OUTPUT_DIR / comp_slug / f"season={folder_season}"
        season_dir.mkdir(parents=True, exist_ok=True)
        base_path = season_dir

        # Guardar partidos crudos
        _save_json(matches, base_path / f"matches_batch_{batch_id}.json")

        # Filtrar por fecha si se especifica from_date
        if from_date_obj:
            matches = [m for m in matches if _get_match_date(m) and _get_match_date(m) >= from_date_obj]
            print(f"  [+] {len(matches)} partidos después de {from_date}")

        # Caché de DB
        scraped_ids = get_scraped_sofascore_match_ids() if not full_refresh else set()
        skipped_matches = 0

        for i, m in enumerate(matches, 1):
            match_id = m["id"]
            home     = m.get("homeTeam", {}).get("name", "?")
            away     = m.get("awayTeam", {}).get("name", "?")
            
            if not full_refresh and match_id in scraped_ids:
                skipped_matches += 1
                continue
                
            print(f"  [{i}/{len(matches)}] Match {match_id}: {home} vs {away}")

            match_dir = base_path / f"match_{match_id}" / f"batch_id={batch_id}"
            match_dir.mkdir(parents=True, exist_ok=True)

            # Tiros
            try:
                shots_raw = get_match_shots(driver, match_id)
                _save_json(shots_raw, match_dir / "shots.json")
                # AÃ±adir contexto al registro crudo
                for s in shots_raw.get("shotmap", []):
                    s["_match_id_ss"]     = match_id
                    s["_season_label"]    = season_label
                    s["_home_team_id_ss"] = m.get("homeTeam", {}).get("id")
                    s["_away_team_id_ss"] = m.get("awayTeam", {}).get("id")
                all_shots.extend(shots_raw.get("shotmap", []))
            except Exception as e:
                log.warning("Shots failed match %d: %s", match_id, e)

            # Eventos
            try:
                events_raw = get_match_events(driver, match_id)
                _save_json(events_raw, match_dir / "events.json")
                for ev in events_raw.get("incidents", []):
                    ev["_match_id_ss"]  = match_id
                    ev["_season_label"] = season_label
                all_events.extend(events_raw.get("incidents", []))
            except Exception as e:
                log.warning("Events failed match %d: %s", match_id, e)

            # Alineaciones
            try:
                lineups_raw = get_match_lineups(driver, match_id)
                _save_json(lineups_raw, match_dir / "lineups.json")
                all_lineups.append({"match_id": match_id, "data": lineups_raw})
            except Exception as e:
                log.warning("Fallo general en partido %d: %s", match_id, e)

    finally:
        if driver is not None:
            driver.quit()

    if not full_refresh:
        print(f"\n  [INFO] Partidos omitidos (ya en DB): {skipped_matches}")

    if matches:
        df_matches = transform_matches(matches)
        df_shots   = transform_shots(all_shots)
        df_events  = transform_events(all_events)
        df_teams   = extract_teams(matches)
        df_players = extract_players(df_shots, df_events)

        df_matches.to_csv(base_path / "matches_clean.csv", index=False, encoding="utf-8-sig")
        df_shots.to_csv(base_path / "shots_clean.csv",   index=False, encoding="utf-8-sig")
        df_events.to_csv(base_path / "events_clean.csv",  index=False, encoding="utf-8-sig")
        df_teams.to_csv(base_path / "teams.csv",   index=False, encoding="utf-8-sig")
        df_players.to_csv(base_path / "players.csv", index=False, encoding="utf-8-sig")

    return matches, all_shots, all_events, all_lineups


# â”€â”€ TRANSFORM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_matches(matches: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de partidos a las columnas de dim_match.

    Columnas generadas:
        id_sofascore, match_date, competition, season,
        home_team_id_ss, away_team_id_ss,
        home_team_name, away_team_name,
        home_score, away_score, data_source
    """
    rows = []
    for m in matches:
        status = m.get("status", {})
        score  = m.get("homeScore", {}), m.get("awayScore", {})

        rows.append({
            "id_sofascore":    m.get("id"),
            "match_date":      _ss_timestamp_to_date(m.get("startTimestamp")),
            "competition":     m.get("tournament", {}).get("name"),
            "season":          m.get("season", {}).get("name"),
            # IDs de equipos en SofaScore (para cruzar con dim_team)
            "home_team_id_ss": m.get("homeTeam", {}).get("id"),
            "away_team_id_ss": m.get("awayTeam", {}).get("id"),
            "home_team_name":  m.get("homeTeam", {}).get("name"),
            "away_team_name":  m.get("awayTeam", {}).get("name"),
            "home_score":      m.get("homeScore", {}).get("current"),
            "away_score":      m.get("awayScore", {}).get("current"),
            "data_source":     "sofascore",
        })
    return pd.DataFrame(rows)


def transform_shots(shots_raw: list[dict]) -> pd.DataFrame:
    """Adapta los tiros crudos a las columnas de fact_shots.

    Columnas generadas:
        id_sofascore (match), player_id_ss, team_id_ss,
        minute, x, y, xg, result, shot_type, situation, data_source
    """
    rows = []
    for s in shots_raw:
        player = s.get("player", {})
        rows.append({
            # Referencias a resolver por el loader en la DB
            "match_id_ss":   s.get("_match_id_ss"),
            "player_id_ss":   player.get("id"),
            "player_name":    player.get("name"),
            "team_id_ss":     s.get("teamId"),
            # Campos de fact_shots
            "minute":         s.get("time"),
            "x":              s.get("playerCoordinates", {}).get("x"),
            "y":              s.get("playerCoordinates", {}).get("y"),
            "xg":             s.get("xg"),
            "result":         s.get("shotType"),          # Goal, Miss, Save...
            "shot_type":      s.get("bodyPart"),          # RightFoot, LeftFoot, Head
            "situation":      s.get("situation"),         # OpenPlay, SetPiece...
            "data_source":    "sofascore",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["x"]   = pd.to_numeric(df["x"],   errors="coerce").round(4)
        df["y"]   = pd.to_numeric(df["y"],   errors="coerce").round(4)
        df["xg"]  = pd.to_numeric(df["xg"],  errors="coerce").round(4)
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
    return df


def transform_events(events_raw: list[dict]) -> pd.DataFrame:
    """Adapta los incidentes crudos a las columnas de fact_events.

    Columnas generadas:
        match_id_ss, player_id_ss, player_name, team_id_ss,
        event_type, minute, x, y, outcome, data_source
    """
    rows = []
    for ev in events_raw:
        player = ev.get("player", {})
        point  = ev.get("incidentPoint") or {}
        rows.append({
            "match_id_ss":  ev.get("_match_id_ss"),
            "player_id_ss": player.get("id"),
            "player_name":  player.get("name"),
            "team_id_ss":   ev.get("teamId"),
            "event_type":   ev.get("incidentType"),
            "minute":       ev.get("time"),
            "second":       None,           # SofaScore no expone segundos en incidentes
            "x":            point.get("x"),
            "y":            point.get("y"),
            "end_x":        None,
            "end_y":        None,
            "outcome":      ev.get("incidentClass"),
            "data_source":  "sofascore",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
        df["x"] = pd.to_numeric(df["x"], errors="coerce").round(4)
        df["y"] = pd.to_numeric(df["y"], errors="coerce").round(4)
    return df


# â”€â”€ DIM EXTRACTORS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def extract_teams(matches: list[dict]) -> pd.DataFrame:
    """Extrae equipos Ãºnicos de la lista de partidos -> columnas de dim_team.

    Columnas: id_sofascore, canonical_name
    """
    teams = {}
    for m in matches:
        for side in ("homeTeam", "awayTeam"):
            t = m.get(side, {})
            tid = t.get("id")
            if tid and tid not in teams:
                teams[tid] = t.get("name")
    df = pd.DataFrame(
        [{"id_sofascore": k, "canonical_name": v} for k, v in teams.items()]
    ).sort_values("id_sofascore").reset_index(drop=True)
    return df


def extract_players(shots_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """Extrae jugadores Ãºnicos de tiros y eventos -> columnas de dim_player.

    Columnas: id_sofascore, canonical_name
    """
    frames = []
    for df in (shots_df, events_df):
        if not df.empty and "player_id_ss" in df.columns:
            frames.append(
                df[["player_id_ss", "player_name"]]
                .rename(columns={"player_id_ss": "id_sofascore", "player_name": "canonical_name"})
            )
    if not frames:
        return pd.DataFrame(columns=["id_sofascore", "canonical_name"])
    return (
        pd.concat(frames)
        .drop_duplicates(subset=["id_sofascore"])
        .dropna(subset=["id_sofascore"])
        .sort_values("id_sofascore")
        .reset_index(drop=True)
    )


# â”€â”€ HELPERS INTERNOS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _save_json(data, path: Path) -> None:
    """Guarda JSON en disco de forma segura."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _ss_timestamp_to_date(ts) -> Optional[str]:
    """Convierte un Unix timestamp de SofaScore a cadena YYYY-MM-DD."""
    if not ts:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    print("=" * 55)
    print(f"  SofaScore scraper - La Liga {SEASON_NAMES[0]} a {SEASON_NAMES[-1]}")
    print("=" * 55)

    for season_name in SEASON_NAMES:
        print(f"\n[SEASON] Descargando temporada {season_name}...")
        print("  [DEBUG] Iniciando orquestador scrape_sofascore...")
        
        try:
            matches, all_shots, all_events, _ = scrape_sofascore(
                competition_name=args.competition, 
                season_name=season_name, 
                tournament_id=TOURNAMENT_ID
            )
        except ValueError as e:
            log.warning(f"Temporada {season_name} no disponible en SofaScore: {e}")
            print(f"  [!] Temporada {season_name} no encontrada. Continuando...")
            continue

        if not matches:
            print(f"  [!] No se obtuvieron partidos para {season_name}")
            continue

        print(f"  Temporada {season_name}:")
        print(f"    Partidos: {len(matches)}")
        print(f"    Tiros:    {len(all_shots)}")
        print(f"    Eventos:  {len(all_events)}")

        # Transformar
        df_matches = transform_matches(matches)
        df_shots   = transform_shots(all_shots)
        df_events  = transform_events(all_events)
        df_teams   = extract_teams(matches)
        df_players = extract_players(df_shots, df_events)

        # Guardar CSVs (capa de presentaciÃ³n para los loaders)
        season_dir = OUTPUT_DIR / f"season={season_name.replace('/', '_')}"
        season_dir.mkdir(parents=True, exist_ok=True)

        paths = {
            "matches": season_dir / "matches_clean.csv",
            "shots":   season_dir / "shots_clean.csv",
            "events":  season_dir / "events_clean.csv",
            "teams":   season_dir / "teams.csv",
            "players": season_dir / "players.csv",
        }

        df_matches.to_csv(paths["matches"], index=False, encoding="utf-8-sig")
        df_shots.to_csv(  paths["shots"],   index=False, encoding="utf-8-sig")
        df_events.to_csv( paths["events"],  index=False, encoding="utf-8-sig")
        df_teams.to_csv(  paths["teams"],   index=False, encoding="utf-8-sig")
        df_players.to_csv(paths["players"], index=False, encoding="utf-8-sig")

        print(f"Archivos guardados en {season_dir}")

    print(f"\n Descarga de SofaScore completada")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scraper de SofaScore")
    parser.add_argument("--tournament-id", "-t", type=int, default=None,
                        help="ID del torneo en SofaScore (ej: 8 para La Liga)")
    parser.add_argument("--season", "-s", type=str, default=None,
                        help="Temporada a scrapear (ej: 2024/2025)")
    parser.add_argument("--competition", "-c", type=str, default="Bundesliga",
                        help="Nombre de la competición")
    
    args = parser.parse_args()
    
    # Usar valores por defecto si no se especifican
    tournament_id = args.tournament_id if args.tournament_id else TOURNAMENT_ID
    season_name = args.season if args.season else SEASON_NAMES[0]
    
    # Sobrescribir constantes globales para esta ejecución
    if args.tournament_id: TOURNAMENT_ID = args.tournament_id
    if args.season: SEASON_NAMES = [args.season]
    
    main()
