"""
scrapers/sofascore_scraper.py
==============================
Scraper unificado de SofaScore. Sigue el mismo patrÃ³n que understat_scraper.py:

    Estructura:
        1. CONSTANTS       - configuraciÃ³n del scraper
        2. HELPERS         - driver Selenium y peticiÃ³n JSON
        3. FETCH           - funciones puras de obtenciÃ³n de datos
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

    Los loaders/ son los Ãºnicos que escriben en la DB.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
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

# â”€â”€ CONSTANTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

TOURNAMENT_ID = 8                          # La Liga en SofaScore
SEASON_NAMES  = ["LaLiga 20/21", "LaLiga 21/22", "LaLiga 22/23", "LaLiga 23/24", "LaLiga 24/25"]  # temporadas a scrapear
DELAY_SEC     = 0.3                        # pausa entre peticiones
PROJECT_ROOT  = Path(__file__).resolve().parent.parent
OUTPUT_DIR    = PROJECT_ROOT / "data" / "raw" / "sofascore"
# Note: mkdir() is called inside scrape_sofascore() to avoid side-effects on import


# â”€â”€ HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Crea un Chrome controlado por Selenium.

    headless=True es suficiente para la API de SofaScore.
    La respuesta siempre es JSON puro en el body, sin JS que renderizar.
    """
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.page_load_strategy = "eager"

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def get_json(driver: webdriver.Chrome, url: str, timeout: float = 2) -> dict:
    """Navega a una URL de la API de SofaScore y devuelve el JSON parseado.

    SofaScore usa una API REST pÃºblica que devuelve JSON directamente
    en el body del navegador; no hay JS que renderizar.
    """
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(DELAY_SEC)
    return json.loads(driver.find_element("tag name", "body").text)


# â”€â”€ FETCH FUNCTIONS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_season_id(driver: webdriver.Chrome, tournament_id: int, season_name: str) -> tuple[Optional[int], Optional[str]]:
    """Devuelve (season_id, season_label) para un nombre de temporada dado.

    Consulta el endpoint de temporadas del torneo y busca la que
    contenga season_name en su nombre.
    """
    data = get_json(
        driver,
        f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons",
    )
    for s in data.get("seasons", []):
        if season_name in s["name"]:
            return s["id"], s["name"]
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


def get_match_shots(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo del mapa de tiros de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/shotmap")


def get_match_events(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de los incidentes de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/incidents")


def get_match_lineups(driver: webdriver.Chrome, match_id: int) -> dict:
    """Devuelve el JSON crudo de las alineaciones de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/lineups")


# â”€â”€ ORCHESTRATOR â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def scrape_sofascore(
    season_name: str  = None,
    tournament_id: int = TOURNAMENT_ID,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Orquestador principal: obtiene partidos, tiros, eventos y alineaciones.

    Args:
        season_name: Nombre de la temporada (ej: "2020/2021"). Si es None, usa la primera.
        tournament_id: ID del torneo en SofaScore.

    Returns:
        (matches, all_shots, all_events, all_lineups)
        Cada elemento es una lista de dicts con los datos crudos de SofaScore.
    """
    if season_name is None:
        season_name = SEASON_NAMES[0]
    
    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # crear directorio al scrapear, no al importar

    driver = create_driver()
    all_shots:    list[dict] = []
    all_events:   list[dict] = []
    all_lineups:  list[dict] = []

    try:
        season_id, season_label = get_season_id(driver, tournament_id, season_name)
        if season_id is None:
            raise ValueError(f"Temporada '{season_name}' no encontrada en SofaScore")

        print(f"\n[SEASON] Temporada: {season_label}  (id={season_id})")
        matches = get_matches(driver, tournament_id, season_id)
        print(f"  [+] {len(matches)} partidos encontrados")

        # Directorio base para la temporada
        base_path = OUTPUT_DIR / f"season={season_label}"
        base_path.mkdir(parents=True, exist_ok=True)

        # Guardar partidos crudos
        _save_json(matches, base_path / f"matches_batch_{batch_id}.json")

        for i, m in enumerate(matches, 1):
            match_id = m["id"]
            home     = m.get("homeTeam", {}).get("name", "?")
            away     = m.get("awayTeam", {}).get("name", "?")
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
                log.warning("Lineups failed match %d: %s", match_id, e)

    finally:
        driver.quit()

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
        
        try:
            matches, all_shots, all_events, _ = scrape_sofascore(season_name, TOURNAMENT_ID)
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
    main()
