"""
scrapers/sofascore_scraper.py
==============================
Scraper GENÉRICO de SofaScore — funciona para cualquier competición.

Uso:
    python -m scrapers.sofascore_scraper --competition "La Liga"
    python -m scrapers.sofascore_scraper --competition "Champions League"
    python -m scrapers.sofascore_scraper --competition "Europa League"
    python -m scrapers.sofascore_scraper --competition "La Liga" --seasons "LaLiga 23/24" "LaLiga 24/25"

La competición debe existir en scripts/competitions.py con fuente "sofascore".

Estructura de salida (data/raw/<competition_slug>/sofascore/):
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

Los loaders/ son los únicos que escriben en la DB.
"""

from __future__ import annotations

import argparse 
import json 
import logging 
import re 
import sys 
import time
from pathlib import Path
from typing import Optional, Dict
import random

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


# ── Importar competitions.py (fuente única de verdad para IDs) ────────────────
from scripts.competitions import get_competition

log = logging.getLogger(__name__)



# ══════════════════════════════════════════════════════════════════════════════
# 1. CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
# NADA está hardcodeado aquí.
# Todo se carga desde competitions.py .
#
# Ejemplo:
#   --competition "La Liga"        → TOURNAMENT_ID=8,  slug="la_liga"
#   --competition "Champions League" → TOURNAMENT_ID=7,  slug="champions_league"
#   --competition "Europa League"  → TOURNAMENT_ID=679, slug="europa_league"


def _load_config(competition_name: str) -> dict:
    """
    Carga la configuración de SofaScore para una competición desde competitions.py.
    Lanza un error claro si la competición no existe o no tiene fuente sofascore.
    """
    comp = get_competition(competition_name)
    if comp is None:
        raise ValueError(
            f"Competición '{competition_name}' no encontrada en competitions.py.\n"
            f"Comprueba que el nombre coincide exactamente con la clave del diccionario."
        )
    source = comp["sources"].get("sofascore")
    if source is None:
        raise ValueError(
            f"La competición '{competition_name}' no tiene configuración de SofaScore "
            f"en competitions.py."
        )
    return {
        "tournament_id": source["tournament_id"],
        "comp_slug":     competition_name.lower().replace(" ", "_"),
        "comp_name":     comp["name"],
    }


# 2. Modifica la sección de constantes 
DELAY_MIN = 2.0   # Mínimo 2 segundos ,SI SE VUELVE A BLOQUEAR, AUMENTAR ESTE RANGO 3.0
DELAY_MAX = 5.0   # Máximo 5 segundos ,SI SE VUELVE A BLOQUEAR, AUMENTAR ESTE RANGO 8.0
PROJECT_ROOT = Path(__file__).resolve().parent.parent



# ══════════════════════════════════════════════════════════════════════════════
# 2. HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def create_driver(headless: bool = True) -> webdriver.Chrome:
    """Crea un Chrome controlado por Selenium.

    headless=True es suficiente para la API de SofaScore.
    La respuesta siempre es JSON puro en el body, sin JS que renderizar.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new") # Usa el nuevo modo headless de Chrome, más rápido y menos detectable
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.7727.117 Safari/537.36")
    
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver



def get_json(driver: webdriver.Chrome, url: str, timeout: float = 5) -> dict:
    """Navega a una URL de la API de SofaScore y devuelve el JSON parseado.

    SofaScore usa una API REST pública que devuelve JSON directamente
    en el body del navegador; no hay JS que renderizar.
    """
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        log.warning(f"Timeout o cuerpo vacío en: {url}")
        pass
    # Respetar un delay aleatorio entre peticiones para evitar bloqueos.
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    try:
        return json.loads(driver.find_element("tag name", "body").text)
    except json.JSONDecodeError:
        print(f"\n[!] Error: SofaScore ha bloqueado la petición (Challenge detectado).")
        print(f"    URL: {url}")
        return {}
    


def _ss_timestamp_to_date(ts) -> Optional[str]:
    """Convierte un Unix timestamp de SofaScore a cadena YYYY-MM-DD."""
    if not ts:
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════════════════════════
# 3. FETCH
# ══════════════════════════════════════════════════════════════════════════════

def get_season_id(driver: webdriver.Chrome, tournament_id: int, season_name: str) -> tuple[Optional[int], Optional[str]]:
    """Devuelve (season_id, season_label) para un nombre de temporada dado.

    Consulta el endpoint de temporadas del torneo y busca la que
    contenga season_name en su nombre (búsqueda parcial).
    """
    data = get_json(
        driver,
        f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons",
    )
    
    for s in data.get("seasons", []):
        if season_name in s.get("name", ""):
            return s["id"], s["name"]
    return None, None


def get_matches(driver: webdriver.Chrome, tournament_id: int, season_id: int) -> list[dict]:
    """Devuelve todos los partidos de una temporada paginando el endpoint.

    El endpoint devuelve hasta ~20 partidos por página.
    Navega hacia atrás hasta agotar las páginas.
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


# ══════════════════════════════════════════════════════════════════════════════
# 4. ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════
def _save_json(data, path: Path) -> None:
    """Guarda datos crudos en JSON para trazabilidad."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
         
def scrape_sofascore(
    season_name:   str,
    tournament_id: int,
    output_dir:    Path,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Orquestador principal: obtiene partidos, tiros, eventos y alineaciones.

    Args:
        season_name:   Nombre de la temporada tal como aparece en SofaScore
                       (ej: "LaLiga 24/25", "Champions League 24/25")
        tournament_id: ID del torneo en SofaScore (viene de competitions.py)
        output_dir:    Carpeta raíz donde guardar los datos
                       (ej: data/raw/la_liga/sofascore)

    Returns:
        (matches, all_shots, all_events, all_lineups) — datos crudos
    """
    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    output_dir.mkdir(parents=True, exist_ok=True)

    driver = create_driver(headless=False) # Cambia a headless=True si quieres que el navegador no se abra, pero ten cuidado con los bloqueos.
    all_shots:   list[dict] = []
    all_events:  list[dict] = []
    all_lineups: list[dict] = []

    try:
        season_id, season_label = get_season_id(driver, tournament_id, season_name)
        if season_id is None:
            raise ValueError(
                f"Temporada '{season_name}' no encontrada en SofaScore "
                f"para tournament_id={tournament_id}.\n"
                f"Comprueba el nombre exacto en la web de SofaScore."
            )

        print(f"\n[SEASON] Temporada: {season_label}  (id={season_id})")
        matches = get_matches(driver, tournament_id, season_id)
        print(f"  [+] {len(matches)} partidos encontrados")

        # Directorio base para la temporada
        base_path = output_dir / f"season={season_label}"
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

            # Pausa entre partidos completos (shots + events + lineups = 3 peticiones seguidas)
            time.sleep(random.uniform(3.0, 7.0))  # Ajusta este rango si sigues experimentando bloqueos 

    finally:
        driver.quit()

    return matches, all_shots, all_events, all_lineups


# ══════════════════════════════════════════════════════════════════════════════
# 5. TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════

def transform_matches(matches: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de partidos a las columnas de dim_match."""
    rows = []
    for m in matches:
        rows.append({
            "id_sofascore":    m.get("id"),
            "match_date":      _ss_timestamp_to_date(m.get("startTimestamp")),
            "competition":     m.get("tournament", {}).get("name"),
            "season":          m.get("season", {}).get("name"),
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
    """Adapta los tiros crudos a las columnas de fact_shots."""
    rows = []
    for s in shots_raw:
        player = s.get("player", {})
        rows.append({
            "match_id_ss":  s.get("_match_id_ss"),
            "player_id_ss": player.get("id"),
            "player_name":  player.get("name"),
            "team_id_ss":   s.get("_home_team_id_ss") if s.get("isHome") else s.get("_away_team_id_ss"),
            "minute":       s.get("time"),
            "x":            s.get("playerCoordinates", {}).get("x"),
            "y":            s.get("playerCoordinates", {}).get("y"),
            "xg":           s.get("xg"),
            "result":       s.get("shotType"),
            "shot_type":    s.get("bodyPart"),
            "situation":    s.get("situation"),
            "data_source":  "sofascore",
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["x"]      = pd.to_numeric(df["x"],      errors="coerce").round(4)
        df["y"]      = pd.to_numeric(df["y"],      errors="coerce").round(4)
        df["xg"]     = pd.to_numeric(df["xg"],     errors="coerce").round(4)
        df["minute"] = pd.to_numeric(df["minute"], errors="coerce").astype("Int16")
    return df


def transform_events(events_raw: list[dict]) -> pd.DataFrame: 
    """Adapta los incidentes crudos a las columnas de fact_events."""
    rows = []
    for ev in events_raw: 
        player = ev.get("player", {})
        point  = ev.get("incidentPoint") or {}
        rows.append({ # Nota: no todos los eventos tienen coordenadas, pero los que las tienen son valiosos para análisis espaciales.
            "match_id_ss":  ev.get("_match_id_ss"),
            "player_id_ss": player.get("id"),
            "player_name":  player.get("name"),
            "team_id_ss":   ev.get("teamId"),
            "event_type":   ev.get("incidentType"),
            "minute":       ev.get("time"),
            "second":       None,
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
        df["x"]      = pd.to_numeric(df["x"],      errors="coerce").round(4)
        df["y"]      = pd.to_numeric(df["y"],      errors="coerce").round(4)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 6. DIM EXTRACTORS
# ══════════════════════════════════════════════════════════════════════════════

def extract_teams(matches: list[dict]) -> pd.DataFrame:
    """Extrae equipos únicos de la lista de partidos -> columnas de dim_team."""
    teams = {}
    for m in matches:
        for side in ("homeTeam", "awayTeam"):
            t   = m.get(side, {})
            tid = t.get("id")
            if tid and tid not in teams:
                country_obj = t.get("country") or {}

                teams[tid] = {
                    "id_sofascore": tid, # el ID del equipo en SofaScore
                    "canonical_name": t.get("name"), # el nombre del equipo
                    "country": country_obj.get("name") # el país del equipo, 
                }
    return (
        pd.DataFrame(list(teams.values())) # convierte el dict de equipos a DataFrame
        .sort_values("id_sofascore")
        .reset_index(drop=True)
    )


def extract_players(shots_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """Extrae jugadores únicos de tiros y eventos -> columnas de dim_player."""
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


# ══════════════════════════════════════════════════════════════════════════════
# 7. MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")

    # ── Argumentos por terminal ───────────────────────────────────────────────
    # Aquí es donde recibimos --competition y --seasons cuando ejecutas el scraper.
    # Si no pasas nada, usa "La Liga" por defecto.
    parser = argparse.ArgumentParser(description="SofaScore scraper genérico")
    parser.add_argument(
        "--competition",
        default="La Liga",
        help='Nombre de la competición (debe existir en competitions.py). '
             'Ejemplos: "La Liga", "Champions League", "Europa League"'
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=None,
        help='Temporadas a descargar tal como aparecen en SofaScore. '
             'Ejemplos: "LaLiga 24/25" "LaLiga 23/24" o "Champions League 24/25"'
    )
    args = parser.parse_args()

    # ── Cargar configuración desde scripts/competitions.py ────────────────────────────
    # Esto reemplaza los TOURNAMENT_ID y OUTPUT_DIR hardcodeados de antes.
    config       = _load_config(args.competition)
    tournament_id = config["tournament_id"]
    output_dir    = PROJECT_ROOT / "data" / "raw" / config["comp_slug"] / "sofascore"

    # ── Temporadas ────────────────────────────────────────────────────────────
    # Si no se pasan --seasons, avisa al usuario para que las especifique.
    # Los nombres de temporada cambian entre competiciones.
    if args.seasons is None:
        print(
            f"\n[!] No especificaste --seasons.\n"
            f"    Consulta los nombres exactos en SofaScore para '{args.competition}'\n"
            f"    y pásalos así:\n"
            f'    python -m scrapers.sofascore_scraper --competition "{args.competition}" '
            f'--seasons "NombreTemporada 24/25"\n'
        )
        return

    print("=" * 60)
    print(f"  SofaScore scraper — {args.competition}")
    print(f"  tournament_id : {tournament_id}")
    print(f"  Temporadas    : {args.seasons}")
    print(f"  Salida        : {output_dir}")
    print("=" * 60)

    # ── Bucle por temporadas ──────────────────────────────────────────────────
    for season_name in args.seasons:
        print(f"\n[SEASON] Descargando temporada '{season_name}'...")

        try:
            matches, all_shots, all_events, _ = scrape_sofascore(
                season_name=season_name,
                tournament_id=tournament_id,
                output_dir=output_dir,
            )
        except ValueError as e:
            log.warning("Temporada '%s' no disponible: %s", season_name, e)
            print(f"  [!] Temporada '{season_name}' no encontrada. Continuando...")
            continue

        if not matches:
            print(f"  [!] No se obtuvieron partidos para '{season_name}'")
            continue

        # ── Transformar ───────────────────────────────────────────────────────
        df_matches = transform_matches(matches)
        df_shots   = transform_shots(all_shots)
        df_events  = transform_events(all_events)
        df_teams   = extract_teams(matches)
        df_players = extract_players(df_shots, df_events)

        # ── Guardar CSVs ──────────────────────────────────────────────────────
        season_label = df_matches["season"].iloc[0] if not df_matches.empty else season_name
        season_dir   = output_dir / f"season={season_label.replace('/', '_')}"
        season_dir.mkdir(parents=True, exist_ok=True)

        df_matches.to_csv(season_dir / "matches_clean.csv", index=False, encoding="utf-8-sig")
        df_shots.to_csv(  season_dir / "shots_clean.csv",   index=False, encoding="utf-8-sig")
        df_events.to_csv( season_dir / "events_clean.csv",  index=False, encoding="utf-8-sig")
        df_teams.to_csv(  season_dir / "teams.csv",         index=False, encoding="utf-8-sig")
        df_players.to_csv(season_dir / "players.csv",       index=False, encoding="utf-8-sig")

        print(f"  Temporada '{season_name}':")
        print(f"    Partidos:  {len(df_matches)}")
        print(f"    Tiros:     {len(df_shots)}")
        print(f"    Eventos:   {len(df_events)}")
        print(f"    Equipos:   {len(df_teams)}")
        print(f"    Jugadores: {len(df_players)}")
        print(f"  Guardado en: {season_dir}")

    print(f"\n SofaScore — {args.competition} completado")
    print(f"   Directorio: {output_dir}")


# ══════════════════════════════════════════════════════════════════════════════
# 8. __main__ GUARD
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
