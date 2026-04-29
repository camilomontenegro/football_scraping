"""
scrapers/sofascore_ucl_standalone.py
======================================
Scraper autónomo de SofaScore para la Champions League.
NO depende de utils/ ni de ningún otro módulo del proyecto.

Temporadas: 2020/21 → 2025/26
Salida (data/raw/ucl/sofascore/):
    season=2020_2021/
        matches_clean.csv    ← partidos con fecha, equipos, resultado
        shots_clean.csv      ← tiros con xG y coordenadas X/Y
        events_clean.csv     ← eventos (incidentes) con coordenadas X/Y
        teams.csv            ← equipos únicos de la temporada
        players.csv          ← jugadores únicos de la temporada
        raw/
            matches_batch.json
            match_<id>/
                shots.json
                events.json
                lineups.json

NOTAS:
    - SofaScore permite headless=True (no tiene anti-bot agresivo como WhoScored)
    - Los loaders/ son los únicos que deben escribir en la BD
    - Este scraper solo escribe en disco (data/raw/)
"""

from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


# ── CONFIGURACIÓN ──────────────────────────────────────────────────────────────

TOURNAMENT_ID = 7   # Champions League en SofaScore

# Todas las temporadas disponibles
SEASON_NAMES = [
    #"Champions League 20/21",
    #"Champions League 21/22",
    #"Champions League 22/23",
    #"Champions League 23/24",
    #"Champions League 24/25",
        "Champions League 25/26",
]

# Pausa entre peticiones (segundos). 0.6 es suficiente para SofaScore.
DELAY_SEC = 0.6

# SofaScore no bloquea headless → True es más rápido.
# Cámbialo a False solo si ves errores de body vacío.
HEADLESS = True


# Ruta absoluta directa — siempre apunta al sitio correcto
OUTPUT_DIR = Path(r"C:\Users\Noeli\football_scraping\data\raw\ucl\sofascore")



# ── HELPERS ────────────────────────────────────────────────────────────────────

def _generate_batch_id() -> str:
    """Genera un ID único para esta ejecución (sin dependencia de utils/)."""
    return "batch_" + datetime.now().strftime("%Y%m%d_%H%M%S")


def _timestamp_to_date(ts) -> Optional[str]:
    """Convierte un Unix timestamp de SofaScore a cadena YYYY-MM-DD."""
    if not ts:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")


def _save_json(data, path: Path) -> None:
    """Guarda un objeto Python como JSON en disco."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _season_name_to_folder(season_name: str) -> str:
    """
    'Champions League 20/21' → 'season=2020_2021'
    Misma convención que WhoScored UCL.
    """
    # Extraer la parte '20/21' del nombre
    part = season_name.split(" ")[-1]          # '20/21'
    y1, y2 = part.split("/")
    # Normalizar a 4 dígitos
    if len(y1) == 2:
        y1 = "20" + y1
    if len(y2) == 2:
        y2 = "20" + y2
    return f"season={y1}_{y2}"


# ── SELENIUM ───────────────────────────────────────────────────────────────────

def create_driver() -> webdriver.Chrome:
    """Crea un Chrome controlado por Selenium."""
    options = Options()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.page_load_strategy = "eager"
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )


def get_json(driver: webdriver.Chrome, url: str, timeout: float = 10) -> dict:
    """
    Navega a una URL de la API de SofaScore y devuelve el JSON parseado.
    SofaScore devuelve JSON puro en el body — no hay JS que renderizar.
    """
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(DELAY_SEC)
    body = driver.find_element("tag name", "body").text
    return json.loads(body)


# ── FETCH ──────────────────────────────────────────────────────────────────────

def get_season_id(
    driver: webdriver.Chrome,
    tournament_id: int,
    season_name: str,
) -> tuple[Optional[int], Optional[str]]:
    """Busca el season_id de SofaScore para el nombre de temporada dado."""
    data = get_json(
        driver,
        f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons",
    )
    for s in data.get("seasons", []):
        if season_name in s.get("name", ""):
            return s["id"], s["name"]
    return None, None


def get_matches(
    driver: webdriver.Chrome,
    tournament_id: int,
    season_id: int,
) -> list[dict]:
    """
    Descarga todos los partidos de una temporada paginando el endpoint.
    El endpoint devuelve ~20 partidos por página; navega hacia atrás hasta agotar.
    """
    events: list[dict] = []
    page = 0
    while True:
        url = (
            f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}"
            f"/season/{season_id}/events/last/{page}"
        )
        data = get_json(driver, url)
        batch = data.get("events", [])
        if not batch:
            break
        events.extend(batch)
        if not data.get("hasNextPage"):
            break
        page += 1
        log.info("    Página %d → %d partidos acumulados", page, len(events))
    return events


def get_match_shots(driver: webdriver.Chrome, match_id: int) -> dict:
    """JSON crudo del shotmap de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/shotmap")


def get_match_events(driver: webdriver.Chrome, match_id: int) -> dict:
    """JSON crudo de los incidentes (eventos) de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/incidents")


def get_match_lineups(driver: webdriver.Chrome, match_id: int) -> dict:
    """JSON crudo de las alineaciones de un partido."""
    return get_json(driver, f"https://api.sofascore.com/api/v1/event/{match_id}/lineups")


# ── TRANSFORM ──────────────────────────────────────────────────────────────────

def transform_matches(matches: list[dict]) -> pd.DataFrame:
    """
    Adapta la lista cruda de partidos al esquema de dim_match.

    Columnas:
        id_sofascore, match_date, competition, season,
        home_team_id_ss, away_team_id_ss,
        home_team_name, away_team_name,
        home_score, away_score, data_source
    """
    rows = []
    for m in matches:
        rows.append({
            "id_sofascore":    m.get("id"),
            "match_date":      _timestamp_to_date(m.get("startTimestamp")),
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
    """
    Adapta los tiros crudos al esquema de fact_shots.

    Columnas:
        match_id_ss, player_id_ss, player_name, team_id_ss,
        minute, x, y, xg, result, shot_type, situation, data_source
    """
    rows = []
    for s in shots_raw:
        player = s.get("player", {})
        rows.append({
            "match_id_ss":  s.get("_match_id_ss"),
            "player_id_ss": player.get("id"),
            "player_name":  player.get("name"),
            "team_id_ss":   s.get("teamId"),
            "minute":       s.get("time"),
            "x":            s.get("playerCoordinates", {}).get("x"),
            "y":            s.get("playerCoordinates", {}).get("y"),
            "xg":           s.get("xg"),
            "result":       s.get("shotType"),      # Goal, Miss, Save...
            "shot_type":    s.get("bodyPart"),      # RightFoot, LeftFoot, Head
            "situation":    s.get("situation"),     # OpenPlay, SetPiece...
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
    """
    Adapta los incidentes crudos al esquema de fact_events.

    Columnas:
        match_id_ss, player_id_ss, player_name, team_id_ss,
        event_type, minute, second, x, y, end_x, end_y,
        outcome, data_source
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
            "second":       None,       # SofaScore no expone segundos en incidentes
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


# ── DIM EXTRACTORS ─────────────────────────────────────────────────────────────

def extract_teams(matches: list[dict]) -> pd.DataFrame:
    """
    Extrae equipos únicos de la lista de partidos.
    Columnas: id_sofascore, canonical_name
    """
    teams: dict[int, str] = {}
    for m in matches:
        for side in ("homeTeam", "awayTeam"):
            t   = m.get(side, {})
            tid = t.get("id")
            if tid and tid not in teams:
                teams[tid] = t.get("name")
    return (
        pd.DataFrame([{"id_sofascore": k, "canonical_name": v} for k, v in teams.items()])
        .sort_values("id_sofascore")
        .reset_index(drop=True)
    )


def extract_players(shots_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae jugadores únicos de tiros y eventos.
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


# ── MAIN ───────────────────────────────────────────────────────────────────────

def main():
    batch_id = _generate_batch_id()

    print("=" * 60)
    print("  SofaScore UCL — scraper autónomo")
    print(f"  Temporadas: {SEASON_NAMES[0]} → {SEASON_NAMES[-1]}")
    print(f"  Batch ID:   {batch_id}")
    print(f"  Salida:     {OUTPUT_DIR}")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    driver = create_driver()

    try:
        for season_name in SEASON_NAMES:
            print(f"\n[SEASON] {season_name}")

            # 1. Obtener season_id
            season_id, season_label = get_season_id(driver, TOURNAMENT_ID, season_name)
            if season_id is None:
                log.warning("  Temporada '%s' no encontrada en SofaScore. Saltando.", season_name)
                continue
            log.info("  season_id=%d  label=%s", season_id, season_label)

            # 2. Descargar lista de partidos
            matches = get_matches(driver, TOURNAMENT_ID, season_id)
            if not matches:
                log.warning("  No se obtuvieron partidos. Saltando.")
                continue
            log.info("  %d partidos encontrados", len(matches))

            season_dir = OUTPUT_DIR / _season_name_to_folder(season_name)
            season_dir.mkdir(parents=True, exist_ok=True)
            raw_dir = season_dir / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)

            # Guardar JSON crudo de partidos
            _save_json(matches, raw_dir / f"matches_batch_{batch_id}.json")

            # 3. Descargar tiros, eventos y alineaciones partido a partido
            all_shots:   list[dict] = []
            all_events:  list[dict] = []

            for i, m in enumerate(matches, 1):
                match_id  = m["id"]
                home_name = m.get("homeTeam", {}).get("name", "?")
                away_name = m.get("awayTeam", {}).get("name", "?")
                print(f"  [{i:>3}/{len(matches)}] {match_id}: {home_name} vs {away_name}", end="  ")

                match_dir = raw_dir / f"match_{match_id}"
                match_dir.mkdir(parents=True, exist_ok=True)

                # Tiros
                try:
                    shots_raw = get_match_shots(driver, match_id)
                    _save_json(shots_raw, match_dir / "shots.json")
                    for s in shots_raw.get("shotmap", []):
                        s["_match_id_ss"]  = match_id
                        s["_season_label"] = season_label
                    all_shots.extend(shots_raw.get("shotmap", []))
                    print(f"shots={len(shots_raw.get('shotmap', []))}", end="  ")
                except Exception as e:
                    log.warning("shots error match %d: %s", match_id, e)
                    print("shots=ERR", end="  ")

                # Eventos (incidentes)
                try:
                    events_raw = get_match_events(driver, match_id)
                    _save_json(events_raw, match_dir / "events.json")
                    for ev in events_raw.get("incidents", []):
                        ev["_match_id_ss"]  = match_id
                        ev["_season_label"] = season_label
                    all_events.extend(events_raw.get("incidents", []))
                    print(f"events={len(events_raw.get('incidents', []))}", end="  ")
                except Exception as e:
                    log.warning("events error match %d: %s", match_id, e)
                    print("events=ERR", end="  ")

                # Alineaciones (solo raw, no se transforma)
                try:
                    lineups_raw = get_match_lineups(driver, match_id)
                    _save_json(lineups_raw, match_dir / "lineups.json")
                    print("lineups=OK")
                except Exception as e:
                    log.warning("lineups error match %d: %s", match_id, e)
                    print("lineups=ERR")

            # 4. Transformar
            df_matches = transform_matches(matches)
            df_shots   = transform_shots(all_shots)
            df_events  = transform_events(all_events)
            df_teams   = extract_teams(matches)
            df_players = extract_players(df_shots, df_events)

            # 5. Guardar CSVs limpios
            df_matches.to_csv(season_dir / "matches_clean.csv", index=False, encoding="utf-8-sig")
            df_shots.to_csv(  season_dir / "shots_clean.csv",   index=False, encoding="utf-8-sig")
            df_events.to_csv( season_dir / "events_clean.csv",  index=False, encoding="utf-8-sig")
            df_teams.to_csv(  season_dir / "teams.csv",         index=False, encoding="utf-8-sig")
            df_players.to_csv(season_dir / "players.csv",       index=False, encoding="utf-8-sig")

            print(f"\n  ✓ Temporada {season_name} completada:")
            print(f"    Partidos:  {len(df_matches)}")
            print(f"    Tiros:     {len(df_shots)}")
            print(f"    Eventos:   {len(df_events)}")
            print(f"    Equipos:   {len(df_teams)}")
            print(f"    Jugadores: {len(df_players)}")
            print(f"    Guardado en: {season_dir}")

    finally:
        driver.quit()
        log.info("Driver cerrado.")

    print("\n✅ SofaScore UCL descarga completada")
    print(f"   Directorio raíz: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()