"""
scrapers/transfermarkt_scraper.py
===================================
Scraper unificado de Transfermarkt. Sigue el mismo patrÃ³n que understat_scraper.py:

    Estructura:
        1. CONSTANTS       â€” configuraciÃ³n del scraper
        2. HELPERS         â€” parse_date, extract_id, request_with_retry
        3. FETCH           â€” funciones puras de obtenciÃ³n de datos
        4. ORCHESTRATOR    â€” scrape_transfermarkt() acumula todo
        5. TRANSFORM       â€” adapta campos al esquema de la DB
        6. DIM EXTRACTORS  â€” (jugadores ya son dimensiÃ³n directa)
        7. MAIN            â€” scrape â†’ transform â†’ guardar en disco
        8. __main__ guard

    Salida (data/raw/transfermarkt/):
        season=<year>/
            <team_slug>/batch_id=<id>/
                players.json            â† plantilla cruda
                injuries.json           â† lesiones crudas
            players_clean.csv           â† dim_player (campos DB)
            injuries_clean.csv          â† fact_injuries (campos DB)

    Transfermarkt es la fuente CANÃ“NICA de jugadores:
        dim_player.id_transfermarkt, canonical_name, nationality,
        birth_date, position

    Los loaders/ son los Ãºnicos que escriben en la DB.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# â”€â”€ CONSTANTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

LEAGUE_CODE = "ES1"       # La Liga en Transfermarkt
SEASONS     = [2020, 2021, 2022, 2023, 2024, 2025]  # aÃ±os de inicio de temporadas (20/21 a 25/26)
DELAY_MIN   = 3.0         # pausa mínima entre peticiones (segundos)
DELAY_MAX   = 6.0         # pausa máxima entre peticiones (segundos)
MAX_RETRIES = 3

# Absolute path robusto para que funcione sin importar desde dÃ³nde de la terminal lo lances
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR  = PROJECT_ROOT / "data" / "raw" / "transfermarkt"
CACHE_FILE  = OUTPUT_DIR / "last_scraped.json"

def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


# â”€â”€ HELPERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def parse_date(date_str: str) -> Optional[date]:
    """Convierte una cadena de fecha en un objeto date de Python.

    Acepta formatos como 30/04/1992, 30.04.1992, 1992-04-30.
    Devuelve None si la cadena es invÃ¡lida o vacÃ­a.
    """
    if not date_str or date_str.strip() in ("-", ""):
        return None
    date_str = date_str.replace(".", "/").replace("-", "/").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def _json_serializer(obj):
    """Serializer personalizado para json.dump que maneja objetos date."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def extract_player_id(href: str) -> Optional[str]:
    """Extrae el ID numÃ©rico de Transfermarkt de un href de jugador.

    Ejemplo: /lionel-messi/profil/spieler/28003 â†’ '28003'
    """
    match = re.search(r"/spieler/(\d+)", href)
    return match.group(1) if match else None


def extract_player_slug(href: str) -> Optional[str]:
    """Extrae el slug de URL de un href de jugador.

    Ejemplo: /lionel-messi/profil/spieler/28003 â†’ 'lionel-messi'
    """
    parts = href.split("/")
    return parts[1] if len(parts) > 1 else None


def request_with_retry(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """Hace una peticiÃ³n GET con reintentos exponenciales.

    Devuelve el objeto Response si tiene Ã©xito, o None si falla.
    """
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("Intento %d/%d fallido para %s: %s", i + 1, retries, url, e)
            time.sleep(2 * (i + 1))
    return None


# â”€â”€ FETCH FUNCTIONS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_league_teams(season: int, competition_slug: str, league_code: str) -> list[dict]:
    """
    Descarga los equipos participantes en una competición para una temporada.
    """
    url = (
        f"https://www.transfermarkt.es/{competition_slug}"
        f"/teilnehmer/pokalwettbewerb/{league_code}/saison_id/{season}"
    )

    r = request_with_retry(url)
    if not r:
        return []
    
    soup = BeautifulSoup(r.content, "html.parser")
    table = soup.find("table", class_="items")

    if not table:
        log.warning("No se encontró la tabla de equipos para la temporada %d", season)
        return []
    
    rows = table.find_all("tr", class_=["odd", "even"])
    teams = []

    for row in rows:
        try:
            anchor = row.find("a", title=True)
            if not anchor:
                continue

            href = anchor.get("href", "")
            parts = href.split("/")

            # /real-madrid/startseite/verein/418
            if len(parts) < 5 or parts[2] != "startseite" or parts[3] != "verein":
                continue

            team_slug = parts[1]
            team_id = int(parts[4])
            team_name = anchor.get("title")

            teams.append({
                "team_id":   team_id,
                "team_slug": team_slug,
                "team_name": team_name,
            })
        except Exception as e:
            log.warning("Error procesando fila de equipo: %s", e)
            continue

    return teams


def get_player_profile(player_slug: str, player_id: str) -> dict:
    """Extrae nacionalidad y fecha de nacimiento del perfil individual."""
    url = f"https://www.transfermarkt.es/{player_slug}/profil/spieler/{player_id}"
    r = request_with_retry(url)
    if not r:
        return {"nationality": None, "birth_date": None}

    soup = BeautifulSoup(r.text, "html.parser")
    profile = {"nationality": None, "birth_date": None}

    # Nueva estructura de Transfermarkt usa spans en lugar de li
    labels = soup.find_all("span", class_="info-table__content--regular")
    for label in labels:
        val = label.find_next_sibling("span")
        if not val:
            continue
            
        text_label = label.text.strip().lower()
        if "nacim" in text_label or "birth" in text_label:
            raw = val.text.split("(")[0]
            raw_match = re.search(r"\d{2}/\d{2}/\d{4}", raw)
            if raw_match:
                profile["birth_date"] = parse_date(raw_match.group())
        elif "nacionalidad" in text_label or "citizenship" in text_label:
            img = val.find("img")
            if img:
                profile["nationality"] = img.get("title")
            else:
                profile["nationality"] = val.get_text(strip=True)

    return profile


def get_squad(team_slug: str, team_id: int, season: int) -> list[dict]:
    """Descarga la plantilla de un equipo para una temporada.

    Devuelve una lista de dicts con: player_id, player_name, player_slug,
    position, nationality, birth_date, team_country.
    """
    url = (
        f"https://www.transfermarkt.es/{team_slug}/kader"
        f"/verein/{team_id}/saison_id/{season}"
    )
    r = request_with_retry(url)
    if not r:
        return []

    soup  = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")
    if not table:
        log.warning("Sin tabla de plantilla para %s", team_slug)
        return []

    # PaÃ­s del equipo (bandera en la cabecera)
    flag = soup.find("img", class_="flaggenrahmen")
    team_country = flag.get("title") if flag else None

    players = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        link = row.select_one("td.hauptlink a")
        if not link:
            continue

        href        = link.get("href", "")
        player_id   = extract_player_id(href)
        player_slug = extract_player_slug(href)

        # PosiciÃ³n (segunda fila de la tabla anidada dentro de la celda)
        position = None
        nested   = row.find("table")
        if nested:
            nested_rows = nested.find_all("tr")
            if len(nested_rows) > 1:
                position = nested_rows[1].get_text(strip=True)

        # Nacionalidad rÃ¡pida desde la tabla
        nationality_table = None
        tds = row.find_all("td")
        if len(tds) > 6:
            nat_img = tds[6].find("img")
            if nat_img:
                nationality_table = nat_img.get("title")

        # Perfil individual para fecha de nacimiento
        profile = get_player_profile(player_slug, player_id)

        players.append({
            "player_id":    player_id,
            "player_name":  link.text.strip(),
            "player_slug":  player_slug,
            "position":     position,
            "nationality":  profile["nationality"] or nationality_table,
            "birth_date":   profile["birth_date"],
            "team_slug":    team_slug,
            "team_id_tm":   team_id,
            "team_country": team_country,
        })

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return players


def get_player_injuries(player_slug: str, player_id: str) -> list[dict]:
    """Descarga el historial de lesiones de un jugador.

    Devuelve lista de dicts con: season, injury_type, date_from,
    date_until, days_absent, matches_missed.
    """
    url = f"https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}"
    r   = request_with_retry(url)
    if not r:
        return []

    soup  = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")
    if not table:
        return []

    injuries = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        days_str  = cols[4].text.strip()
        days_m    = re.search(r"\d+", days_str)

        span = cols[5].find("span")

        injuries.append({
            "season":         cols[0].text.strip(),
            "injury_type":    cols[1].text.strip(),
            "date_from":      parse_date(cols[2].text.strip()),
            "date_until":     parse_date(cols[3].text.strip()),
            "days_absent":    int(days_m.group()) if days_m else None,
            "matches_missed": int(span.text.strip()) if span and span.text.strip().isdigit() else None,
        })

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return injuries


# â”€â”€ ORCHESTRATOR â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def scrape_transfermarkt(
    competition_name: str = None,
    league_code: str = LEAGUE_CODE,
    season: int      = None,
    teams: Optional[dict[str, int]] = None,
    from_date: Optional[str] = None,
    full_refresh: bool = False,
    season_label: str = None,
) -> tuple[list[dict], list[dict]]:
    """Orquestador principal: descarga plantillas y lesiones de todos los equipos.

    Args:
        league_code: Código de liga en Transfermarkt (p.ej. 'ES1')
        season:      Año de inicio de la temporada (p.ej. 2020). Si es None, usa la primera.
        teams:       Dict {slug: id} de equipos. Si es None, se auto-descubren.
        from_date:   Fecha mínima para lesiones (formato YYYY-MM-DD). Lesiones desde esta fecha.

    Returns:
        (all_players, all_injuries) — listas de dicts con datos crudos.
    """
    # Parse from_date if provided
    from_date_obj = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        log.info("Filtrando lesiones desde: %s", from_date_obj)
    if season is None:
        season = SEASONS[0]
    
    if season_label is None:
        season_label = f"{season}_{season+1}"
    
    # Normalizar season_label para carpetas
    folder_season = season_label.replace("/", "_")
    
    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Obtener slug de la competición
    from scripts.competitions import get_competition_slug_transfermarkt
    comp_slug = competition_name.lower().replace(" ", "-") if competition_name else "la-liga"
    tm_slug = get_competition_slug_transfermarkt(competition_name) or "laliga"

    if not teams:
        teams_list = get_league_teams(season, tm_slug, league_code)
        teams = {t["team_slug"]: t["team_id"] for t in teams_list}
        log.info("Auto-descubiertos %d equipos para %s %d", len(teams), league_code, season)

    print("=" * 55)
    print(f"  Transfermarkt scraper — {league_code} {season_label}")
    print("=" * 55)

    all_players:  list[dict] = []
    all_injuries: list[dict] = []
    failed: list[str] = []

    cache = load_cache() if not full_refresh else {}
    today_str = str(date.today())
    skipped_players = 0

    season_dir = OUTPUT_DIR / comp_slug / f"season={folder_season}"
    season_dir.mkdir(parents=True, exist_ok=True)

    for team_slug, team_id in teams.items():
        print(f"\n[INFO] Equipo: {team_slug} (id={team_id})")

        # Directorio del equipo/batch
        team_dir = season_dir / team_slug / f"batch_id={batch_id}"
        team_dir.mkdir(parents=True, exist_ok=True)

        # Plantilla â€” con reintentos
        players = None
        for attempt in range(MAX_RETRIES):
            try:
                players = get_squad(team_slug, team_id, season)
                if players:
                    break
            except Exception as e:
                log.warning("%s intento %d: %s", team_slug, attempt + 1, e)
            time.sleep(2 * (attempt + 1))

        if not players:
            log.error("%s sin datos de plantilla", team_slug)
            failed.append(team_slug)
            continue

        # Enriquecer cada jugador con metadatos de extracciÃ³n
        for p in players:
            p["season"]   = season
            p["batch_id"] = batch_id

        # Lesiones por jugador
        team_injuries: list[dict] = []
        for p in players:
            player_id_str = str(p["player_id"])
            last_scraped = cache.get(player_id_str)
            
            # Si no es full_refresh, comprobar si pasaron menos de 7 días
            if not full_refresh and last_scraped:
                days_since = (date.today() - datetime.strptime(last_scraped, "%Y-%m-%d").date()).days
                if days_since < 7:
                    skipped_players += 1
                    continue
            
            try:
                injuries = get_player_injuries(p["player_slug"], p["player_id"])
                # Actualizamos caché
                cache[player_id_str] = today_str                
                # Filter injuries by from_date if provided
                if from_date_obj:
                    filtered_injuries = []
                    for inj in injuries:
                        date_from = inj.get("date_from")
                        if date_from:
                            try:
                                # Handle various date formats from Transfermarkt
                                if isinstance(date_from, str):
                                    # Try common formats
                                    for fmt in ["%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"]:
                                        try:
                                            inj_date = datetime.strptime(date_from, fmt).date()
                                            break
                                        except ValueError:
                                            continue
                                    else:
                                        # Could not parse date, include it
                                        filtered_injuries.append(inj)
                                        continue
                                else:
                                    inj_date = date_from

                                if inj_date >= from_date_obj:
                                    filtered_injuries.append(inj)
                            except Exception:
                                # If we can't parse, include it
                                filtered_injuries.append(inj)
                        else:
                            filtered_injuries.append(inj)
                    injuries = filtered_injuries

                for inj in injuries:
                    inj["player_id_tm"] = p["player_id"]
                    inj["player_name"]  = p["player_name"]
                    inj["team_slug"]    = team_slug
                    inj["batch_id"]     = batch_id
                team_injuries.extend(injuries)
            except Exception as e:
                log.warning("%s â€” lesiones fallidas: %s", p["player_name"], e)

        # Guardar JSON crudos por equipo
        _save_json(players,       team_dir / "players.json")
        _save_json(team_injuries, team_dir / "injuries.json")

        all_players.extend(players)
        all_injuries.extend(team_injuries)

        print(f"  [OK] {len(players)} jugadores | {len(team_injuries)} lesiones")
        
        # Guardar estado de caché tras cada equipo
        save_cache(cache)

    print(f"\n  Equipos procesados: {len(teams) - len(failed)}/{len(teams)}")
    if not full_refresh:
        print(f"  Jugadores omitidos por caché (<7 días): {skipped_players}")
    if failed:
        print(f"  [WARNING] Fallidos: {failed}")

    # Guardar estado de caché
    save_cache(cache)

    if all_players:
        df_players = transform_players(all_players)
        df_injuries = transform_injuries(all_injuries)
        season_dir = OUTPUT_DIR / f"season={season}"
        season_dir.mkdir(parents=True, exist_ok=True)
        df_players.to_csv(season_dir / "transfermarkt_players.csv", index=False, encoding="utf-8-sig")
        df_injuries.to_csv(season_dir / "transfermarkt_injuries.csv", index=False, encoding="utf-8-sig")

    return all_players, all_injuries


# â”€â”€ TRANSFORM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_players(players_raw: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de jugadores a las columnas de dim_player.

    Columnas generadas (alineadas con create_tables.sql dim_player):
        id_transfermarkt, canonical_name, nationality,
        birth_date, position
    """
    rows = []
    for p in players_raw:
        rows.append({
            "id_transfermarkt": p.get("player_id"),
            "canonical_name":   p.get("player_name"),
            "nationality":      p.get("nationality"),
            "birth_date":       p.get("birth_date"),
            "position":         p.get("position"),
            # Metadatos de procedencia (Ãºtil para resoluciÃ³n en loader)
            "team_slug":        p.get("team_slug"),
            "team_id_tm":       p.get("team_id_tm"),
            "season":           p.get("season"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["id_transfermarkt"] = pd.to_numeric(df["id_transfermarkt"], errors="coerce").astype("Int64")
        df = df.drop_duplicates(subset=["id_transfermarkt"]).sort_values("id_transfermarkt")
    return df.reset_index(drop=True)


def transform_injuries(injuries_raw: list[dict]) -> pd.DataFrame:
    """Adapta la lista cruda de lesiones a las columnas de fact_injuries.

    Columnas generadas (alineadas con create_tables.sql fact_injuries):
        player_id_tm (FK â†’ dim_player.id_transfermarkt),
        season, injury_type, date_from, date_until,
        days_absent, matches_missed
    """
    rows = []
    for inj in injuries_raw:
        rows.append({
            "player_id_tm":  inj.get("player_id_tm"),
            "player_name":   inj.get("player_name"),   # para facilitar el join en loader
            "season":        inj.get("season"),
            "injury_type":   inj.get("injury_type"),
            "date_from":     inj.get("date_from"),
            "date_until":    inj.get("date_until"),
            "days_absent":   inj.get("days_absent"),
            "matches_missed": inj.get("matches_missed"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["player_id_tm"]  = pd.to_numeric(df["player_id_tm"],  errors="coerce").astype("Int64")
        df["days_absent"]   = pd.to_numeric(df["days_absent"],   errors="coerce").astype("Int32")
        df["matches_missed"]= pd.to_numeric(df["matches_missed"],errors="coerce").astype("Int16")
    return df.reset_index(drop=True)


# â”€â”€ HELPERS INTERNOS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _save_json(data, path: Path) -> None:
    """Guarda JSON en disco de forma segura, con soporte para objetos date."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=_json_serializer)


# â”€â”€ MAIN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    """
    Punto de entrada del scraper genérico de Transfermarkt.
    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition", required=True, help='Ej: "La Liga", "Premier League"')
    parser.add_argument("--seasons", nargs="+", type=int, default=[2024])
    parser.add_argument("--update", action="store_true", help="Incremental update")
    parser.add_argument("--from-date", type=str, help="Start date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    from scripts.competitions import get_competition
    comp_config = get_competition(args.competition)
    if not comp_config:
        print(f"Error: Competición '{args.competition}' no encontrada.")
        return

    league_code = comp_config["sources"]["transfermarkt"]["league_code"]
    
    for season_year in args.seasons:
        scrape_transfermarkt(
            league_code=league_code,
            season=season_year,
            competition_name=args.competition,
            from_date=args.from_date
        )

if __name__ == "__main__":
    main()
