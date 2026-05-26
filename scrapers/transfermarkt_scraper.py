"""
scrapers/transfermarkt_scraper.py
==================================
Scraper unificado de Transfermarkt (plantillas + lesiones).

Estructura:
    1. CONSTANTS       — configuración del scraper
    2. HELPERS         — parse_date, extract_id, request_with_retry
    3. FETCH           — funciones puras de obtención de datos
    4. ORCHESTRATOR    — scrape_transfermarkt() acumula todo
    5. TRANSFORM       — adapta campos al esquema de la DB
    6. MAIN            — scrape → transform → guardar en disco

Rutas estándar (`utils.data_paths`):
    data/raw/<comp>/<season>/transfermarkt/players/<team>.json    ← plantilla cruda
    data/raw/<comp>/<season>/transfermarkt/injuries/<team>.json   ← lesiones crudas
    data/clean/<comp>/<season>/transfermarkt/players.csv          ← dim_player
    data/clean/<comp>/<season>/transfermarkt/injuries.csv         ← fact_injuries

Caché global:
    data/.cache/transfermarkt_players_last_scraped.json
    (clave: "<player_id>")

Transfermarkt es la fuente CANÓNICA de jugadores. Los loaders/ son
los únicos que escriben en la DB.
"""

from __future__ import annotations

import logging
import random
import re
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# Permite ejecutar como script suelto
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.data_paths import (
    save_raw_json,
    save_clean_csv,
    load_cache as _load_cache_file,
    save_cache as _save_cache_file,
)

log = logging.getLogger(__name__)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

LEAGUE_CODE = "ES1"       # La Liga en Transfermarkt
SEASONS     = [2020, 2021, 2022, 2023, 2024, 2025]
DELAY_MIN   = 3.0
DELAY_MAX   = 6.0
MAX_RETRIES = 3

CACHE_NAME = "transfermarkt_players"   # data/.cache/transfermarkt_players_last_scraped.json

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


def load_cache() -> dict:
    return _load_cache_file(CACHE_NAME)


def save_cache(cache: dict) -> None:
    _save_cache_file(CACHE_NAME, cache)


# ── HELPERS ──────────────────────────────────────────────────────────────────

def parse_date(date_str: str) -> Optional[date]:
    """Convierte una cadena de fecha en date. Acepta dd/mm/yyyy, dd.mm.yyyy, etc."""
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
    """Serializer compatible con json.dump para date/datetime."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def extract_player_id(href: str) -> Optional[str]:
    """`/lionel-messi/profil/spieler/28003` → `'28003'`."""
    m = re.search(r"/spieler/(\d+)", href)
    return m.group(1) if m else None


def extract_player_slug(href: str) -> Optional[str]:
    """`/lionel-messi/profil/spieler/28003` → `'lionel-messi'`."""
    parts = href.split("/")
    return parts[1] if len(parts) > 1 else None


def request_with_retry(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """GET con reintentos exponenciales."""
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("Intento %d/%d fallido para %s: %s", i + 1, retries, url, e)
            time.sleep(2 * (i + 1))
    return None


# Wrapper retro-compatible para módulos que aún lo importan.
def _save_json(data, path: Path) -> None:
    """Guarda JSON con date-aware serializer. Compat con código antiguo."""
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=_json_serializer)


def search_player_by_name(name: str) -> list[dict]:
    """Busca jugadores en Transfermarkt por nombre.

    Returns:
        Lista de dicts con keys: player_name, player_id, player_slug, club, age.
    """
    url = f"https://www.transfermarkt.es/schnellsuche/ergebnis/schnellsuche?query={name}&x=0&y=0"
    r = request_with_retry(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    # Find player results table
    player_table = None
    for header in soup.find_all("div", class_="table-header"):
        if "jugador" in header.get_text(strip=True).lower() or "player" in header.get_text(strip=True).lower():
            player_table = header.find_next("table")
            break

    if not player_table:
        return []

    for row in player_table.find_all("tr", class_=["odd", "even"]):
        try:
            name_cell = row.find("td", class_="hauptlink")
            if not name_cell:
                continue
            link = name_cell.find("a")
            if not link:
                continue

            href = link.get("href", "")
            parts = href.split("/")
            player_slug = parts[1] if len(parts) > 1 else ""
            player_id = parts[-1] if parts else ""

            results.append({
                "player_name": link.get_text(strip=True),
                "player_id": player_id,
                "player_slug": player_slug,
            })
        except Exception:
            continue

    return results


# ── FETCH ────────────────────────────────────────────────────────────────────

def get_league_teams(season: int, competition_slug: str, league_code: str) -> list[dict]:
    """Descarga los equipos participantes de una competición + temporada."""
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

    teams: list[dict] = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        try:
            anchor = row.find("a", title=True)
            if not anchor:
                continue
            href = anchor.get("href", "")
            parts = href.split("/")
            if len(parts) < 5 or parts[2] != "startseite" or parts[3] != "verein":
                continue
            teams.append({
                "team_id":   int(parts[4]),
                "team_slug": parts[1],
                "team_name": anchor.get("title"),
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

    for label in soup.find_all("span", class_="info-table__content--regular"):
        val = label.find_next_sibling("span")
        if not val:
            continue
        text_label = label.text.strip().lower()
        if "nacim" in text_label or "birth" in text_label:
            raw = val.text.split("(")[0]
            m = re.search(r"\d{2}/\d{2}/\d{4}", raw)
            if m:
                profile["birth_date"] = parse_date(m.group())
        elif "nacionalidad" in text_label or "citizenship" in text_label:
            img = val.find("img")
            profile["nationality"] = img.get("title") if img else val.get_text(strip=True)

    return profile


def get_squad(team_slug: str, team_id: int, season: int) -> list[dict]:
    """Descarga la plantilla de un equipo para una temporada."""
    url = (
        f"https://www.transfermarkt.es/{team_slug}/kader"
        f"/verein/{team_id}/saison_id/{season}"
    )
    r = request_with_retry(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")
    if not table:
        log.warning("Sin tabla de plantilla para %s", team_slug)
        return []

    flag = soup.find("img", class_="flaggenrahmen")
    team_country = flag.get("title") if flag else None

    players: list[dict] = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        link = row.select_one("td.hauptlink a")
        if not link:
            continue
        href        = link.get("href", "")
        player_id   = extract_player_id(href)
        player_slug = extract_player_slug(href)

        position = None
        nested = row.find("table")
        if nested:
            nested_rows = nested.find_all("tr")
            if len(nested_rows) > 1:
                position = nested_rows[1].get_text(strip=True)

        nationality_table = None
        tds = row.find_all("td")
        if len(tds) > 6:
            nat_img = tds[6].find("img")
            if nat_img:
                nationality_table = nat_img.get("title")

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
    """Historial de lesiones de un jugador."""
    url = f"https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}"
    r = request_with_retry(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")
    if not table:
        return []

    injuries: list[dict] = []
    for row in table.find_all("tr", class_=["odd", "even"]):
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        days_m = re.search(r"\d+", cols[4].text.strip())
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


# ── ORCHESTRATOR ─────────────────────────────────────────────────────────────

def scrape_transfermarkt(
    competition_name: Optional[str] = None,
    league_code: str = LEAGUE_CODE,
    season: Optional[int] = None,
    teams: Optional[dict[str, int]] = None,
    from_date: Optional[str] = None,
    full_refresh: bool = False,
    season_label: Optional[str] = None,
) -> tuple[list[dict], list[dict]]:
    """
    Descarga plantillas y lesiones de todos los equipos de la competición.

    Returns:
        (all_players, all_injuries) — listas de dicts crudos.
    """
    from_date_obj = None
    if from_date:
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        log.info("Filtrando lesiones desde: %s", from_date_obj)

    if season is None:
        season = SEASONS[0]
    if season_label is None:
        season_label = f"{season}_{season + 1}"
    if competition_name is None:
        competition_name = "La Liga"

    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    from scripts.competitions import get_competition_slug_transfermarkt
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

    for team_slug, team_id in teams.items():
        print(f"\n[INFO] Equipo: {team_slug} (id={team_id})")

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

        for p in players:
            p["season"]   = season
            p["batch_id"] = batch_id

        team_injuries: list[dict] = []
        for p in players:
            player_id_str = str(p["player_id"])
            last_scraped = cache.get(player_id_str)
            if not full_refresh and last_scraped:
                try:
                    days_since = (date.today() - datetime.strptime(
                        last_scraped, "%Y-%m-%d").date()).days
                    if days_since < 7:
                        skipped_players += 1
                        continue
                except Exception:
                    pass

            try:
                injuries = get_player_injuries(p["player_slug"], p["player_id"])
                cache[player_id_str] = today_str
                if from_date_obj:
                    filtered: list[dict] = []
                    for inj in injuries:
                        date_from = inj.get("date_from")
                        if date_from is None:
                            filtered.append(inj)
                            continue
                        try:
                            inj_date = date_from
                            if isinstance(date_from, str):
                                for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
                                    try:
                                        inj_date = datetime.strptime(date_from, fmt).date()
                                        break
                                    except ValueError:
                                        continue
                                else:
                                    filtered.append(inj)
                                    continue
                            if inj_date >= from_date_obj:
                                filtered.append(inj)
                        except Exception:
                            filtered.append(inj)
                    injuries = filtered

                for inj in injuries:
                    inj["player_id_tm"] = p["player_id"]
                    inj["player_name"]  = p["player_name"]
                    inj["team_slug"]    = team_slug
                    inj["batch_id"]     = batch_id
                team_injuries.extend(injuries)
            except Exception as e:
                log.warning("%s — lesiones fallidas: %s", p["player_name"], e)

        save_raw_json(
            competition_name, season_label, "transfermarkt",
            team_slug, {"batch_id": batch_id, "players": players},
            subdir="players",
        )
        save_raw_json(
            competition_name, season_label, "transfermarkt",
            team_slug, {"batch_id": batch_id, "injuries": team_injuries},
            subdir="injuries",
        )

        all_players.extend(players)
        all_injuries.extend(team_injuries)

        print(f"  [OK] {len(players)} jugadores | {len(team_injuries)} lesiones")
        save_cache(cache)

    print(f"\n  Equipos procesados: {len(teams) - len(failed)}/{len(teams)}")
    if not full_refresh:
        print(f"  Jugadores omitidos por caché (<7 días): {skipped_players}")
    if failed:
        print(f"  [WARNING] Fallidos: {failed}")

    save_cache(cache)

    if all_players:
        df_players = transform_players(all_players)
        df_injuries = transform_injuries(all_injuries)
        p_csv = save_clean_csv(competition_name, season_label, "transfermarkt",
                               "players", df_players)
        i_csv = save_clean_csv(competition_name, season_label, "transfermarkt",
                               "injuries", df_injuries)
        print(f"  CSV jugadores: {p_csv}")
        print(f"  CSV lesiones:  {i_csv}")

    return all_players, all_injuries


# ── TRANSFORM ────────────────────────────────────────────────────────────────

def transform_players(players_raw: list[dict]) -> pd.DataFrame:
    """Adapta a las columnas de `dim_player`."""
    rows = [{
        "id_transfermarkt": p.get("player_id"),
        "canonical_name":   p.get("player_name"),
        "nationality":      p.get("nationality"),
        "birth_date":       p.get("birth_date"),
        "position":         p.get("position"),
        "team_slug":        p.get("team_slug"),
        "team_id_tm":       p.get("team_id_tm"),
        "season":           p.get("season"),
    } for p in players_raw]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["id_transfermarkt"] = pd.to_numeric(df["id_transfermarkt"], errors="coerce").astype("Int64")
        df = df.drop_duplicates(subset=["id_transfermarkt"]).sort_values("id_transfermarkt")
    return df.reset_index(drop=True)


def transform_injuries(injuries_raw: list[dict]) -> pd.DataFrame:
    """Adapta a las columnas de `fact_injuries`."""
    rows = [{
        "player_id_tm":   inj.get("player_id_tm"),
        "player_name":    inj.get("player_name"),
        "season":         inj.get("season"),
        "injury_type":    inj.get("injury_type"),
        "date_from":      inj.get("date_from"),
        "date_until":     inj.get("date_until"),
        "days_absent":    inj.get("days_absent"),
        "matches_missed": inj.get("matches_missed"),
    } for inj in injuries_raw]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["player_id_tm"]   = pd.to_numeric(df["player_id_tm"], errors="coerce").astype("Int64")
        df["days_absent"]    = pd.to_numeric(df["days_absent"], errors="coerce").astype("Int32")
        df["matches_missed"] = pd.to_numeric(df["matches_missed"], errors="coerce").astype("Int16")
    return df.reset_index(drop=True)


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition", required=True,
                        help='Ej: "La Liga", "Premier League"')
    parser.add_argument("--seasons", nargs="+", type=int, default=[2024])
    parser.add_argument("--update", action="store_true", help="Incremental update")
    parser.add_argument("--from-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignora caché de 7 días")
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
            from_date=args.from_date,
            full_refresh=args.full_refresh,
        )


if __name__ == "__main__":
    main()
