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
    data/raw/<comp>/<season>/transfermarkt/players/<team>.json        ← plantilla de la temporada
    data/raw/<comp>/<season>/transfermarkt/injuries/<player_id>.json  ← historial completo de lesiones
    data/clean/<comp>/<season>/transfermarkt/players.csv              ← dim_player
    data/clean/<comp>/<season>/transfermarkt/injuries.csv             ← fact_injuries

Lesiones: se recorre kader + leistungsdaten (toda la plantilla de la temporada).
Para cada jugador, se descarga su historial COMPLETO de lesiones (todas las
temporadas y clubes) desde /verletzungen/spieler/{id}. Cada registro incluye
el club en el que estaba el jugador en ese momento.
Las URLs de lesiones usan el ID canónico del jugador, no el slug de la tabla.

Caché global:
    data/.cache/transfermarkt_players_last_scraped.json
    (clave: "<player_id>")

Transfermarkt es la fuente CANÓNICA de jugadores. Los loaders/ son
los únicos que escriben en la DB.
"""

from __future__ import annotations

import json
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
    raw_dir,
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
    """Descarga la plantilla actual (kader) de un equipo para una temporada."""
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
    from utils.team_countries import resolve_team_country

    team_country = resolve_team_country(
        "",
        tm_country=flag.get("title") if flag else None,
        tm_id=team_id,
    )

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
        display_name = (link.get("title") or link.text or "").strip()
        players.append({
            "player_id":    player_id,
            "player_name":  display_name,
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


def _player_from_link(link) -> Optional[dict]:
    """Extrae id/slug/nombre desde un enlace de perfil de Transfermarkt."""
    if not link:
        return None
    href = link.get("href", "")
    player_id = extract_player_id(href)
    if not player_id:
        return None
    return {
        "player_id":   player_id,
        "player_slug": extract_player_slug(href),
        "player_name": link.get("title") or link.get_text(strip=True),
    }


def _players_from_table(table) -> list[dict]:
    """Parsea filas de una tabla TM con enlaces a /profil/spieler/."""
    if not table:
        return []
    players: list[dict] = []
    seen: set[str] = set()
    for row in table.find_all("tr", class_=["odd", "even"]):
        link = row.find("a", href=lambda h: h and "/profil/spieler/" in h)
        p = _player_from_link(link)
        if not p or p["player_id"] in seen:
            continue
        seen.add(p["player_id"])
        players.append(p)
    return players


def resolve_player_from_id(player_id: str) -> dict:
    """Obtiene slug y nombre canónicos desde el perfil TM (evita slug/id inconsistentes)."""
    url = f"https://www.transfermarkt.es/-/profil/spieler/{player_id}"
    r = request_with_retry(url)
    if not r:
        return {"player_id": player_id, "player_slug": None, "player_name": None}

    soup = BeautifulSoup(r.text, "html.parser")
    canonical = ""
    link = soup.find("link", rel="canonical")
    if link and link.get("href"):
        canonical = link["href"]
    elif soup.find("meta", property="og:url"):
        canonical = soup.find("meta", property="og:url")["content"]

    slug = extract_player_slug(canonical) if canonical else None
    name_el = soup.find("h1")
    return {
        "player_id":   str(player_id),
        "player_slug": slug,
        "player_name": name_el.get_text(strip=True) if name_el else None,
    }


def get_season_roster(team_slug: str, team_id: int, season: int) -> list[dict]:
    """Jugadores que aparecen en las estadísticas de la temporada (incluye bajas)."""
    url = (
        f"https://www.transfermarkt.es/{team_slug}/leistungsdaten/verein/{team_id}"
        f"/plus/1?saison_id={season}"
    )
    r = request_with_retry(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    return _players_from_table(soup.find("table", class_="items"))


def get_season_transfer_players(team_slug: str, team_id: int, season: int) -> list[dict]:
    """Jugadores con fichaje o salida en la temporada (altas + bajas).

    Nota: TM a veces enlaza perfiles homónimos incorrectos; siempre validar con
    `resolve_player_from_id()` antes de usar estos registros.
    """
    url = (
        f"https://www.transfermarkt.es/{team_slug}/transfers/verein/{team_id}"
        f"/saison_id={season}"
    )
    r = request_with_retry(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    players: list[dict] = []
    seen: set[str] = set()
    for table in soup.find_all("table", class_="items"):
        for p in _players_from_table(table):
            if p["player_id"] in seen:
                continue
            seen.add(p["player_id"])
            players.append(p)
    return players


def _merge_season_players(
    team_slug: str,
    team_id: int,
    team_country: Optional[str],
    *groups: list[dict],
) -> list[dict]:
    """Une listas de jugadores deduplicando por id; prioriza el registro más completo."""
    merged: dict[str, dict] = {}
    for group in groups:
        for p in group:
            pid = str(p["player_id"])
            if pid not in merged:
                merged[pid] = dict(p)
                continue
            for key, val in p.items():
                if val and not merged[pid].get(key):
                    merged[pid][key] = val

    result: list[dict] = []
    for p in merged.values():
        if not p.get("player_slug") or not p.get("player_name"):
            resolved = resolve_player_from_id(str(p["player_id"]))
            p.setdefault("player_slug", resolved.get("player_slug"))
            p.setdefault("player_name", resolved.get("player_name"))
        if not p.get("birth_date") or not p.get("nationality"):
            slug = p.get("player_slug")
            pid = p.get("player_id")
            if slug and pid:
                profile = get_player_profile(slug, pid)
                if not p.get("birth_date"):
                    p["birth_date"] = profile.get("birth_date")
                if not p.get("nationality"):
                    p["nationality"] = profile.get("nationality")
        p.setdefault("team_slug", team_slug)
        p.setdefault("team_id_tm", team_id)
        p.setdefault("team_country", team_country)
        result.append(p)
    return result


def _players_for_injury_scrape(
    team_slug: str,
    team_id: int,
    season: int,
    kader_players: list[dict],
) -> list[dict]:
    """Plantilla completa de la temporada: kader + leistungsdaten (sin fichajes)."""
    team_country = kader_players[0].get("team_country") if kader_players else None
    season_roster = get_season_roster(team_slug, team_id, season)
    return _merge_season_players(
        team_slug, team_id, team_country,
        kader_players, season_roster,
    )



def _load_cached_player_injuries(
    competition_name: str,
    season_label: str,
    player_id: str,
) -> list[dict]:
    """Reutiliza lesiones ya guardadas en raw JSON por player_id."""
    path = raw_dir(competition_name, season_label, "transfermarkt", "injuries") / f"{player_id}.json"
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    return data.get("injuries", [])


def _extract_club_from_td(td) -> dict:
    """Extrae club name / id / slug del td class=wappen_verletzung.

    Estructura HTML:
        <td class="rechts hauptlink wappen_verletzung">
            <a href="/club-slug/startseite/verein/{id}/saison_id/{year}" title="Club Name">
                <img ... title="Club Name"/>
            </a>
            <span>7</span>          ← matches_missed
        </td>
    Puede haber >1 club si el jugador estuvo en varios durante la lesión;
    se toma el primero como principal.
    """
    link = td.find("a", href=lambda h: h and "/verein/" in h)
    if not link:
        return {"club_name": None, "club_id_tm": None, "club_slug": None}
    href = link.get("href", "")
    club_id_m = re.search(r"/verein/(\d+)", href)
    parts = href.split("/")
    return {
        "club_name": link.get("title") or (link.find("img") or {}).get("title"),
        "club_id_tm": int(club_id_m.group(1)) if club_id_m else None,
        "club_slug":  parts[1] if len(parts) > 1 else None,
    }


def get_player_injuries(player_slug: str, player_id: str) -> list[dict]:
    """Historial COMPLETO de lesiones de un jugador (todas las temporadas).

    Cada registro incluye temporada, tipo de lesión, fechas, días de baja,
    partidos perdidos y el club en el que estaba el jugador en ese momento.
    """
    # URL por ID: evita lesiones de otro jugador cuando slug e id no coinciden.
    url = f"https://www.transfermarkt.es/-/verletzungen/spieler/{player_id}"
    r = request_with_retry(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    canonical = soup.find("link", rel="canonical")
    if canonical and canonical.get("href"):
        resolved_slug = extract_player_slug(canonical["href"])
        if resolved_slug and player_slug and resolved_slug != player_slug:
            log.debug(
                "Slug corregido para lesiones: %s → %s (id=%s)",
                player_slug, resolved_slug, player_id,
            )
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
        club = _extract_club_from_td(cols[5])
        injuries.append({
            "season":         cols[0].text.strip(),
            "injury_type":    cols[1].text.strip(),
            "date_from":      parse_date(cols[2].text.strip()),
            "date_until":     parse_date(cols[3].text.strip()),
            "days_absent":    int(days_m.group()) if days_m else None,
            "matches_missed": int(span.text.strip()) if span and span.text.strip().isdigit() else None,
            "club_name":      club["club_name"],
            "club_id_tm":     club["club_id_tm"],
            "club_slug":      club["club_slug"],
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

    log.info("Lesiones: historial completo por jugador")

    from utils.batch import generate_batch_id
    batch_id = generate_batch_id()

    from scripts.competitions import get_competition_slug_transfermarkt
    tm_slug = get_competition_slug_transfermarkt(competition_name) or "laliga"

    if not teams:
        teams_list = get_league_teams(season, tm_slug, league_code)
        teams = {t["team_slug"]: t["team_id"] for t in teams_list}
        team_names = {t["team_slug"]: t["team_name"] for t in teams_list}
        log.info("Auto-descubiertos %d equipos para %s %d", len(teams), league_code, season)
    else:
        team_names = {}

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
        kader_players: list[dict] = []
        for attempt in range(MAX_RETRIES):
            try:
                kader_players = get_squad(team_slug, team_id, season)
                if kader_players:
                    break
            except Exception as e:
                log.warning("%s intento %d: %s", team_slug, attempt + 1, e)
            time.sleep(2 * (attempt + 1))

        if not kader_players:
            log.error("%s sin datos de plantilla", team_slug)
            failed.append(team_slug)
            continue

        team_country = kader_players[0].get("team_country")
        players = _players_for_injury_scrape(team_slug, team_id, season, kader_players)
        extra_players = len(players) - len(kader_players)
        if extra_players:
            log.info(
                "%s: %d kader + %d leistungsdaten = %d jugadores para lesiones",
                team_slug, len(kader_players), extra_players, len(players),
            )

        for p in players:
            p["season"]      = season
            p["competition"] = competition_name
            p["team_name"]   = team_names.get(team_slug) or team_slug.replace("-", " ").title()
            p["batch_id"]    = batch_id

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
                        injuries = _load_cached_player_injuries(
                            competition_name, season_label, player_id_str,
                        )
                        for inj in injuries:
                            row = dict(inj)
                            row["player_id_tm"] = p["player_id"]
                            row["player_name"]  = p["player_name"]
                            row["squad_team_slug"] = team_slug
                            row["batch_id"]     = batch_id
                            team_injuries.append(row)
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
                    inj["squad_team_slug"] = team_slug
                    inj["batch_id"]     = batch_id

                # Guardar raw por jugador (historial completo)
                save_raw_json(
                    competition_name, season_label, "transfermarkt",
                    player_id_str,
                    {
                        "batch_id":    batch_id,
                        "player_id":   player_id_str,
                        "player_name": p["player_name"],
                        "player_slug": p["player_slug"],
                        "injuries":    injuries,
                    },
                    subdir="injuries",
                )

                team_injuries.extend(injuries)
            except Exception as e:
                log.warning("%s — lesiones fallidas: %s", p["player_name"], e)

        save_raw_json(
            competition_name, season_label, "transfermarkt",
            team_slug, {"batch_id": batch_id, "players": players},
            subdir="players",
        )

        all_players.extend(players)
        all_injuries.extend(team_injuries)

        print(
            f"  [OK] {len(players)} jugadores "
            f"({len(kader_players)} kader + {extra_players} temporada) "
            f"| {len(team_injuries)} lesiones (historial completo)"
        )
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
    """Adapta a las columnas de `dim_player` + metadatos de procedencia."""
    rows = [{
        "id_transfermarkt": p.get("player_id"),
        "canonical_name":   p.get("player_name"),
        "nationality":      p.get("nationality"),
        "birth_date":       p.get("birth_date"),
        "position":         p.get("position"),
        "competition":      p.get("competition"),
        "team_name":        p.get("team_name") or p.get("team_slug"),
        "team_slug":        p.get("team_slug"),
        "team_id_tm":       p.get("team_id_tm"),
        "season":           p.get("season"),
        "source":           "transfermarkt",
    } for p in players_raw]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["id_transfermarkt"] = pd.to_numeric(df["id_transfermarkt"], errors="coerce").astype("Int64")
        df = df.drop_duplicates(subset=["id_transfermarkt"]).sort_values("id_transfermarkt")
    return df.reset_index(drop=True)


def transform_injuries(injuries_raw: list[dict]) -> pd.DataFrame:
    """Adapta a las columnas de `fact_injuries`.

    Cada fila representa una lesión histórica del jugador con el club
    en el que estaba en ese momento (extraído de la página de lesiones).
    ``squad_team_slug`` indica desde qué plantilla se descubrió al jugador.
    """
    rows = [{
        "player_id_tm":    inj.get("player_id_tm"),
        "player_name":     inj.get("player_name"),
        "season":          inj.get("season"),
        "injury_type":     inj.get("injury_type"),
        "date_from":       inj.get("date_from"),
        "date_until":      inj.get("date_until"),
        "days_absent":     inj.get("days_absent"),
        "matches_missed":  inj.get("matches_missed"),
        "club_name":       inj.get("club_name"),
        "club_id_tm":      inj.get("club_id_tm"),
        "club_slug":       inj.get("club_slug"),
        "squad_team_slug": inj.get("squad_team_slug"),
    } for inj in injuries_raw]
    df = pd.DataFrame(rows)
    if not df.empty:
        df["player_id_tm"]   = pd.to_numeric(df["player_id_tm"], errors="coerce").astype("Int64")
        df["days_absent"]    = pd.to_numeric(df["days_absent"], errors="coerce").astype("Int32")
        df["matches_missed"] = pd.to_numeric(df["matches_missed"], errors="coerce").astype("Int16")
        df["club_id_tm"]     = pd.to_numeric(df["club_id_tm"], errors="coerce").astype("Int64")
    return df.reset_index(drop=True)


# ── ATTENDANCE SCRAPER ────────────────────────────────────────────────────────

def _parse_attendance(raw: str) -> Optional[int]:
    """Parse attendance string like '81.044 Zuschauer' or '45,012' into int."""
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if digits:
        val = int(digits)
        return val if val > 100 else None
    return None


def scrape_transfermarkt_attendance(
    competition_name: str = "La Liga",
    league_code: str = LEAGUE_CODE,
    season: int = 2024,
    max_matchdays: int = 50,
) -> list[dict]:
    """Scrape attendance per match from Transfermarkt gesamtspielplan page.

    Uses the full-season schedule page (1 request) instead of paginating
    through individual matchdays. Falls back to matchday-by-matchday if
    the gesamtspielplan doesn't return data.

    Args:
        competition_name: Name as in competitions.py (e.g. "La Liga")
        league_code: Transfermarkt league code (e.g. "ES1")
        season: Start year of the season (e.g. 2024 for 2024/25)
        max_matchdays: Maximum matchdays for fallback mode

    Returns:
        List of dicts with keys: home_team, away_team, match_date,
        home_score, away_score, attendance, matchday, competition, season
    """
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(name)s - %(message)s")

    from scripts.competitions import get_competition_slug_transfermarkt

    slug = get_competition_slug_transfermarkt(competition_name)
    if not slug:
        print(f"[ERROR] No se encontro slug de Transfermarkt para '{competition_name}'")
        return []

    season_label = f"{season}_{season + 1}"
    print(f"\n{'='*60}")
    print(f"  Attendance scraper: {competition_name} {season}/{season+1}")
    print(f"  Slug: {slug}  |  League code: {league_code}")
    print(f"{'='*60}")

    # ── Estrategia 1: gesamtspielplan (toda la temporada en 1 request) ────
    all_matches = _scrape_gesamtspielplan(
        slug, league_code, season, season_label, competition_name,
    )

    # ── Estrategia 2: fallback jornada por jornada ────────────────────────
    if not all_matches:
        print("  [!] gesamtspielplan vacio, probando jornada por jornada...")
        all_matches = _scrape_matchday_by_matchday(
            slug, league_code, season, season_label,
            competition_name, max_matchdays,
        )

    # ── Resultados ────────────────────────────────────────────────────────
    with_attendance = [m for m in all_matches if m.get("attendance")]
    print(f"\n  Resultado: {len(all_matches)} partidos, {len(with_attendance)} con asistencia")

    if all_matches:
        df = pd.DataFrame(all_matches)
        csv_path = save_clean_csv(
            competition_name, season_label, "transfermarkt", "attendance", df,
        )
        print(f"  CSV guardado: {csv_path}")
    else:
        print("  [!] No se obtuvieron partidos.")

    return all_matches


def _parse_match_rows(soup, competition_name: str, season_label: str,
                      matchday: Optional[int] = None) -> list[dict]:
    """Parse match rows from a TM page. Works for both gesamtspielplan and spieltag."""
    matches = []
    current_date = None
    current_matchday = matchday

    # TM uses table rows. Date rows and match rows alternate.
    for tr in soup.find_all("tr"):
        classes = tr.get("class", [])

        # ── Date/matchday header row ──
        # These are often in <td colspan=...> or a row with just a date
        td_date = tr.find("td", colspan=True)
        if td_date:
            text = td_date.get_text(strip=True)
            # Extract date: "Vie 16/08/2024" or "16.08.2024" or "16/08/2024"
            dm = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", text)
            if dm:
                d, m, y = dm.group(1), dm.group(2), dm.group(3)
                if len(y) == 2:
                    y = "20" + y
                current_date = f"{y}-{int(m):02d}-{int(d):02d}"
            # Extract matchday: "1. Jornada" or "Jornada 1" or "1. Spieltag"
            md = re.search(r"(\d+)\.\s*(?:Jornada|Spieltag|Matchday|matchday)", text)
            if not md:
                md = re.search(r"(?:Jornada|Spieltag|Matchday)\s*(\d+)", text, re.IGNORECASE)
            if md:
                current_matchday = int(md.group(1))
            continue

        # ── Match result row ──
        # Find team links — TM uses various classes
        team_links = tr.find_all("a", class_="vereinprofil_tooltip")
        if not team_links:
            # Fallback: look for any links with /verein/ in href
            team_links = [a for a in tr.find_all("a", href=True)
                          if "/verein/" in a.get("href", "")
                          and a.get_text(strip=True)]

        if len(team_links) < 2:
            continue

        home_team = team_links[0].get("title") or team_links[0].get_text(strip=True)
        away_team = team_links[1].get("title") or team_links[1].get_text(strip=True)
        if not home_team or not away_team:
            continue

        # ── Score ──
        home_score = away_score = None
        score_link = tr.find("a", class_="ergebnis-link")
        if not score_link:
            # Fallback: look for score pattern in any link or span
            for el in tr.find_all(["a", "span"]):
                t = el.get_text(strip=True)
                sm = re.match(r"^(\d+)\s*:\s*(\d+)$", t)
                if sm:
                    score_link = el
                    break
        if score_link:
            sm = re.match(r"(\d+)\s*:\s*(\d+)", score_link.get_text(strip=True))
            if sm:
                home_score = int(sm.group(1))
                away_score = int(sm.group(2))

        # ── Date from row (some layouts have date in a cell) ──
        match_date = current_date
        for td in tr.find_all("td", class_="zentriert"):
            text = td.get_text(strip=True)
            dm = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", text)
            if dm:
                d, m, y = dm.group(1), dm.group(2), dm.group(3)
                if len(y) == 2:
                    y = "20" + y
                match_date = f"{y}-{int(m):02d}-{int(d):02d}"
                break

        # ── Attendance ──
        attendance = None
        tds = tr.find_all("td")
        for td in tds:
            td_text = td.get_text(strip=True)
            # Attendance: pure digits with dots like "81.044" or a number > 100
            if re.match(r"^[\d.]+$", td_text) and len(td_text) >= 3:
                candidate = _parse_attendance(td_text)
                if candidate:
                    attendance = candidate
            # Also check for "Zuschauer" text nearby
            if "zuschauer" in td_text.lower() or "espectadores" in td_text.lower():
                candidate = _parse_attendance(td_text)
                if candidate:
                    attendance = candidate

        matches.append({
            "home_team":    home_team,
            "away_team":    away_team,
            "match_date":   match_date,
            "home_score":   home_score,
            "away_score":   away_score,
            "attendance":   attendance,
            "matchday":     current_matchday,
            "competition":  competition_name,
            "season":       season_label,
        })

    return matches


def _scrape_gesamtspielplan(
    slug: str, league_code: str, season: int,
    season_label: str, competition_name: str,
) -> list[dict]:
    """Intenta descargar toda la temporada desde gesamtspielplan (1 sola request)."""
    url = (
        f"https://www.transfermarkt.es/{slug}"
        f"/gesamtspielplan/wettbewerb/{league_code}"
        f"/saison_id/{season}"
    )
    print(f"  Probando gesamtspielplan: {url}")
    r = request_with_retry(url)
    if not r:
        print(f"  [!] Sin respuesta de gesamtspielplan")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    matches = _parse_match_rows(soup, competition_name, season_label)
    print(f"  gesamtspielplan: {len(matches)} partidos encontrados")
    return matches


def _scrape_matchday_by_matchday(
    slug: str, league_code: str, season: int,
    season_label: str, competition_name: str, max_matchdays: int,
) -> list[dict]:
    """Fallback: recorre jornada por jornada."""
    all_matches: list[dict] = []
    empty_streak = 0

    for matchday in range(1, max_matchdays + 1):
        url = (
            f"https://www.transfermarkt.es/{slug}"
            f"/spieltag/wettbewerb/{league_code}"
            f"/saison_id/{season}/spieltag/{matchday}"
        )
        r = request_with_retry(url)
        if not r:
            empty_streak += 1
            if empty_streak >= 3:
                print(f"  3 jornadas sin respuesta — fin (jornada {matchday}).")
                break
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        matches = _parse_match_rows(soup, competition_name, season_label, matchday)

        if not matches:
            empty_streak += 1
            if empty_streak >= 3:
                print(f"  3 jornadas vacias seguidas — fin (jornada {matchday}).")
                break
        else:
            empty_streak = 0
            with_att = sum(1 for m in matches if m.get("attendance"))
            print(f"  Jornada {matchday}: {len(matches)} partidos, {with_att} con asistencia")
            all_matches.extend(matches)

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    return all_matches


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
    parser.add_argument("--attendance", action="store_true",
                        help="Solo scrapear asistencia (attendance) por jornada.")
    args = parser.parse_args()

    from scripts.competitions import get_competition
    comp_config = get_competition(args.competition)
    if not comp_config:
        print(f"Error: Competición '{args.competition}' no encontrada.")
        return

    league_code = comp_config["sources"]["transfermarkt"]["league_code"]

    if args.attendance:
        for season_year in args.seasons:
            scrape_transfermarkt_attendance(
                competition_name=args.competition,
                league_code=league_code,
                season=season_year,
            )
        return

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
