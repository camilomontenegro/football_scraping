import requests
from bs4 import BeautifulSoup
import time
import random
import re
from datetime import datetime

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}


# ─────────────────────────────
# HELPERS
# ─────────────────────────────

def parse_date(date_str: str):
    if not date_str or date_str.strip() in ["-", ""]:
        return None

    # Normalizar separadores: puntos y guiones → barras
    date_str = date_str.replace(".", "/").replace("-", "/").strip()

    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            continue

    return None


def extract_id(href: str):
    match = re.search(r"/spieler/(\d+)", href)
    return match.group(1) if match else None


def extract_slug(href: str):
    parts = href.split("/")
    return parts[1] if len(parts) > 1 else None


def request_with_retry(url, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except:
            time.sleep(2 * (i + 1))
    return None


# ─────────────────────────────
# PROFILE (ROBUSTO)
# ─────────────────────────────

def get_player_profile(player_slug: str, player_id: str):

    url = f"https://www.transfermarkt.es/{player_slug}/profil/spieler/{player_id}"
    r = request_with_retry(url)

    if not r:
        return {"nationality": None, "birth_date": None}

    soup = BeautifulSoup(r.text, "html.parser")

    profile = {
        "nationality": None,
        "birth_date": None
    }

    # ✔️ Más robusto: buscar por texto
    items = soup.select("li")

    for item in items:

        text = item.get_text(" ", strip=True).lower()

        if "nacim" in text or "nacimiento" in text:
            # Transfermarkt a veces añade saltos de linea dentro de la fecha: 30/0 \n 04/1992
            raw_text = item.get_text(" ", strip=True)
            # Removemos todos los espacios en blanco para que quede: 30/04/1992
            clean_text = re.sub(r"\s+", "", raw_text)
            
            raw_match = re.search(r"(\d{2}[./]\d{2}[./]\d{4})", clean_text)
            if raw_match:
                profile["birth_date"] = parse_date(raw_match.group(1))

        elif "nacionalidad" in text or "ciudadanía" in text:
            img = item.find("img")
            if img:
                profile["nationality"] = img.get("title")
            else:
                profile["nationality"] = item.get_text(strip=True)

    return profile


# ─────────────────────────────
# SQUAD (FIX POSITION)
# ─────────────────────────────

def get_squad(team_slug, team_id, season):

    url = f"https://www.transfermarkt.es/{team_slug}/kader/verein/{team_id}/saison_id/{season}"
    r = request_with_retry(url)

    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")

    if not table:
        print(f"⚠️ No table for {team_slug}")
        return []

    # Get team country
    country_img = soup.find("img", class_="flaggenrahmen")
    team_country = country_img.get("title") if country_img else None

    players = []

    for row in table.find_all("tr", class_=["odd", "even"]):

        link = row.select_one("td.hauptlink a")
        if not link:
            continue

        href = link.get("href")

        player_id = extract_id(href)
        player_slug = extract_slug(href)

        # ✔️ FIX POSITION (correcto)
        position = None
        nested = row.find("table")
        if nested:
            rows_nested = nested.find_all("tr")
            if len(rows_nested) > 1:
                position = rows_nested[1].get_text(strip=True)

        profile = get_player_profile(player_slug, player_id)

        players.append({
            "player_id": player_id,
            "player_name": link.text.strip(),
            "player_slug": player_slug,
            "position": position,
            "nationality": profile["nationality"],
            "birth_date": profile["birth_date"],
            "team_country": team_country
        })

        time.sleep(random.uniform(1, 2))

    return players

def get_player_injuries(player_slug: str, player_id: str):

    url = f"https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}"
    r = request_with_retry(url)

    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="items")

    if not table:
        return []

    injuries = []

    for row in table.find_all("tr", class_=["odd", "even"]):

        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        season = cols[0].text.strip()
        injury_type = cols[1].text.strip()

        date_from = parse_date(cols[2].text.strip())
        date_until = parse_date(cols[3].text.strip())

        # días
        days_str = cols[4].text.strip()
        days_match = re.search(r'\d+', days_str)
        days_absent = int(days_match.group()) if days_match else None

        # partidos perdidos
        span = cols[5].find("span")
        matches_missed = int(span.text.strip()) if span and span.text.strip().isdigit() else None

        injuries.append({
            "season": season,
            "injury_type": injury_type,
            "date_from": date_from,
            "date_until": date_until,
            "days_absent": days_absent,
            "matches_missed": matches_missed
        })

    time.sleep(random.uniform(1, 2))
    return injuries

# ─────────────────────────────
# LEAGUE TEAMS
# ─────────────────────────────

def get_league_teams(league_code: str = "ES1", season: str = "2020") -> dict:
    """
    Escanea la tabla de la liga y recoge el slug e ID de todos los equipos.
    Devuelve: {"slug_equipo": id_equipo, ...}
    """
    url = f"https://www.transfermarkt.es/laliga/startseite/wettbewerb/{league_code}/saison_id/{season}"
    r = request_with_retry(url)
    if not r:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    teams = {}
    
    for td in soup.select("td.hauptlink.no-border-links"):
        a = td.select_one("a")
        if a:
            parts = a.get("href", "").split("/")
            if "startseite" in parts and len(parts) >= 5:
                teams[parts[1]] = int(parts[4])
                
    return teams