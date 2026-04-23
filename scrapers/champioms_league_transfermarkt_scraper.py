r"""
Scraper de Transfermarkt para la UEFA Champions League.
 
Extrae para las temporadas 2020/21 → 2024/25:
    - Equipos:   team_id, team_slug, team_name
    - Jugadores: player_id, player_slug, player_name, position,
                 nationality, birth_date, team, season
    - Lesiones:  season, injury_type, date_from, date_until,
                 days_absent, matches_missed, player_id
 
La URL de la Champions en Transfermarkt usa el código CL:
    https://www.transfermarkt.es/uefa-champions-league/teilnehmer/pokalwettbewerb/CL/saison_id/{season}
 
Salida (data/raw/transfermarkt/champions/):
    transfermarkt_champions_teams.csv
    transfermarkt_champions_players.csv
    transfermarkt_champions_injuries.csv
 
Uso:
    python transfermarkt_champions_scraper.py
"""

r"""
La estructura de la ruta en Transfermarket  usa términos en alemán.

Términos en alemán y sus  traducciones en castellano 

Verletzungen  → "Lesiones".
Spieler → "Jugador".
Kader → Plantilla
Verein → Club
Startseite → "Página de inicio".
Slug → parte corta de la URL que identifica la página ej www.web.de/startseite  -> slug es startseite
wettbewerb → competición
teilnehmer -> "participantes" 
pokalwettbewerb -> "competición de copa" 
zentriert → centrado
hauptlink → enlace principal
rechts → derecha


"""
 
import os
import re
import time
import random
from datetime import datetime, date
from typing import Optional
 
import requests
import pandas as pd
from bs4 import BeautifulSoup
 
 
# ══════════════════════════════════════════════════
# CONSTANTES
# ══════════════════════════════════════════════════
 
LEAGUE_CODE = "CL"   # Champions League en Transfermarkt
SEASONS     = [2020, 2021, 2022, 2023, 2024]
 
DELAY_MIN   = 2.0    # pausa mínima entre peticiones (segundos)
DELAY_MAX   = 4.0    # pausa máxima entre peticiones (segundos)
MAX_RETRIES = 3
 
OUTPUT_DIR = os.path.join("data", "raw", "transfermarkt", "champions")
 
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}
 
 
# ══════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════
 
def request_with_retry(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """
    Hace una petición GET con reintentos y backoff exponencial.
 
    Parámetros:
        url     (str): URL a descargar
        retries (int): número máximo de intentos
 
    Devuelve:
        requests.Response si tiene éxito, None si agota los reintentos
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            print(f"  [HTTP {e.response.status_code}] intento {attempt + 1}/{retries} — {url}")
        except requests.exceptions.ConnectionError as e:
            print(f"  [CONNECTION ERROR] intento {attempt + 1}/{retries} — {url}")
        except requests.exceptions.Timeout:
            print(f"  [TIMEOUT] intento {attempt + 1}/{retries} — {url}")
        except Exception as e:
            print(f"  [ERROR] intento {attempt + 1}/{retries} — {type(e).__name__}: {e}")
 
        # espera exponencial antes del siguiente intento: 2s, 4s, 8s...
        time.sleep(2 ** (attempt + 1))
 
    print(f"  [FALLIDO] Se agotaron los {retries} reintentos para {url}")
    return None


def parse_date(date_str: str) -> Optional[date]:
    """
    Convierte una cadena de fecha en un objeto date de Python.
 
   En Tranfermarkt las fechas pueden aparecen en distintos formatos.

        dd/mm/yyyy  →  30/04/1992 
        dd.mm.yyyy  →  30.04.1992
        yyyy-mm-dd  →  1992-04-30

    Se comprueba el formato concreto y  se devuelve un objeto datetime.date (1992, 4, 30)
    Devuelve None si la cadena está vacía, es un guión o no tiene formato reconocido.
    """
    if not date_str or date_str.strip() in ("-", ""):
        return None
 
    # normaliza separadores a '/'
    date_str = date_str.strip().replace(".", "/").replace("-", "/")
 
    # se  comprueba si la fecha viene con alguno de los tres formatos, y se intenta  normalizar
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
 
    return None


def extract_player_id(href: str) -> Optional[str]:
    """
    Extrae el ID numérico de un jugador del href de Transfermarkt.
 
    Ejemplo:
        /sergio-ramos/profil/spieler/25557 -> '25557'
    """
    match = re.search(r"/spieler/(\d+)", href)
    return match.group(1) if match else None
 
 
def extract_player_slug(href: str) -> Optional[str]:
    """
    Extrae el slug de URL de un jugador del href de Transfermarkt.
 
    Ejemplo:
        /sergio-ramos/profil/spieler/25557 -> 'sergio-ramos'
    """
    parts = href.split("/")
    return parts[1] if len(parts) > 1 else None

# ══════════════════════════════════════════════════
# SCRAPING — EQUIPOS
# ══════════════════════════════════════════════════

def get_league_teams(season: int) -> list[dict]:
    """
    Descarga los equipos participantes en la Champions League para una temporada.

    URL:
        https://www.transfermarkt.es/uefa-champions-league/teilnehmer/pokalwettbewerb/CL/saison_id/{season}

    La tabla de participantes usa la clase 'items'. Cada fila <tr class="odd/even">
    tiene un enlace con atributo title en <td class="hauptlink"> con el href:
        /{team_slug}/startseite/verein/{team_id}

    Parámetros:
        season (int): año de inicio de la temporada, ej: 2020 para 2020/2021

    Devuelve:
        list[dict]: lista de equipos, cada uno con team_id, team_slug y team_name
        []         si hay error en la petición o no se encuentra la tabla
    """
    url = (
        f"https://www.transfermarkt.es/uefa-champions-league"
        f"/teilnehmer/pokalwettbewerb/{LEAGUE_CODE}/saison_id/{season}"
    )

    response = request_with_retry(url)
    if not response:
        return []
    
    #parsea el html  con BeatifulSoup 
    soup  = BeautifulSoup(response.content, "html.parser")

    # los equipos de la champions entan en una etiqueta table con la clase items 
    table = soup.find("table", class_="items")

    if not table:
        print(f"  No se encontró la tabla de equipos para la temporada {season}")
        return []
    
    # los las filas <tr> tienen la clase odd o even. 
    rows  = table.find_all("tr", class_=["odd", "even"])
    teams = []

    for row in rows:
        href = ""
        try:
            # busca por atributo semántico —  busca  un <a> que tenga el atributo title
            anchor = row.find("a", title=True)
            if not anchor:
                continue

            href  = anchor.get("href", "")
            parts = href.split("/")

            # valida la estructura del href antes de acceder por índice
            # /real-madrid/startseite/verein/418
            # ["", "real-madrid", "startseite", "verein", "418"]
            if len(parts) < 5 or parts[2] != "startseite" or parts[3] != "verein":
                continue

            team_slug = parts[1]       # "real-madrid"
            team_id   = int(parts[4])  # 418
            team_name = anchor.get("title")

            teams.append({
                "team_id":   team_id,
                "team_slug": team_slug,
                "team_name": team_name,
            })

        except (ValueError, IndexError) as e:
            # registra el error con el href para poder depurar sin nuevas peticiones
            print(f"  Error procesando fila de equipo: {e} — href: {href}")
            continue

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return teams

# ══════════════════════════════════════════════════
# SCRAPING — fecha de nacimeinto 
# ══════════════════════════════════════════════════
def get_birth_date(player_slug: str, player_id: str) -> Optional[date]:
    
    """
    Accede al perfil del jugador y extrae su fecha de nacimiento

    URL:
        https://www.transfermarkt.es/{player_slug}/profil/spieler/{player_id}

    La fecha está en un par de spans label/valor:
        <span class="info-table__content info-table__content--regular">F. Nacim./Edad:</span>
        <span class="info-table__content info-table__content--bold">
            <a href="...">30/03/1986 (40)</a>
        </span>

    Parámetros:
        player_slug (str): slug del jugador, ej: "sergio-ramos"
        player_id   (str): ID del jugador, ej: "25557"

    Devuelve:
        date con la fecha de nacimiento, o None si no se encuentra
    """
    
    url      = f"https://www.transfermarkt.es/{player_slug}/profil/spieler/{player_id}"
    response = request_with_retry(url)
    if not response:
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    # busca el label por texto — más estable que clases o itemprop
    for label in soup.find_all("span", class_="info-table__content--regular"):
        if "Nacim" in label.text:
            valor = label.find_next_sibling("span")
            if not valor:
                continue
            # "30/03/1986 (40)" → "30/03/1986"
            raw = valor.get_text(strip=True).split("(")[0].strip()
            return parse_date(raw)

    return None

# ══════════════════════════════════════════════════
# SCRAPING — PLANTILLAS
# ══════════════════════════════════════════════════
 
def get_squad(team_slug: str, team_id: int, season: int) -> list[dict]:
    """
    Descarga y parsea la plantilla de un equipo para una temporada.
 
    URL:
        https://www.transfermarkt.es/{team_slug}/kader/verein/{team_id}/saison_id/{season}
 
    La tabla de jugadores usa la clase 'items'. Cada fila <tr class="odd/even">
    tiene el enlace del jugador en <td class="hauptlink"> con href:
        /{player_slug}/profil/spieler/{player_id}
 
    Para cada jugador se hace una petición adicional al perfil para obtener
    la fecha de nacimiento (campo birth_date).
 
    Parámetros:
        team_slug (str): slug del equipo, ej: "real-madrid"
        team_id   (int): ID del equipo en Transfermarkt, ej: 418
        season    (int): año de inicio de la temporada, ej: 2020
 
    Devuelve:
        list[dict]: lista de jugadores, cada uno con:
            - player_id   (str):        ID del jugador
            - player_slug (str):        slug del jugador
            - player_name (str):        nombre completo
            - position    (str|None):   posición, ej: "Delantero centro"
            - nationality (str|None):   nacionalidad
            - birth_date  (date|None):   fecha de nacimiento dd/mm/yyyy
            - team        (str):        team_slug del equipo
            - season      (int):        año de inicio de la temporada
        [] si hay error en la petición o no se encuentra la tabla
    """
    url      = f"https://www.transfermarkt.es/{team_slug}/kader/verein/{team_id}/saison_id/{season}"
    response = request_with_retry(url)
    if not response:
        return []
 
    soup  = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", class_="items")
 
    if not table:
        print(f"  Sin tabla de plantilla para {team_slug} ({season})")
        return []
 
    rows    = table.find_all("tr", class_=["odd", "even"])
    players = []
 
    for row in rows:
        try:
            td_hauptlink = row.find("td", class_="hauptlink")
            anchor       = td_hauptlink.find("a") if td_hauptlink else None
            if not anchor:
                continue
 
            href        = anchor.get("href", "")
            player_id   = extract_player_id(href)
            player_slug = extract_player_slug(href)
 
            if not player_id or not player_slug:
                continue
 
            # limpia el nombre — algunos jugadores tienen un <span> de lesión dentro del enlace
            # que añade un carácter \xa0 (espacio no separable)
            player_name = anchor.get_text(strip=True).replace("\xa0", "").strip()
 
            # posición — está en la segunda fila de la tabla anidada dentro de la celda del nombre
            # estructura: <table><tr>[foto+nombre]</tr><tr>[posición]</tr></table>
            position = None
            nested   = row.find("table")
            if nested:
                nested_rows = nested.find_all("tr")
                if len(nested_rows) > 1:
                    position = nested_rows[1].get_text(strip=True)
 
            # nacionalidad — imagen con clase flaggenrahmen dentro de la fila
            # <img alt="España" class="flaggenrahmen">
            flag_img    = row.find("img", class_="flaggenrahmen")
            nationality = flag_img.get("alt") if flag_img else None
 
            # fecha de nacimiento — petición adicional al perfil individual
            birth_date = get_birth_date(player_slug, player_id)
 
            players.append({
                "player_id":   player_id,
                "player_slug": player_slug,
                "player_name": player_name,
                "position":    position,
                "nationality": nationality,
                "birth_date":  birth_date,
                "team":        team_slug,
                "season":      season,
            })
 
        except (KeyError, IndexError, AttributeError) as e:
            print(f"  Error procesando jugador: {type(e).__name__}: {e}")
            continue
        except Exception as e:
            print(f"  Error inesperado en jugador: {type(e).__name__}: {e}")
            continue
 
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return players

 
# ══════════════════════════════════════════════════
# SCRAPING — LESIONES
# ══════════════════════════════════════════════════

def get_player_injuries(player_slug: str, player_id: str) -> list[dict]:
    """
    Descarga y parsea el historial completo de lesiones de un jugador.

    URL:
        https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}

    Estructura de cada fila de la tabla:
        <tr class="odd/even">
            <td class="zentriert">25/26</td>                      → season
            <td class="hauptlink">Desgarro del ligamento</td>     → injury_type
            <td class="zentriert">09/03/2026</td>                 → date_from
            <td class="zentriert">01/04/2026</td>                 → date_until
            <td class="rechts">24 dias</td>                       → days_absent
            <td class="rechts hauptlink wappen_verletzung">
                <span>3</span>                                    → matches_missed
            </td>                                                    puede ser "-" si no hay datos
        </tr>

    Parámetros:
        player_slug (str): slug del jugador, ej: "sergio-ramos"
        player_id   (str): ID del jugador, ej: "25557"

    Devuelve:
        list[dict]: lista de lesiones, cada una con:
            - season         (str):       temporada, ej: "20/21"
            - injury_type    (str):       tipo de lesión
            - date_from      (date|None): fecha inicio
            - date_until     (date|None): fecha fin
            - days_absent    (int|None):  días de baja
            - matches_missed (int|None):  partidos perdidos, None si no hay datos
            - player_id      (str):       ID del jugador
        [] si hay error o el jugador no tiene lesiones registradas
    """
    url      = f"https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}"
    response = request_with_retry(url)
    if not response:
        return []

    soup  = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", class_="items")

    # es normal que un jugador no tenga lesiones — no es un error
    if not table:
        return []

    rows     = table.find_all("tr", class_=["odd", "even"])
    injuries = []

    for row in rows:
        try:
            cols = row.find_all("td")
            if len(cols) < 6:
                continue

            # días de baja: "24 dias" → extraemos solo el número con regex
            days_str   = cols[4].text.strip()
            days_match = re.search(r"\d+", days_str)
            days_absent = int(days_match.group()) if days_match else None

            # partidos perdidos: dentro de un <span> en la última celda
            # puede ser "-" si no hay datos → None
            span           = cols[5].find("span")
            matches_missed = int(span.text.strip()) if span and span.text.strip().isdigit() else None

            injuries.append({
                "season":         cols[0].text.strip(),
                "injury_type":    cols[1].text.strip(),
                "date_from":      parse_date(cols[2].text.strip()),
                "date_until":     parse_date(cols[3].text.strip()),
                "days_absent":    days_absent,
                "matches_missed": matches_missed,
                "player_id":      player_id,
            })

        except IndexError as e:
            print(f"  Estructura HTML inesperada en lesiones: {e} — jugador: {player_slug}")
            continue
        except AttributeError as e:
            print(f"  Elemento no encontrado en lesiones: {e} — jugador: {player_slug}")
            continue
        except ValueError as e:
            print(f"  Error convirtiendo a número en lesiones: {e} — jugador: {player_slug}")
            continue
        except Exception as e:
            print(f"  Error inesperado en lesiones: {type(e).__name__}: {e} — jugador: {player_slug}")
            continue

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return injuries

# ══════════════════════════════════════════════════
# ORQUESTADOR
# ══════════════════════════════════════════════════
 
def scrape_champions() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Orquesta la extracción completa de datos de la Champions League.
 
    Fase 1 — Equipos:
        Por cada temporada en SEASONS llama a get_league_teams() para obtener
        los equipos participantes ese año (varían cada temporada) en una lista de diccionarios 
 
    Fase 2 — Plantillas:
        Por cada equipo llama a get_squad() para obtener sus jugadores.
        get_squad() ya incorpora la fecha de nacimiento de cada jugador
        haciendo una petición adicional al perfil individual.
        Acumula todos los jugadores con el campo 'season'.
 
    Fase 3 — Lesiones:
        Por cada jugador único (deduplicado por player_id) llama a
        get_player_injuries() para obtener su historial completo de lesiones.
 
    Devuelve:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
            - df_players:  un registro por (jugador, temporada, equipo) con
                           player_id, player_slug, player_name, position,
                           nationality, birth_date, team, season
            - df_injuries: una lesión por fila con season, injury_type,
                           date_from, date_until, days_absent, matches_missed
                           y player_id para cruzar con df_players
            - df_teams:    un equipo por fila (sin duplicados) con
                           team_id, team_slug y team_name
        tuple[pd.DataFrame vacío x3] si no se obtienen jugadores
    """
    # todos los jugadores de todos los equipos
    all_players  = []
    # todas las lesiones de todos lso jugadores de todos lso equipos 
    all_injuries = []
    # todos los equipos 
    all_teams    = []
 
    # equipos ya vistos para deduplicar df_teams. En cada temporada habra equippos qeu vuelvan a aparecer 
    seen_team_ids = set()
 
    # jugadores ya procesados para no repetir get_player_injuries y evitar obtener las lesiones de un jugador mas de un vez 
    # un mismo jugador puede aparecer en varios equipos y temporadas
    processed_player_ids = set()
 
    for season in SEASONS:
        print(f"\n{'=' * 50}")
        print(f"  Temporada {season}/{season + 1}")
        print(f"{'=' * 50}")
 
        # fase 1: equipos de la temporada. 
        teams = get_league_teams(season)
        print(f"  {len(teams)} equipos encontrados")
 
        if not teams:
            print(f"  No se obtuvieron equipos para {season}, saltando...")
            continue
 
        # fase 2: plantillas por equipo. 
        # Recorre la lista de diccionarios 
        for team in teams:
            if team["team_id"] not in seen_team_ids:
                seen_team_ids.add(team["team_id"])
                all_teams.append(team)
 
            print(f"\n  Obteniendo plantilla de {team['team_name']}...")
            players = get_squad(team["team_slug"], team["team_id"], season)
            print(f"  {len(players)} jugadores encontrados")
            all_players.extend(players)
 
    if not all_players:
        print("\n  No se obtuvieron jugadores.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
 
    # fase 3: lesiones por jugador único
    print(f"\n  Obteniendo lesiones ({len(all_players)} registros de jugadores)...")
    for player in all_players:
        player_id = player["player_id"]
        if player_id in processed_player_ids:
            continue
        processed_player_ids.add(player_id)
 
        print(f"  → Lesiones de {player['player_name']}...")
        injuries = get_player_injuries(player["player_slug"], player_id)
        all_injuries.extend(injuries)
 
    print(f"\n  Resumen:")
    print(f"    Equipos únicos:         {len(all_teams)}")
    print(f"    Registros de jugadores: {len(all_players)}")
    print(f"    Jugadores únicos:       {len(processed_player_ids)}")
    print(f"    Lesiones:               {len(all_injuries)}")
 
    return (
        pd.DataFrame(all_players),
        pd.DataFrame(all_injuries),
        pd.DataFrame(all_teams),
    )
 
 
# ══════════════════════════════════════════════════
# PROGRAMA PRINCIPAL
# ══════════════════════════════════════════════════
 
def main():
    """
    Punto de entrada del script.
 
    Llama al orquestador scrape_champions(), crea el directorio de salida
    OUTPUT_DIR si no existe y guarda los resultados en tres CSVs:
        - transfermarkt_champions_teams.csv
        - transfermarkt_champions_players.csv
        - transfermarkt_champions_injuries.csv
    """
    print("=" * 55)
    print(f"  Champions League scraper — {SEASONS[0]}/{SEASONS[0]+1} → {SEASONS[-1]}/{SEASONS[-1]+1}")
    print("=" * 55)
    
    # desempaqueta al tupla devuelta con los DataFrames
    df_players, df_injuries, df_teams = scrape_champions()
 
    if df_players.empty:
        print("\n  No se obtuvieron datos.")
        return
        
    #  crea el directorio si no existe. Si existe, no hace nada
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # define las rutas 
    teams_path   = os.path.join(OUTPUT_DIR, "transfermarkt_champions_teams.csv")
    players_path = os.path.join(OUTPUT_DIR, "transfermarkt_champions_players.csv")
    injuries_path = os.path.join(OUTPUT_DIR, "transfermarkt_champions_injuries.csv")

    df_teams.to_csv(teams_path,    index=False)
    df_players.to_csv(players_path,  index=False)
    df_injuries.to_csv(injuries_path, index=False)
 
    print(f"\n  Archivos guardados:")
    print(f"    {teams_path}    ({len(df_teams)} filas)")
    print(f"    {players_path}  ({len(df_players)} filas)")
    print(f"    {injuries_path} ({len(df_injuries)} filas)")
 
 
if __name__ == "__main__":
    main()
 