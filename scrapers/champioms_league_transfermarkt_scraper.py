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


def _append_to_csv(records: list[dict], path: str) -> None:
    if not records:
        return
    df = pd.DataFrame(records)
    write_header = not os.path.exists(path)
    df.to_csv(path, mode="a", index=False, header=write_header)
    print(f"    → {len(records)} filas guardadas en {os.path.basename(path)}")
 
 
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
            country = get_team_country(team_slug,team_id)

            teams.append({
                "team_id":   team_id,
                "team_slug": team_slug,
                "team_name": team_name,
                "team_country":country,
            })

        except (ValueError, IndexError) as e:
            # registra el error con el href para poder depurar sin nuevas peticiones
            print(f"  Error procesando fila de equipo: {e} — href: {href}")
            continue

    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    return teams


def get_team_country(team_slug: str, team_id: int) -> Optional[str]:
    """
    Accede a la página del equipo y extrae el país.

    URL:
        https://www.transfermarkt.es/{team_slug}/startseite/verein/{team_id}

    El país está en el label "Liga:" dentro de data-header:
        <span class="data-header__label">
            <strong>Liga:</strong>
            <span class="data-header__content">
                <a href="...">
                    <img title="Inglaterra" alt="Inglaterra" class="flaggenrahmen">
                </a>
            </span>
        </span>

    Parámetros:
        team_slug (str): slug del equipo, ej: "manchester-city"
        team_id   (int): ID del equipo en Transfermarkt, ej: 281

    Devuelve:
        str con el país, ej: "Inglaterra", o None si no se encuentra

        ** Este metodo se va a llamar en get_league_teams. Otra opcion seria  prescindir del método y  usar la logia de extraccion en get_squad ya que  el dato del pais del equipo se encuentra en la misma pagian que la plantilla del equipo. Habria que modificar get_squady  para que devolvise tambien un string.
        Se deja asi porsi solo se quiere obtener datos de los equipos. 
    """
    url = f"https://www.transfermarkt.es/{team_slug}/startseite/verein/{team_id}"
    response = request_with_retry(url)
    if not response:
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    # busca el label "Liga:" por texto — más estable que posición en el DOM
    for label in soup.find_all("span", class_="data-header__label"):
        if "Liga" in label.text:
            flag = label.find("img", class_="flaggenrahmen")
            if flag:
                return flag.get("title")

    return None



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

# ──────────────────────────────────────────── CAMBIO ────────────────────────────────────────────
def scrape_champions() -> None:
    """
    Orquesta la extracción completa de datos de la Champions League.
    Guarda los resultados de forma incremental tras cada temporada, de modo que
    si el proceso se interrumpe no se pierden las temporadas ya completadas.

    Resume automático: si los CSVs ya existen detecta qué temporadas están
    procesadas leyendo el CSV de jugadores, y las salta.

    Fase 1 — Equipos:
        Por cada temporada llama a get_league_teams().

    Fase 2 — Plantillas:
        Por cada equipo llama a get_squad().

    Fase 3 — Lesiones (dentro del bucle de temporadas):
        Por cada jugador nuevo (deduplicado por player_id) llama a
        get_player_injuries(). Se ejecuta antes del guardado para que cada
        temporada quede completa en disco.

    Salida (data/raw/transfermarkt/champions/):
        transfermarkt_champions_teams.csv
        transfermarkt_champions_players.csv
        transfermarkt_champions_injuries.csv
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    teams_path    = os.path.join(OUTPUT_DIR, "transfermarkt_champions_teams.csv")
    players_path  = os.path.join(OUTPUT_DIR, "transfermarkt_champions_players.csv")
    injuries_path = os.path.join(OUTPUT_DIR, "transfermarkt_champions_injuries.csv")

    # detecta temporadas ya procesadas leyendo el CSV de jugadores
    done_seasons: set[int] = set()
    if os.path.exists(players_path):
        df_existing = pd.read_csv(players_path, usecols=["season"])
        done_seasons = set(df_existing["season"].dropna().astype(int).unique())
        if done_seasons:
            print(f"  Resume: temporadas ya guardadas → {sorted(done_seasons)}")

    # resume carga los player_id ya procesados del CSV de lesiones
    # para no volver a descargar lesiones de jugadores ya procesados en ejecuciones anteriores
    processed_player_ids: set[str] = set()
    if os.path.exists(injuries_path):
        df_injuries_existing = pd.read_csv(injuries_path, usecols=["player_id"])
        processed_player_ids = set(df_injuries_existing["player_id"].dropna().astype(str).unique())
        if processed_player_ids:
            print(f"  Resume: {len(processed_player_ids)} jugadores con lesiones ya descargadas")

    # equipos ya vistos para deduplicar df_teams entre temporadas
    seen_team_ids: set[int] = set()

    for season in SEASONS:
        print(f"\n{'=' * 50}")
        print(f"  Temporada {season}/{season + 1}")
        print(f"{'=' * 50}")

        if season in done_seasons:
            print(f"  Ya procesada, omitiendo.")
            continue

        season_teams:    list[dict] = []
        season_players:  list[dict] = []
        season_injuries: list[dict] = []

        # fase 1: equipos de la temporada
        teams = get_league_teams(season)
        print(f"  {len(teams)} equipos encontrados")

        if not teams:
            print(f"  No se obtuvieron equipos para {season}, saltando...")
            continue

        # fase 2: plantillas por equipo
        for team in teams:
            if team["team_id"] not in seen_team_ids:
                seen_team_ids.add(team["team_id"])
                season_teams.append(team)

            print(f"\n  Obteniendo plantilla de {team['team_name']}...")
            players = get_squad(team["team_slug"], team["team_id"], season)
            print(f"  {len(players)} jugadores encontrados")
            season_players.extend(players)

        # fase 3: lesiones de jugadores nuevos en esta temporada
        new_players = [p for p in season_players if p["player_id"] not in processed_player_ids]
        print(f"\n  Obteniendo lesiones de {len(new_players)} jugadores nuevos...")
        for player in new_players:
            processed_player_ids.add(player["player_id"])
            print(f"  → Lesiones de {player['player_name']}...")
            injuries = get_player_injuries(player["player_slug"], player["player_id"])
            season_injuries.extend(injuries)

        # guardar temporada completa a disco
        print(f"\n  Guardando temporada {season}/{season + 1}...")
        _append_to_csv(season_teams,    teams_path)
        _append_to_csv(season_players,  players_path)
        _append_to_csv(season_injuries, injuries_path)
        print(f"  Temporada {season}/{season + 1} guardada.")

    print(f"\n  Proceso finalizado.")
    print(f"    {teams_path}")
    print(f"    {players_path}")
    print(f"    {injuries_path}")
# ────────────────────────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════
# PROGRAMA PRINCIPAL
# ══════════════════════════════════════════════════

# ──────────────────────────────────────────── CAMBIO ────────────────────────────────────────────
def main():
    """
    Punto de entrada del script.

    Llama a scrape_champions(), que gestiona internamente el directorio de
    salida, el guardado incremental por temporada y el resume automático.
    """
    print("=" * 55)
    print(f"  Champions League scraper — {SEASONS[0]}/{SEASONS[0]+1} → {SEASONS[-1]}/{SEASONS[-1]+1}")
    print("=" * 55)

    scrape_champions()
# ────────────────────────────────────────────────────────────────────────────────────────────────


if __name__ == "__main__":
    main()