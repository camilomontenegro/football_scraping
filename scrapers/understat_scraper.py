"""
scrapers/understat_scraper.py
==============================
Scraper de Understat. Extrae partidos y tiros de una liga/temporada.

Salida (data/raw/understat/):
    understat_matches_laliga.csv
    understat_shots_laliga.csv
    understat_players_laliga.csv
    understat_teams_laliga.csv
"""

import re
import os
import json
import sys
import asyncio
import aiohttp
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import pandas as pd

# Allow running directly as a script
sys.path.append(str(Path(__file__).resolve().parent.parent))

# Valores por defecto
LEAGUE_DEFAULT  = "La_Liga"
SEASONS_DEFAULT = [2020, 2021, 2022, 2023, 2024]
DELAY_SEC       = 1.5
PROJECT_ROOT    = Path(__file__).resolve().parent.parent
OUTPUT_DIR      = PROJECT_ROOT / "data" / "raw" / "understat"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def _parse_understat_date(date_str: str) -> "date | None":
    """Parsea fecha de Understat (formato: '2025-05-25 00:00:00')."""
    from datetime import datetime
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str[:19], fmt).date()
        except ValueError:
            continue
    return None




#Si no indicasemos expresamente el user-agent , cuando se hace la peticicoon http de aiohttp  
#  la libreria  aiohttp manda un User-Agent generico -> User-Agent: Python/aiohttp 3.x , que Understat puede detectar com oun bot o scraper y bloquear la peticion
# Al establecer uno   expresamente,  simulamos que las peticiones vienen de un navegador real y evitar que Understat  bloquee las peticiones 
# 
#La pagina  bloqueaba mis peticiones porque no basta con usar el user-Agent sino  mas  elementos en los headers. 
# Para encontrarlo  me voy aqui https://understat.com/league/La_liga/2021 -> f12 , network -> la peticion y request headers 
# El x-requested-with: XMLHttpRequest le dice a Understat que es una peticiÃ³n AJAX legÃ­tima desde su propia pÃ¡gina, no un scraper externo.
#  HEADERS  queda solo solo con los headers que son comunes a todas las peticiones:
#Y el Referer se gestiona dinÃ¡micamente en fetch porque cambia segÃºn la peticiÃ³n:
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"  # tu versiÃ³n real
    ),
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

# â”€â”€ Helpers de parsing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

#  html: str, var_name: str -> indicamos el tipo de los paramentros 
# > Optional[list | dict]: el tipo de retorno de la funciÃ³n. Puede ser una lista, un diccionario o None. Es solo una anotaciÃ³n para documentar el cÃ³digo, no afecta al comportamiento.
def parse_embedded_json(html: str, var_name: str) -> Optional[list | dict]:
    r"""
    html: str, 
    var_name: str -> indicamos el tipo de los paramentros 

    # > Optional[list | dict]: el tipo de retorno de la funciÃ³n. Puede ser una lista, un diccionario o None. Es solo una anotaciÃ³n para documentar el cÃ³digo, no afecta al comportamiento.

    Recibe el HTML completo de la pÃ¡gina y el nombre de la variable que quiere encontrar dentro de ese HTML.
    Understat no tiene una API. Los datos estÃ¡n escondidos dentro del HTML de la pÃ¡gina como un script de JavaScript:
    
    Understat embeds data como:
        var datesData = JSON.parse('...')
    El contenido estÃ¡ escapado con unicode escapes.

        REGULAR EXPRESION - BREAKDOWN 
        el patron de bÃºsqueda y  grupo de captura.  l oqeu se captura es todo lo que vaya entre las comillas simples dentro del JSON.parse() 

            El rf al principio significa que es un string que combina dos cosas: r de raw (las \ no se escapan) y f de f-string (permite meter variables).
            {var_name}     ->  la variable que buscamos, ej: "datesData"
            \s*            ->  cero o mÃ¡s espacios
            =              ->  el signo igual
            \s*            ->  cero o mÃ¡s espacios
            JSON\.parse    ->  literal "JSON.parse" (el \. escapa el punto) porque no quiero que .se interprete como un metacharacter de la expresion regular 
            \(            ->  parÃ©ntesis escapado -> busca literalmente "(" en el HTML. Si no lo pones el parentesis se interopreta como un un metacaracter de la expression regular con su significado esperical 
        
            '(.+?)'        ->  captura todo lo que hay entre comillas simples
            Grupo de captura	Los parÃ©ntesis dicen: "Guarda todo lo que encuentres aquÃ­".
            \)             ->   parÃ©ntesis escapado -> busca literalmente ")" en el HTML
        
    """
    # Captura lo que estÃ© dentro de las comillas simples que van dentro del JSON.parse().
    pattern = rf"{var_name}\s*=\s*JSON\.parse\('(.+?)'\)"

    # Busca el patrÃ³n dentro de todo el HTML. Si lo encuentra devuelve un objeto match, si no devuelve None.
    # re.search() es mejor que el metodo findall()  porque esa variable solo aparece una vez por pÃ¡gina.
    match = re.search(pattern, html)
    if not match:
        return None
        
        # usas group(1) porque quieres solo el contenido del grupo y  y no el aptron completo
        #match.group(1) extrae lo que capturÃ³ el patrÃ³n, el contenido dentro de las comillas simples. 
        # encode("utf-8").decode("unicode_escape")
        # Ese contenido viene escapado asÃ­:
        # \x7b\x22id\x22\x3a\x2214093\x22...
        # encode("utf-8").decode("unicode_escape") convirite a texto legible 
    raw = match.group(1).encode("utf-8").decode("unicode_escape")
    # Convierte ese string JSON en una estructura Python, lista o diccionario segÃºn el caso:
    return json.loads(raw)

# â”€â”€ Funciones de scraping â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def fetch(session: aiohttp.ClientSession, url: str, referer: str = None) ->Optional[str]:
    r""" 
        session: aiohttp.ClientSession. la sesiÃ³n HTTP, se crea una vez y se reutiliza
        url: str     la URL a la que hacer la peticiÃ³n
        referer: str  opcional, la URL de referencia para el header "Referer". Understat puede bloquear peticiones sin un Referer vÃ¡lido.
        el Referer se gestiona dinÃ¡micamente en fetch porque cambia segÃºn la peticiÃ³n:

        El session es como una conexiÃ³n abierta al servidor. En lugar de abrir y cerrar una conexiÃ³n por cada peticiÃ³n, se crea una sola vez y se reutiliza para todas

        with es un gestor de contexto. Se encarga de abrir y cerrar un recurso automÃ¡ticamente, aunque haya errores:
        
        Hace la peticiÃ³n GET al servidor con la URL y el header
        Guarda la respuesta en resp
        Comprueba que no hay error HTTP con raise_for_status(). Si hay error laznza exception 
        Extrae el HTML como string con resp.text()
        Cierra la conexiÃ³n automÃ¡ticamente al salir del bloque

        Y
        
    """
    headers = HEADERS.copy()
    if referer:
        headers["Referer"] = referer
        
    try:
        async with session.get(url, headers= headers) as resp:
            # NO necesita await. Raise for status si se manda un codigo de error lanza una excepcion
            resp.raise_for_status()
            return await resp.text()
        
    # Captura errores del servidor (como el 404 o 403).
    except aiohttp.ClientResponseError as e:
        print(f"  [WARNING] Error HTTP {e.status} en {url}")
        return None
    except aiohttp.ClientError as e:
        print(f"  [WARNING] Error de conexiÃ³n en {url}: {e}")
        return None



async def get_league_matches(session: aiohttp.ClientSession, season: int, league: str = None) -> list[dict]:
    r"""
    Endpoint JSON de liga -> lista de partidos con IDs de Understat.
    URL ejemplo: https://understat.com/getLeagueData/La_liga/2021

    Args:
        session: Sesión HTTP de aiohttp
        season: Año de inicio de temporada (2020 = 20/21)
        league: Nombre de la liga en formato Understat (ej: "La_Liga", "EPL", "Bundesliga")
                Si es None, usa la constante global LEAGUE (para compatibilidad)

    Devuelve una lista de diccionarios con los partidos de la liga.
    El endpoint devuelve JSON con tres claves: dates, teams, players.
    Los partidos están en data["dates"].

    1. fetch()        ->  JSON crudo del endpoint getLeagueData
    2. json.loads()   ->  diccionario Python con claves dates/teams/players
    3. data["dates"]  ->  lista de partidos
    4. bucle for      ->  recorre la lista y construye diccionarios limpios
    5. return matches ->  devuelve la lista de diccionarios

    Se llama en scrape_laliga()
    """
    # Usar el valor por defecto o el pasado como parámetro
    league_code = league if league else LEAGUE_DEFAULT
    
    url = f"https://understat.com/getLeagueData/{quote(league_code)}/{season}"

    raw = await fetch(session, url, referer=f"https://understat.com/league/{league_code}/{season}")
    
    if not raw:
        print(f"  [WARNING] No se pudo obtener datos para temporada {season}")
        return []

    try:
        # convierte el string que devuevle fetch en un diccionario ptyhon con tres claves: dates, teams, players
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  [WARNING] Error parseando JSON para temporada {season}: {e}")
        return []

    dates = data.get("dates", [])

    if not dates:
        print(f"  [WARNING] No se encontraron partidos para {season}")
        return []

    matches = []
    for m in dates:
        matches.append({
            "understat_match_id": m["id"],
            "season":             season,
            "datetime":           m.get("datetime"),
            "home_team":          m["h"]["title"],
            "away_team":          m["a"]["title"],
            "home_team_id":       m["h"]["id"],
            "away_team_id":       m["a"]["id"],
            "home_goals":         m["goals"]["h"],
            "away_goals":         m["goals"]["a"],
            "home_xg":            m.get("xG", {}).get("h"),
            "away_xg":            m.get("xG", {}).get("a"),
        })
    return matches
   
    


async def get_match_shots(session: aiohttp.ClientSession, understat_match_id: str) -> list[dict]:
    r"""
    PÃ¡gina de partido -> datos de cada tiro.
    URL ejemplo: https://understat.com/match/14093
    
    Campos raw de Understat:
    id, minute, result, X, Y, xG, player, h_a (home/away),
    player_id, situation, season, shotType, match_id,
    h_team, a_team, h_goals, a_goals, date, player_assisted, lastAction

    Se llama en scrape_laliga()
    """
    url = f"https://understat.com/getMatchData/{understat_match_id}"
    raw = await fetch(session, url, referer=f"https://understat.com/match/{understat_match_id}")
    
    if not raw:
        return []
    


    try:
        # json.loads convierte el string con formato JSON  en un diccionario 
        # data es un diccioanrio con dos claves ( 'a', 'h' ) y el valor de cada  clave es una lsita de diccioanrios
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON para partido {understat_match_id}: {e}")
        return []

    shots_data = data.get("shots", {})

    if not shots_data:
        return []
    
    shots = []
    # shotsData tiene dos claves: 'h' (home) y 'a' (away)
    # receurda el metodo get para acceder a los valores de las calves, dmite somo segunda parametro un valor por defecto  en caso de que la calve no exista 
    #si la clave "h" o "a" no existiera en el diccionario devolverÃ­a una lista vacÃ­a en lugar de None o un error.
    for side in ("h", "a"):

        ## aqui se recorre la la lsita de diccionarios. El valor de cada clave h o a es uan lsita de diccionarios y cada diccioanrios es un tiro
        # es cada tiro , es cada diccioanrio de la lista 
        for shot in shots_data.get(side, []):
            shots.append({
                "understat_shot_id":   shot.get("id"),
                "understat_match_id":  understat_match_id,
                "understat_player_id": shot.get("player_id"),
                "understat_team":      shot.get("h_team") if side == "h" else shot.get("a_team"),
                "side":                side,           # h=local, a=visitante
                "player_name":         shot.get("player"),
                "minute":              shot.get("minute"),
                "x":                   shot.get("X"),  # Understat usa X,Y en mayÃºscula
                "y":                   shot.get("Y"),
                "xg":                  shot.get("xG"),
                "result":              shot.get("result"),
                "shot_type":           shot.get("shotType"),   # RightFoot, LeftFoot, Head
                "situation":           shot.get("situation"),  # OpenPlay, SetPiece, FromCorner, DirectFreekick, Penalty
                "last_action":         shot.get("lastAction"),
                "player_assisted":     shot.get("player_assisted"),
                "season":              shot.get("season"),
                "source":              "understat",
            })
    # lista de diccionarios con los tiros del  partido 
    return shots




# â”€â”€ Orquestador principal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def scrape_laliga(seasons: list[int], league: str = None, from_date: str = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    r"""
    Orquestador principal del scraping. Recibe la lista de temporadas y devuelve
    dos DataFrames: uno con los partidos y otro con los tiros.

    Args:
        seasons: Lista de años de inicio de temporada (ej: [2020, 2021, 2022])
        league: Código de la liga en formato Understat (ej: "La_Liga", "EPL", "Bundesliga")
                Si es None, usa la constante global LEAGUE (para compatibilidad)
        from_date: Fecha inicial (YYYY-MM-DD). Descarga solo partidos desde esta fecha.

    Crea una sesiÃ³n HTTP compartida con:
    - TCPConnector(limit=3): mÃ¡ximo 3 conexiones paralelas para no saturar el servidor
    - ClientTimeout(total=30): cancela peticiones que tarden mÃ¡s de 30 segundos

    Por cada temporada:
    1. Obtiene la lista de partidos con get_league_matches()
    2. Recorre cada partido y obtiene sus tiros con get_match_shots()
    3. AÃ±ade el campo 'season' como entero a cada tiro
    4. Acumula partidos y tiros en all_matches y all_shots

    Devuelve una tupla con dos DataFrames (df_matches, df_shots) que se desempaqueta en main:
        df_matches, df_shots = await scrape_laliga(SEASONS)
    """
    # Usar el valor por defecto o el pasado como parámetro
    league_code = league if league else LEAGUE_DEFAULT

    from_date_obj = None
    if from_date:
        from datetime import datetime
        from_date_obj = datetime.strptime(from_date, "%Y-%m-%d").date()
        print(f"\n[FILTER] Descargando solo partidos desde: {from_date}")

    all_matches = []
    all_shots   = []

    connector = aiohttp.TCPConnector(limit=3)  # mÃ¡x 3 conexiones paralelas

    #ClientTimeout(total=30) significa que si una peticiÃ³n tarda mÃ¡s de 30 segundos, se cancela y lanza error. Sin esto una peticiÃ³n podrÃ­a quedarse colgada indefinidamente.
    timeout   = aiohttp.ClientTimeout(total=30)

    ##  async with abre y cierra la sesion http
    ##  Los metodos que necesitan la sesion  se llaman dentro del bloque asycn with 
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        
        # Recorre la lista  con las temporadas que recibe como parametro
        for season in seasons:
            print(f"\n[SEASON] Temporada {season}/{season+1}")
    
            try:
                # lsita de diccioanrios 
                matches = await get_league_matches(session, season, league=league_code)
            except Exception as e:
                print(f"  [!] Error obteniendo partidos de temporada {season}: {e}")
                continue

            print(f"  [+] {len(matches)} partidos encontrados")
            
            # Filtrar por fecha si se especifica from_date
            if from_date_obj:
                original_count = len(matches)
                matches = [m for m in matches if m.get("datetime") and _parse_understat_date(m["datetime"]) >= from_date_obj]
                print(f"  [+] {len(matches)} partidos después de {from_date} (filtrados {original_count - len(matches)})")
            
            all_matches.extend(matches)

            from datetime import date
            processed_count = 0
            for i, match in enumerate(matches, 1):
                # extrae el id de cada partido 
                mid = match["understat_match_id"]
                match_date = _parse_understat_date(match.get("datetime"))
                if match_date and match_date > date.today():
                    continue

                try:
                    # obtiene los tiros de los partidos y aÃ±ade la temporada a cada tiro 
                    shots = await get_match_shots(session, mid)
                    for s in shots:
                        s["season"] = season
                    all_shots.extend(shots)
                    processed_count += 1
                    
                    # Esto se usa mucho para logs de progreso en loops grandes para no imprimir en cada iteraciÃ³n.
                    if processed_count % 20 == 0:
                        print(f"  -> {processed_count}/{len(matches)} partidos procesados | tiros acumulados: {len(all_shots)}")

                    await asyncio.sleep(DELAY_SEC)

                except aiohttp.ClientError as e:
                    print(f"  [WARNING] Error HTTP en partido {mid}: {e}")
                    await asyncio.sleep(5)
                except Exception as e:
                    print(f"  [!] Error inesperado en partido {mid}: {e}")
                    await asyncio.sleep(5)

            print(f"[+] Temporada {season} completa: {len(matches)} partidos, {len(all_shots)} tiros totales acumulados")
    ##Devuelve una tupla con dos DataFrames: el primero con los partidos y el segundo con los tiros.
    return pd.DataFrame(all_matches), pd.DataFrame(all_shots)

# â”€â”€ TransformaciÃ³n -> esquema fact_shots â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def transform_shots(df_shots: pd.DataFrame, df_matches: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara el DataFrame de tiros para carga en fact_shots.
    
    IMPORTANTE: match_id, player_id, team_id son las PKs de tus tablas dim_*.
    Este script genera las FK de Understat; el ETL final deberÃ¡ hacer el join
    con tus dimensiones para resolver los IDs definitivos.
    """
    # Normalizar tipos
    df = df_shots.copy()
    df["minute"]   = pd.to_numeric(df["minute"],  errors="coerce").astype("Int16")
    df["x"]        = (pd.to_numeric(df["x"], errors="coerce") * 105).round(2)  # 0-1 -> 0-105 metros
    df["y"]        = (pd.to_numeric(df["y"], errors="coerce") * 68).round(2)   # 0-1 -> 0-68 metros
    df["xg"]       = pd.to_numeric(df["xg"],       errors="coerce").round(4)

    # Mapear resultado a valores estÃ¡ndar

        #El Metodo map en pandas no es el metodo map  genericio de python. funciona de distitna manera 
        # El metodo map en pandas Recorre cada celda de la columna, mira si ese valor existe como clave en el diccionario, y si existe lo reemplaza por el valor correspondiente. y si no o sustitueye por Nan
        #AquÃ­ entra fillna. Como map devuelve NaN para los valores que no estÃ¡n en el diccionario, fillna los rellena con el valor original de la columna:

    result_map = {
        "Goal":            "Goal",
        "SavedShot":       "Saved",
        "MissedShots":     "Off T",
        "BlockedShot":     "Blocked",
        "ShotOnPost":      "Post",
        "OwnGoal":         "OwnGoal",
    }
    # recuerda que la  columna antes de operador de asignacion es la que se va a modificar y la de la derecha es la que se usa para hacer la comparacion de los valores con las claves del diccioanrios
    df["result"] = df["result"].map(result_map).fillna(df["result"])

    # Mapear tipo de disparo
    shottype_map = {
        "RightFoot": "Right Foot",
        "LeftFoot":  "Left Foot",
        "Head":      "Head",
    }
    df["shot_type"] = df["shot_type"].map(shottype_map).fillna(df["shot_type"])

    # Mapear situaciÃ³n
    situation_map = {
        "OpenPlay":        "Open Play",
        "SetPiece":        "Set Piece",
        "FromCorner":      "From Corner",
        "DirectFreekick":  "Direct Freekick",
        "Penalty":         "Penalty",
    }
    df["situation"] = df["situation"].map(situation_map).fillna(df["situation"])

    # Columnas finales alineadas con fact_shots
    # (sin shot_id que es SERIAL, sin match_id/player_id/team_id definitivos
    #  -> se resuelven en el ETL cruzando con tus dim_*)
    cols = [
        "understat_match_id",   # -> cruzar con dim_match
        "understat_player_id",  # -> cruzar con dim_player
        "understat_team",       # -> cruzar con dim_team
        "player_name",
        "minute",
        "x", "y", "xg",
        "result", "shot_type", "situation",
        "side", "last_action", "player_assisted",
        "season", "source",
    ]
    # Devuelve el DataFrame pero solo con las columnas que estÃ¡n en cols, en el orden definido en cols, y descartando cualquier columna extra que pudiera haber llegado del scraping pero que no necesitamos en fact_shots.
    return df[[ c     for c in cols     if c in df.columns]]

# Por qui me quedo 

# â”€â”€ ETL de dimensiones auxiliares â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def extract_players(df_shots: pd.DataFrame) -> pd.DataFrame:
    return (
        df_shots[["understat_player_id", "player_name"]]
        .drop_duplicates()
        .dropna(subset=["understat_player_id"])
        .sort_values("understat_player_id")
        .reset_index(drop=True)
    )

def extract_teams(df_matches: pd.DataFrame) -> pd.DataFrame:
    home = df_matches[["home_team_id", "home_team"]].rename(
        columns={"home_team_id": "understat_team_id", "home_team": "team_name"})
    away = df_matches[["away_team_id", "away_team"]].rename(
        columns={"away_team_id": "understat_team_id", "away_team": "team_name"})
    return (
        pd.concat([home, away])
        .drop_duplicates()
        .sort_values("understat_team_id")
        .reset_index(drop=True)
    )


def save_understat_data(df_matches: pd.DataFrame, df_shots: pd.DataFrame) -> None:
    """Guarda los CSV de Understat en data/raw/understat."""
    df_shots_clean = transform_shots(df_shots, df_matches)
    df_players = extract_players(df_shots)
    df_teams = extract_teams(df_matches)

    shots_path = OUTPUT_DIR / "understat_shots_laliga.csv"
    matches_path = OUTPUT_DIR / "understat_matches_laliga.csv"
    players_path = OUTPUT_DIR / "understat_players_laliga.csv"
    teams_path = OUTPUT_DIR / "understat_teams_laliga.csv"

    df_shots_clean.to_csv(shots_path, index=False, encoding="utf-8-sig")
    df_matches.to_csv(matches_path, index=False, encoding="utf-8-sig")
    df_players.to_csv(players_path, index=False, encoding="utf-8-sig")
    df_teams.to_csv(teams_path, index=False, encoding="utf-8-sig")

# â”€â”€ Main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ã

async def main():
    print("=" * 55)
    print(f"  Understat scraper - {LEAGUE_DEFAULT} {SEASONS_DEFAULT[0]}-{SEASONS_DEFAULT[-1]}")
    print("=" * 55)

    df_matches, df_shots = await scrape_laliga(SEASONS_DEFAULT)

    if df_shots.empty:
        print("\n[!] No se obtuvieron datos. Revisa la conexiÃ³n o las URLs.")
        return

    print(f"\n[SUMMARY] Resumen:")
    print(f"  Partidos: {len(df_matches)}")
    print(f"  Tiros:    {len(df_shots)}")

    # Transformar
    df_shots_clean = transform_shots(df_shots, df_matches)
    df_players     = extract_players(df_shots)
    df_teams       = extract_teams(df_matches)

    # Guardar archivos
    shots_path   = os.path.join(OUTPUT_DIR, "understat_shots_laliga.csv")
    matches_path = os.path.join(OUTPUT_DIR, "understat_matches_laliga.csv")
    players_path = os.path.join(OUTPUT_DIR, "understat_players_laliga.csv")
    teams_path   = os.path.join(OUTPUT_DIR, "understat_teams_laliga.csv")

    df_shots_clean.to_csv(shots_path,   index=False, encoding="utf-8-sig")
    df_matches.to_csv(    matches_path, index=False, encoding="utf-8-sig")
    df_players.to_csv(    players_path, index=False, encoding="utf-8-sig")
    df_teams.to_csv(      teams_path,   index=False, encoding="utf-8-sig")

    print(f"\n[OK] Archivos guardados:")
    print(f"  {shots_path}   ({len(df_shots_clean)} filas)")
    print(f"  {matches_path} ({len(df_matches)} filas)")
    print(f"  {players_path} ({len(df_players)} filas)")
    print(f"  {teams_path}   ({len(df_teams)} filas)")

    # Preview
    print(f"\n[SAMPLE] Muestra de tiros:")
    print(df_shots_clean.head(3).to_string(index=False))




if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scraper de Understat")
    parser.add_argument("--league", "-l", type=str, default=None,
                        help="Código de la liga en formato Understat (ej: La_Liga, EPL, Bundesliga)")
    parser.add_argument("--seasons", "-s", type=str, default=None,
                        help="Temporadas a scrapear (ej: 2020,2021,2022 o 'all' para todas)")
    
    args = parser.parse_args()
    
    # Procesar temporadas
    if args.seasons:
        if args.seasons.lower() == "all":
            seasons = SEASONS_DEFAULT
        else:
            seasons = [int(y.strip()) for y in args.seasons.split(",")]
    else:
        seasons = SEASONS_DEFAULT
    
    league = args.league if args.league else LEAGUE_DEFAULT

    asyncio.run(scrape_laliga(seasons, league))

