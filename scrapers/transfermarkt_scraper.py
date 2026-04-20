r"""
La estructura de la ruta en Transfermarket  usa términos en alemán.

Términos en alemán y sus  traducciones en castellano 

Verletzungen  → "Lesiones".
Spieler → "Jugador".
Kader → Plantilla
Verein → Club
Startseite → "Página de inicio".
Slug → parte corta de la URL que identifica la página ej www.web.de/startseite  -> slug es startseite

zentriert → centrado
hauptlink → enlace principal
rechts → derecha
wappen_verletzung → escudo lesión (o “icono de lesión”)
tiny_wappen → escudo pequeño

"""
## ID RealMadrid en transfermarlet es 418 
## Id  FC Barcelona es 131

## https://www.transfermarkt.es/real-madrid/kader/verein/418/saison_id/2020
##https://www.transfermarkt.es/fc-barcelona/kader/verein/131/saison_id/2020

import requests
from bs4 import BeautifulSoup
import pandas as pd, time, random
import os

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"  
    ),
     "Accept-Language": "es-ES,es;q=0.9",
   
}

# Si se quiere obtener mas equipos, se  se añaden al diccionario  con el team_slug y el id
TEAMS = {
    "real-madrid": 418,
    "fc-barcelona": 131,
}

#En Transfermarkt el año de la temporada es el año de inicio. 2020 significa la temporada 2020/2021
SEASON       = 2020
OUTPUT_DIR   = os.path.join('data', 'raw', 'transfermarkt')

# ══════════════════════════════════════════════════
# SCRAPING — PLANTILLAS
# ══════════════════════════════════════════════════

def  get_squad(team_slug, team_id, season):
    """
    Descarga y parsea la plantilla de un equipo en Transfermarkt para una temporada.

    Realiza una petición HTTP a la ruta de plantilla:
        https://www.transfermarkt.es/{team_slug}/kader/verein/{team_id}/saison_id/{season}

    Parsea la tabla HTML con clase 'items', donde cada fila <tr class="odd/even">
    representa un jugador. El enlace del jugador está en <td class="hauptlink">
    y contiene el slug e id en el href: /{slug}/profil/spieler/{id}

    Parámetros:
        team_slug  (str): identificador del equipo en la URL, ej: "real-madrid"
        team_id    (int): ID numérico del equipo en Transfermarkt, ej: 418
        season     (int): año de inicio de la temporada, ej: 2020 para 2020/2021

    Devuelve:
        list[dict]: lista de jugadores, cada uno con:
            - player_id   (str): ID del jugador en Transfermarkt
            - player_slug (str): slug del jugador, ej: "karim-benzema"
            - player_name (str): nombre completo del jugador
            - position    (str): posición en español, ej: "Delantero centro"
            - team        (str): team_slug del equipo
        [] si hay error en la petición o no se encuentra la tabla
    """
    url =f'https://www.transfermarkt.es/{team_slug}/kader/verein/{team_id}/saison_id/{season}'
    
    #  hace  la peticion  para descargar el HTML de la página del equipo
    try: 
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # servidor devuelve 4xx o 5xx
        print(f" Error HTTP {e}")
        return []
    except requests.exceptions.ConnectionError as e:
        # no hay conexión
        print(f" Error de conexión: {e}")
        return []
    except requests.exceptions.Timeout as e:
        # la petición tardó demasiado
        print(f" Timeout: {e}")
        return []
    
    
    # parsea el html  con BeatifulShop
    soup = BeautifulSoup(response.content, 'html.parser')
    

    # los jugadores de un equicpo están en la web en una tabla con la clase items 
    table = soup.find('table', class_='items')

   
    if not table:
        return []

    # busca  en table todas las tr con  class 'odd' o  'even'
    rows = table.find_all('tr', class_=['odd', 'even'])
   
    # cada row es un objeto BeautifulShop  que representa una  fila <tr> en la tabla 
    
    players=[]
    for row in rows: 
        try: 
                
            # busca el <td> con class 'hauptlink'
            td_hauptlink = row.find('td', class_='hauptlink')
            # el enlace del jugador está dentro de <td class="hauptlink">
            anchor = td_hauptlink.find('a') if td_hauptlink else None
            if not anchor: 
                    continue
            
            # /karim-benzema/profil/spieler/18922   ← formato real
            href = anchor['href']  
        
            ## href.split('/') → ["", "karim-benzema", "profil", "spieler", "18922"]
            # [1] → "karim-benzema"
            player_slug= href.split('/')[1]
            ## [-1] → "18922"  (último elemento)
            player_id= href.split('/')[-1]
            
           
            #Algunos jugadores tienen un <span> adicional dentro del enlace para indicar que están lesionados:
            #En ese caso anchor.text.strip() devolvería "Thibaut Courtois\xa0"
            player_name = anchor.get_text(strip=True).replace('\xa0', '').strip()

            # row.find('table') → encuentra la tabla anidada inline-table
            # .find_all('tr')   → [fila con foto y nombre, fila con posición]
            # [1]               → segunda fila
            # .text.strip()     → "Delantero centro"
            position    = row.find('table').find_all('tr')[1].text.strip()

            players.append({
                "player_id": player_id,
                "player_slug": player_slug,
                "player_name": player_name,
                "position":position,
                "team": team_slug
            })
            

        except KeyError as e:
    
            print(f" Fila sin href: {e}")
            continue
        except IndexError as e:
            # split('/') no devuelve suficientes elementos
            # o find_all('tr') no tiene segunda fila
            print(f" Estructura HTML inesperada: {e}")
            continue
        except AttributeError as e:
        # Por si se devuelve None  y se intenta llamar find_all sobre None
            print(f" Elemento no encontrado: {e}")
            continue
        except Exception as e:
            print(f" Error inesperado: {e}")
            print(f" Error inesperado en fila: {type(e).__name__}: {e}")
    
            continue
        
    # pausa aleatoria entre 2 y 4 segundos para evitar bloqueos por parte de la web
    time.sleep(random.uniform(2, 4))
    return players

# ══════════════════════════════════════════════════
# SCRAPING — LESIONES
# ══════════════════════════════════════════════════

def get_player_injuries(player_slug, player_id): 
    """
    Descarga y parsea el historial de lesiones de un jugador en Transfermarkt.

    Realiza una petición HTTP a la ruta de lesiones:
        https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}

    Parsea la tabla HTML con clase 'items', donde cada fila <tr class="odd/even">
    representa una lesión. Solo extrae las lesiones de la temporada TARGET_SEASON
    usando el formato corto de Transfermarkt, ej: "20/21" para 2020/2021.

    Parámetros:
        player_slug (str): slug del jugador en la URL, ej: "karim-benzema"
        player_id   (str): ID del jugador en Transfermarkt, ej: "18922"

    Devuelve:
        list[dict]: lista de lesiones de la temporada TARGET_SEASON, cada una con:
            - season         (str): temporada en formato corto, ej: "20/21"
            - injury_type    (str): tipo de lesión, ej: "Lesión muscular"
            - date_from      (str): fecha de inicio en formato dd/mm/yyyy
            - date_until     (str): fecha de fin en formato dd/mm/yyyy
            - days_absent    (int|None): días de baja, None si no disponible
            - matches_missed (int|None): partidos perdidos, None si no disponible
            - player_id      (str): ID del jugador en Transfermarkt
        [] si hay error en la petición o el jugador no tiene lesiones
    """

    url= f'https://www.transfermarkt.es/{player_slug}/verletzungen/spieler/{player_id}'

    try: 
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # servidor devuelve 4xx o 5xx
        print(f" Error HTTP {e}")
        return []
    except requests.exceptions.ConnectionError as e:
        # no hay conexión
        print(f" Error de conexión: {e}")
        return []
    except requests.exceptions.Timeout as e:
        # la petición tardó demasiado
        print(f"  Timeout: {e}")
        return []
    
    # parsea el html  con BeatifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # las lesiones de un jugador están en la web en una tabla con la clase items 
    table = soup.find('table', class_='items')

    # puede que un jugador no tenga lesiones 
    if not table:
        return []
    
    # cada fila  con la clase odd o even representa una lesion
    rows= table.find_all('tr', class_=['odd','even'])

    """ Estrucutra de cada tr 
    <tr class="odd">
        <td class="zentriert">25/26</td>
        <td class="hauptlink">Lesión en el dedo del pie</td>
        <td class="zentriert">04/04/2026</td>
        <td class="zentriert">06/04/2026</td>
        <td class="rechts">3 dias</td>
        <td class="rechts hauptlink wappen_verletzung">
        <a title="Al-Hilal SFC" href="/al-hilal-riad/startseite/verein/1114/saison_id/2025"><img src="https://tmssl.akamaized.net//images/wappen/tiny/1114.png?lm=1755170975" title="Al-Hilal SFC" alt="Al-Hilal SFC" class="tiny_wappen"></a><span>1</span></td>
    </tr>
    """
    injuries= []
    for row in rows: 
        try:
            # extrae los td de la fila 
            cols= row.find_all('td')
            
            season= cols[0].text.strip()

            # de momento solo interesa la temporado 20/21
            if season =='20/21': 
                injury_type= cols[1].text.strip()
                date_from= cols[2].text.strip()
                date_until= cols[3].text.strip()
                
                ## cols[4] devuelve "3 dias" → limpiamos el texto y convertimos a int
                days_str = cols[4].text.strip().replace(' dias', '').replace(' día', '').strip()
                days_absent = int(days_str) if days_str.isdigit() else None
                
                # los partidos perdidos están dentro de un <span> en la última celda
                ## puede ser None si el jugador no tiene partidos registrados
                span = cols[5].find('span')
                matches_missed = int(span.text.strip()) if span else None

                injuries.append({
                        "season": season,
                        "injury_type": injury_type,
                        "date_from": date_from,
                        "date_until": date_until,
                        "days_absent": days_absent,
                        "matches_missed": matches_missed,
                        "player_id" : player_id
                    }
                )

        except IndexError as e:
            # la fila no tiene la estructura esperada
            print(f" Estructura HTML inesperada: {e}")
            continue
        except AttributeError as e:
            # algún elemento devuelve None al utilizar find sobre él
            print(f" Elemento no encontrado: {e}")
            continue
        except ValueError as e:
            # int() no puede convertir el string
            print(f" Error convirtiendo a número: {e}")
            continue
        except Exception as e:
            print(f" Error inesperado: {e}")
            continue

    # pausa aleatoria entre 2 y 4 segundos para evitar bloqueos por parte de la web
    time.sleep(random.uniform(2, 4))
    return  injuries

# ══════════════════════════════════════════════════
# ORQUESTADOR
# ══════════════════════════════════════════════════

def scrape_transfermarkt():
    
    """
    Orquestador principal del scraping de Transfermarkt.
    Devuelve DataFrames con los jugadores y las lesiones 
    Llama a get_squad()  y a get_players_injuries() 

    Fase 1 — Plantillas:
        Recorre el diccionario TEAMS y llama a get_squad() por cada equipo.
        Acumula todos los jugadores en una lista única independientemente del equipo,
        que se puede filtrar por el campo 'team' de cada diccionario.

    Fase 2 — Lesiones:
        Por cada jugador obtenido en la Fase 1 llama a get_player_injuries()
        usando player_slug y player_id. Solo se guardan las lesiones de
        TARGET_SEASON definida en las constantes de configuración.

    Devuelve:
        tuple[pd.DataFrame, pd.DataFrame]:
            - df_players:  un jugador por fila con player_id, player_slug,
                           player_name, position y team
            - df_injuries: una lesión por fila con season, injury_type,
                           date_from, date_until, days_absent, matches_missed
                           y player_id para cruzar con df_players
        tuple[pd.DataFrame vacío, pd.DataFrame vacío] si no se obtienen jugadores
    """
    all_players= []
    all_injuries= []

    ## obtiene las plantillas de cada equipo 
    for team_slug, team_id in TEAMS.items():
        print(f"\n Obteniendo plantilla de {team_slug}...")
        players = get_squad(team_slug,team_id,SEASON)
        print(f" {len(players)} jugadores encontrados")
        all_players.extend(players)
    
    if not all_players:
        print(" No se obtuvieron jugadores.")
        return pd.DataFrame(), pd.DataFrame()
    

    ## obtiene las lesiones de todos los jugadores 
    for player in all_players: 
        print(f"  → Lesiones de {player['player_name']}...")
        injuries = get_player_injuries(
            player['player_slug'],
            player['player_id']
        )

        all_injuries.extend(injuries)
    
    print(f"\n Resumen:")
    print(f"  Jugadores: {len(all_players)}")
    print(f"  Lesiones:  {len(all_injuries)}")
    
    return pd.DataFrame(all_players),pd.DataFrame(all_injuries)

# ══════════════════════════════════════════════════
# PROGRAMA PRINCIPAL
# ══════════════════════════════════════════════════
def main():

    """
    Punto de entrada del script. 
    
    Llama al orquestador scrape_transfermarkt(), crea el directorio de salida
    OUTPUT_DIR si no existe, y guarda los resultados en dos CSVs:
        - transfermarket_players.csv:  plantillas de los equipos en TEAMS
        - transfermarket_injuries.csv: lesiones de la temporada TARGET_SEASON
    """
    
    print("=" * 55)
    print(f"  Transfermarkt scraper — temporada {TARGET_SEASON}")
    print("=" * 55)

    #  llama al orquestador
    df_players, df_injuries =  scrape_transfermarkt()

    if df_players.empty:
        print(" No se obtuvieron datos.")
        return
    
    #  crea el directorio si no existe. Si existe, no hace nada
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # definición de rutas 
    players_path = os.path.join(OUTPUT_DIR,'transfermarket_players.csv')
    injuries_path = os.path.join(OUTPUT_DIR,'transfermarket_injuries.csv')

    #  guarda los CSVs
    df_players.to_csv(players_path, index= False)
    df_injuries.to_csv(injuries_path,index=False)

    #  imprime rutas de salida
    print(f"\n Archivos guardados:")
    print(f"  {players_path}  ({len(df_players)} filas)")
    print(f"  {injuries_path} ({len(df_injuries)} filas)")

if __name__ == "__main__":
    main()