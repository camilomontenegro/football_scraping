from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import json
import time
import random
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count
from functools import partial

# ══════════════════════════════════════════════════
# CONFIGURACIÓN OPTIMIZADA
# ══════════════════════════════════════════════════

HEADLESS = True  # Si quiero ver el navegador lo tengo que cambiar a false. 
ESPERA_API = 0.3  
ESPERA_MIN = 0.1
ESPERA_MAX = 0.4
PARALLEL_WORKERS = 4  # Número de partidos simultáneos con los que la API trabaja
PAGE_LOAD_STRATEGY = 'eager'  # 'normal', 'eager', 'none'

# ══════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ══════════════════════════════════════════════════

def create_driver():
    """Crea un driver de Chrome con configuración optimizada"""
    options = Options()
    
    if HEADLESS:
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
    
    # Optimizaciones que he ido recibiendo para reducir los tiempos 
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-images')
    options.add_argument('--disable-javascript')  # Las APIs no necesitan JS
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    
    # Estrategia de carga de página
    options.page_load_strategy = PAGE_LOAD_STRATEGY
    
    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

def get_json_optimized(driver, url, timeout=2):
    """Obtiene JSON de API con espera optimizada"""
    driver.get(url)
    
    # Esperar a que el body tenga contenido (aleatoria)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element('tag name', 'body').text.strip()) > 0
        )
    except:
        pass  # Si timeout, continuar de todas formas
    
    # Pausa mínima solo si es necesario
    time.sleep(ESPERA_API)
    
    return json.loads(driver.find_element('tag name', 'body').text)

def random_sleep_optimized():
    """Pausa aleatoria más corta"""
    time.sleep(random.uniform(ESPERA_MIN, ESPERA_MAX))

def save_json(data, path):
    """Guarda datos en JSON"""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user_input():
    """Solicita al usuario el ID del equipo y la temporada"""
    print("\n" + "="*55)
    print("EXTRACTOR DE DATOS SOFASCORE (VERSIÓN OPTIMIZADA)")
    print("="*55)
    
    while True:
        try:
            team_id = int(input("\n Ingresa el ID del equipo (ejemplo: 2829 para Real Madrid): ").strip())
            break
        except ValueError:
            print(" Error: Debes ingresar un número válido.")
    
    season_name = input(" Ingresa la temporada (ejemplo: 20/21, 21/22, 2022/2023): ").strip()
    while not season_name:
        season_name = input(" La temporada no puede estar vacía. Ingresa nuevamente: ").strip()
    
    team_name = input(" Ingresa el nombre del equipo (para las carpetas, ej: Real Madrid): ").strip()
    if not team_name:
        team_name = f"equipo_{team_id}"
    
    tournament_input = input(" ID del torneo (opcional, presiona Enter para usar 8 - LaLiga): ").strip()
    tournament_id = int(tournament_input) if tournament_input else 8
    
    # Opción de paralelización
    use_parallel = input(" Usar procesamiento paralelo? (s/N): ").strip().lower() == 's'
    
    print("\n" + "="*55)
    print(f" Configuración guardada:")
    print(f"   Equipo ID: {team_id} ({team_name})")
    print(f"   Temporada: {season_name}")
    print(f"   Torneo ID: {tournament_id}")
    print(f"   Paralelo: {'Sí' if use_parallel else 'No'}")
    print("="*55 + "\n")
    
    return team_id, team_name, season_name, tournament_id, use_parallel

# ══════════════════════════════════════════════════
# PROCESAMIENTO DE PARTIDOS (VERSIÓN SECUENCIAL)
# ══════════════════════════════════════════════════

def procesar_partido_secuencial(driver, row, base_dir, team_name, idx, total):
    """Procesa un partido de forma secuencial"""
    mid = int(row['match_id'])
    is_home = bool(row['is_home'])
    label = f"{row['home_team']} vs {row['away_team']} ({row['date']})"
    
    print(f"\n [{idx+1}/{total}] {label}")
    
    resultados = {'shots': 0, 'stats': False, 'incidents': 0, 'lineups': False}
    
    # Shotmap
    try:
        data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/shotmap")
        shots = [s for s in data.get('shotmap', []) if s.get('isHome') == is_home]
        save_json(shots, base_dir / 'shotmaps' / f'shotmap_{mid}.json')
        
        # Procesar shots para CSV
        shots_data = []
        for s in shots:
            coords = s.get('playerCoordinates') or {}
            shots_data.append({
                'match_id': mid,
                'team': team_name,
                'date': row['date'],
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'player': s.get('player', {}).get('name'),
                'player_id': s.get('player', {}).get('id'),
                'minute': s.get('time'),
                'shot_type': s.get('shotType'),
                'situation': s.get('situation'),
                'xg': s.get('xg'),
                'is_goal': s.get('isGoal'),
                'x_raw': coords.get('x'),
                'y_raw': coords.get('y'),
            })
        resultados['shots'] = len(shots)
        resultados['shots_data'] = shots_data
        print(f" Tiros: {len(shots)}")
    except Exception as e:
        print(f" Shotmap: {e}")
        resultados['shots_data'] = []
    
    random_sleep_optimized()
    
    # Estadísticas
    try:
        data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/statistics")
        save_json(data, base_dir / 'statistics' / f'statistics_{mid}.json')
        resultados['stats'] = True
        print(f" Estadísticas: OK")
    except Exception as e:
        print(f" Estadísticas: {e}")
    
    random_sleep_optimized()
    
    # Incidentes
    try:
        data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/incidents")
        incidents = [inc for inc in data.get('incidents', []) if inc.get('isHome') == is_home]
        save_json(incidents, base_dir / 'incidents' / f'incidents_{mid}.json')
        resultados['incidents'] = len(incidents)
        print(f" Incidentes: {len(incidents)}")
    except Exception as e:
        print(f" Incidentes: {e}")
    
    random_sleep_optimized()
    
    # Alineaciones
    try:
        data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/lineups")
        side = 'home' if is_home else 'away'
        lineup = {side: data.get(side, {})}
        save_json(lineup, base_dir / 'lineups' / f'lineups_{mid}.json')
        resultados['lineups'] = True
        print(f" Alineación: OK")
    except Exception as e:
        print(f" Alineaciones: {e}")
    
    random_sleep_optimized()
    
    return resultados

# ══════════════════════════════════════════════════
# PROCESAMIENTO DE PARTIDOS (VERSIÓN PARALELA)
# ══════════════════════════════════════════════════

def procesar_partido_paralelo(args):
    """Procesa un partido en un proceso independiente"""
    mid, is_home, row_dict, base_dir_str, team_name, idx, total = args
    
    # Convertir strings a Path
    base_dir = Path(base_dir_str)
    row = pd.Series(row_dict)
    
    # Crear driver propio para este proceso
    driver = create_driver()
    
    try:
        print(f"\n [{idx+1}/{total}] Iniciando partido {mid}")
        
        resultados = {'shots': 0, 'stats': False, 'incidents': 0, 'lineups': False}
        
        # Shotmap
        try:
            data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/shotmap")
            shots = [s for s in data.get('shotmap', []) if s.get('isHome') == is_home]
            save_json(shots, base_dir / 'shotmaps' / f'shotmap_{mid}.json')
            
            shots_data = []
            for s in shots:
                coords = s.get('playerCoordinates') or {}
                shots_data.append({
                    'match_id': mid,
                    'team': team_name,
                    'date': row['date'],
                    'home_team': row['home_team'],
                    'away_team': row['away_team'],
                    'player': s.get('player', {}).get('name'),
                    'player_id': s.get('player', {}).get('id'),
                    'minute': s.get('time'),
                    'shot_type': s.get('shotType'),
                    'situation': s.get('situation'),
                    'xg': s.get('xg'),
                    'is_goal': s.get('isGoal'),
                    'x_raw': coords.get('x'),
                    'y_raw': coords.get('y'),
                })
            resultados['shots'] = len(shots)
            resultados['shots_data'] = shots_data
            print(f" [{idx+1}/{total}] Partido {mid}: {len(shots)} tiros")
        except Exception as e:
            print(f" [{idx+1}/{total}] Partido {mid} - Shotmap error: {e}")
            resultados['shots_data'] = []
        
        # Estadísticas
        try:
            data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/statistics")
            save_json(data, base_dir / 'statistics' / f'statistics_{mid}.json')
            resultados['stats'] = True
        except Exception as e:
            print(f" [{idx+1}/{total}] Partido {mid} - Stats error: {e}")
        
        # Incidentes
        try:
            data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/incidents")
            incidents = [inc for inc in data.get('incidents', []) if inc.get('isHome') == is_home]
            save_json(incidents, base_dir / 'incidents' / f'incidents_{mid}.json')
            resultados['incidents'] = len(incidents)
        except Exception as e:
            print(f" [{idx+1}/{total}] Partido {mid} - Incidents error: {e}")
        
        # Alineaciones
        try:
            data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/event/{mid}/lineups")
            side = 'home' if is_home else 'away'
            lineup = {side: data.get(side, {})}
            save_json(lineup, base_dir / 'lineups' / f'lineups_{mid}.json')
            resultados['lineups'] = True
        except Exception as e:
            print(f" [{idx+1}/{total}] Partido {mid} - Lineups error: {e}")
        
        print(f" [{idx+1}/{total}] Partido {mid} completado")
        return resultados
        
    except Exception as e:
        print(f" [{idx+1}/{total}] Error fatal en partido {mid}: {e}")
        return {'shots': 0, 'shots_data': [], 'stats': False, 'incidents': 0, 'lineups': False}
    finally:
        driver.quit()

# ══════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL
# ══════════════════════════════════════════════════

def main():
    # Obtener inputs del usuario
    TEAM_ID, TEAM_NAME, SEASON_NAME, TOURNAMENT_ID, USE_PARALLEL = get_user_input()
    
    # ── Carpeta de salida ─────────────────────────────
    equipo_slug = TEAM_NAME.lower().replace(' ', '_')
    temporada_clean = SEASON_NAME.replace('/', '_')
    BASE_DIR = Path(f"../data/raw/sofascore/{equipo_slug}_{temporada_clean}")
    
    for folder in ['matches', 'shotmaps', 'statistics', 'incidents', 'lineups']:
        (BASE_DIR / folder).mkdir(parents=True, exist_ok=True)
    
    # ── Driver principal para obtener datos iniciales ──
    driver = create_driver()
    
    try:
        print(" Inicializando navegador...")
        driver.get('https://www.sofascore.com/')
        time.sleep(2)  # Espera inicial reducida
        
        # ── Buscar season_id por nombre de temporada ───
        print(f" Buscando temporada '{SEASON_NAME}'...")
        data = get_json_optimized(driver, f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/seasons")
        seasons = data.get('seasons', [])
        
        season_id = None
        season_full_name = None
        for s in seasons:
            if SEASON_NAME in s['name']:
                season_id = s['id']
                season_full_name = s['name']
                break
        
        if not season_id:
            print(f" Temporada '{SEASON_NAME}' no encontrada. Disponibles:")
            for s in seasons[:10]:
                print(f"  - {s['name']}")
            raise SystemExit
        
        print(f" Temporada encontrada: {season_full_name} (id={season_id})")
        random_sleep_optimized()
        
        # ── Descargar todos los partidos de la temporada ──
        print(f"\n Descargando partidos de {season_full_name}...")
        all_events = []
        page = 0
        
        while True:
            url = f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/season/{season_id}/events/last/{page}"
            data = get_json_optimized(driver, url)
            events = data.get('events', [])
            
            if not events:
                break
            
            all_events.extend(events)
            print(f"   Página {page}: {len(events)} partidos | total: {len(all_events)}")
            
            if not data.get('hasNextPage', False):
                break
            
            page += 1
            random_sleep_optimized()
        
        print(f" Total partidos en temporada: {len(all_events)}")
        
        # ── Filtrar partidos del equipo por ID ──────────
        partidos_equipo = []
        for e in all_events:
            home_id = e.get('homeTeam', {}).get('id')
            away_id = e.get('awayTeam', {}).get('id')
            if home_id == TEAM_ID or away_id == TEAM_ID:
                ts = e.get('startTimestamp', 0)
                partidos_equipo.append({
                    'match_id': e['id'],
                    'team_name': TEAM_NAME,
                    'team_id': TEAM_ID,
                    'is_home': home_id == TEAM_ID,
                    'date': datetime.fromtimestamp(ts).strftime('%Y-%m-%d') if ts else None,
                    'competition': e.get('tournament', {}).get('name', ''),
                    'home_team': e.get('homeTeam', {}).get('name', ''),
                    'home_team_id': e.get('homeTeam', {}).get('id'),
                    'away_team': e.get('awayTeam', {}).get('name', ''),
                    'away_team_id': e.get('awayTeam', {}).get('id'),
                    'home_score': e.get('homeScore', {}).get('current'),
                    'away_score': e.get('awayScore', {}).get('current'),
                })
        
        if not partidos_equipo:
            print(f" No se encontraron partidos para el equipo {TEAM_ID} en esta temporada")
            raise SystemExit
        
        df_matches = pd.DataFrame(partidos_equipo)
        df_matches.to_csv(BASE_DIR / 'matches' / 'matches.csv', index=False)
        print(f" Partidos encontrados: {len(df_matches)}")
        
        # ── Descargar datos de cada partido ───────────
        all_shots = []
        total = len(df_matches)
        
        if USE_PARALLEL and total > 1:
            print(f"\n Usando procesamiento paralelo con {PARALLEL_WORKERS} workers...")
            
            # Preparar argumentos para procesamiento paralelo
            partidos_args = []
            for idx, row in df_matches.iterrows():
                partidos_args.append((
                    int(row['match_id']),
                    bool(row['is_home']),
                    row.to_dict(),
                    str(BASE_DIR),
                    TEAM_NAME,
                    idx,
                    total
                ))
            
            # Procesar en paralelo
            with Pool(processes=min(PARALLEL_WORKERS, total)) as pool:
                resultados = pool.map(procesar_partido_paralelo, partidos_args)
            
            # Recopilar todos los shots
            for res in resultados:
                if res.get('shots_data'):
                    all_shots.extend(res['shots_data'])
            
        else:
            print(f"\n Usando procesamiento secuencial...")
            for idx, row in df_matches.iterrows():
                resultado = procesar_partido_secuencial(driver, row, BASE_DIR, TEAM_NAME, idx, total)
                if resultado.get('shots_data'):
                    all_shots.extend(resultado['shots_data'])
        
        # ── CSV consolidado tiros ─────────────────────
        if all_shots:
            df_shots = pd.DataFrame(all_shots)
            df_shots.to_csv(BASE_DIR / 'shotmaps' / 'shots_all.csv', index=False)
            total_shots = len(df_shots)
        else:
            total_shots = 0
        
        print(f"\n{'='*55}")
        print(f"  EXTRACCIÓN COMPLETADA")
        print(f"  Equipo    : {TEAM_NAME} (ID: {TEAM_ID})")
        print(f"  Temporada : {season_full_name}")
        print(f"  Partidos  : {len(df_matches)}")
        print(f"  Tiros     : {total_shots}")
        print(f"  Modo      : {'Paralelo' if USE_PARALLEL else 'Secuencial'}")
        print(f"  Datos en  : {BASE_DIR.resolve()}")
        print(f"{'='*55}")
        
    except Exception as ex:
        print(f"\n ERROR: {ex}")
        import traceback
        traceback.print_exc()
    
    finally:
        driver.quit()
        print("\n Driver cerrado.")

if __name__ == "__main__":
    # Configuración para multiprocessing en Windows
    from multiprocessing import freeze_support
    freeze_support()
    main()