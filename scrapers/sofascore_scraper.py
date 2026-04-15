from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import pandas as pd
import json, time, random
from pathlib import Path
from datetime import datetime

# ══════════════════════════════════════════════════
# FUNCIONES AUXILIARES
# ══════════════════════════════════════════════════

def get_json(driver, url, espera=3):
    driver.get(url)
    time.sleep(espera)
    return json.loads(driver.find_element('tag name', 'body').text)

def save_json(data, path):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def random_sleep():
    time.sleep(random.uniform(3, 5))

def get_user_input():
    """Solicita al usuario el ID del equipo y la temporada"""
    print("\n" + "="*55)
    print("EXTRACTOR DE DATOS SOFASCORE")
    print("="*55)
    
    # Solicitar ID del equipo
    while True:
        try:
            team_id = int(input("\n Ingresa el ID del equipo (ejemplo: 2829 para Real Madrid): ").strip())
            break
        except ValueError:
            print(" Error: Debes ingresar un número válido.")
    
    # Solicitar temporada
    season_name = input(" Ingresa la temporada (ejemplo: 20/21, 21/22, 2022/2023): ").strip()
    while not season_name:
        season_name = input(" La temporada no puede estar vacía. Ingresa nuevamente: ").strip()
    
    # Solicitar nombre del equipo (opcional, para las carpetas)
    team_name = input(" Ingresa el nombre del equipo (para las carpetas, ej: Real Madrid): ").strip()
    if not team_name:
        team_name = f"equipo_{team_id}"
    
    # Solicitar tournament_id (opcional, con valor por defecto)
    tournament_input = input(" ID del torneo (opcional, presiona Enter para usar 8 - LaLiga): ").strip()
    tournament_id = int(tournament_input) if tournament_input else 8
    
    print("\n" + "="*55)
    print(f" Configuración guardada:")
    print(f"   Equipo ID: {team_id} ({team_name})")
    print(f"   Temporada: {season_name}")
    print(f"   Torneo ID: {tournament_id}")
    print("="*55 + "\n")
    
    return team_id, team_name, season_name, tournament_id

# ══════════════════════════════════════════════════
# PROGRAMA PRINCIPAL
# ══════════════════════════════════════════════════

def main():
    # Obtener inputs del usuario
    TEAM_ID, TEAM_NAME, SEASON_NAME, TOURNAMENT_ID = get_user_input()
    
    # ── Carpeta de salida ─────────────────────────────
    equipo_slug = TEAM_NAME.lower().replace(' ', '_')
    temporada_clean = SEASON_NAME.replace('/', '_')
    BASE_DIR = Path(f"../data/raw/sofascore/{equipo_slug}_{temporada_clean}")
    
    for folder in ['matches', 'shotmaps', 'statistics', 'incidents', 'lineups']:
        (BASE_DIR / folder).mkdir(parents=True, exist_ok=True)
    
    # ── Abrir navegador ───────────────────────────────
    options = Options()
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    try:
        driver.get('https://www.sofascore.com/')
        time.sleep(5)
        
        # ── Buscar season_id por nombre de temporada ───
        print(f" Buscando temporada '{SEASON_NAME}'...")
        data = get_json(driver, f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/seasons")
        seasons = data.get('seasons', [])
        
        season_id = None
        season_full_name = None
        for s in seasons:
            if SEASON_NAME in s['name']:
                season_id = s['id']
                season_full_name = s['name']
                break
        
        if not season_id:
            print(f"Temporada '{SEASON_NAME}' no encontrada. Disponibles:")
            for s in seasons[:10]:  # Mostrar solo primeras 10
                print(f"  - {s['name']}")
            raise SystemExit
        
        print(f"Temporada encontrada: {season_full_name} (id={season_id})")
        random_sleep()
        
        # ── Descargar todos los partidos de la temporada ──
        print(f"\n Descargando partidos de {season_full_name}...")
        all_events = []
        page = 0
        
        while True:
            url = f"https://api.sofascore.com/api/v1/unique-tournament/{TOURNAMENT_ID}/season/{season_id}/events/last/{page}"
            data = get_json(driver, url)
            events = data.get('events', [])
            
            if not events:
                break
            
            all_events.extend(events)
            print(f"   Página {page}: {len(events)} partidos | total: {len(all_events)}")
            
            if not data.get('hasNextPage', False):
                break
            
            page += 1
            random_sleep()
        
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
        
        for i, row in df_matches.iterrows():
            mid = int(row['match_id'])
            is_home = bool(row['is_home'])
            label = f"{row['home_team']} vs {row['away_team']} ({row['date']})"
            print(f"\n [{i+1}/{total}] {label}")
            
            # Shotmap
            try:
                data = get_json(driver, f"https://api.sofascore.com/api/v1/event/{mid}/shotmap")
                shots = [s for s in data.get('shotmap', []) if s.get('isHome') == is_home]
                save_json(shots, BASE_DIR / 'shotmaps' / f'shotmap_{mid}.json')
                for s in shots:
                    coords = s.get('playerCoordinates') or {}
                    all_shots.append({
                        'match_id': mid,
                        'team': TEAM_NAME,
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
                print(f" Tiros: {len(shots)}")
            except Exception as e:
                print(f" Shotmap: {e}")
            random_sleep()
            
            # Estadísticas
            try:
                data = get_json(driver, f"https://api.sofascore.com/api/v1/event/{mid}/statistics")
                save_json(data, BASE_DIR / 'statistics' / f'statistics_{mid}.json')
                print(f" Estadísticas: OK")
            except Exception as e:
                print(f" Estadísticas: {e}")
            random_sleep()
            
            # Incidentes
            try:
                data = get_json(driver, f"https://api.sofascore.com/api/v1/event/{mid}/incidents")
                incidents = [inc for inc in data.get('incidents', []) if inc.get('isHome') == is_home]
                save_json(incidents, BASE_DIR / 'incidents' / f'incidents_{mid}.json')
                print(f" Incidentes: {len(incidents)}")
            except Exception as e:
                print(f" Incidentes: {e}")
            random_sleep()
            
            # Alineaciones
            try:
                data = get_json(driver, f"https://api.sofascore.com/api/v1/event/{mid}/lineups")
                side = 'home' if is_home else 'away'
                lineup = {side: data.get(side, {})}
                save_json(lineup, BASE_DIR / 'lineups' / f'lineups_{mid}.json')
                print(f"  Alineación: OK")
            except Exception as e:
                print(f"  Alineaciones: {e}")
            random_sleep()
        
        # ── CSV consolidado tiros ─────────────────────
        df_shots = pd.DataFrame(all_shots)
        df_shots.to_csv(BASE_DIR / 'shotmaps' / 'shots_all.csv', index=False)
        
        print(f"\n{'='*55}")
        print(f"  EXTRACCIÓN COMPLETADA")
        print(f"  Equipo    : {TEAM_NAME} (ID: {TEAM_ID})")
        print(f"  Temporada : {season_full_name}")
        print(f"  Partidos  : {len(df_matches)}")
        print(f"  Tiros     : {len(df_shots)}")
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
    main()