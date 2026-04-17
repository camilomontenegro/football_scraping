from statsbombpy import sb
import pandas as pd
import json
import os
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.append('..')

RAW_PATH = '../data/raw'  # Ajusta la ruta según desde dónde ejecutes

def save_raw(data, source, season, filename):
    """Guarda un DataFrame en data/raw/{source}/{season}/."""
    path = os.path.join(RAW_PATH, source, season)
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, filename)
    
    if isinstance(data, pd.DataFrame):
        data.to_json(filepath, orient='records', force_ascii=False, indent=2)
    else:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"  ✓ Guardado: {source}/{season}/{filename}")

def load_matches(competition_id=11, season_id=90):
    matches = sb.matches(competition_id=competition_id, season_id=season_id)
    print(f"Partidos disponibles: {len(matches)}")
    return matches

def load_events(match_id):
    events = sb.events(match_id=match_id)
    cols = ['id', 'type', 'minute', 'second', 'period', 'player_id', 'player', 
            'team_id', 'team', 'location', 'pass_end_location', 
            'shot_statsbomb_xg', 'shot_outcome', 'under_pressure']
    cols_available = [c for c in cols if c in events.columns]
    return events[cols_available]

def extract_shots(events_df):
    shots = events_df[events_df['type'] == 'Shot'].copy()
    shots['xg'] = shots.get('shot_statsbomb_xg', None)
    shots['x'] = shots['location'].apply(lambda l: l[0] if isinstance(l, list) else None)
    shots['y'] = shots['location'].apply(lambda l: l[1] if isinstance(l, list) else None)
    shots['result'] = shots['shot_outcome']
    shots['data_source'] = 'statsbomb'
    return shots[['player_id', 'player', 'team_id', 'team', 'minute', 'x', 'y', 'xg', 'result', 'data_source']]
