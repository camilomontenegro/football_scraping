from statsbomb_loader import load_matches, load_events, extract_shots, save_raw
from statsbombpy import sb
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

COMPETITION_ID = 11
SEASON_ID = 90          # Cambia este ID para otra temporada
SEASON_NAME = '2020_2021'  # Cambia el nombre acorde

if __name__ == '__main__':
    matches = load_matches(competition_id=COMPETITION_ID, season_id=SEASON_ID)
    all_shots = []

    for i, match_id in enumerate(matches['match_id']):
        print(f"Partido {i+1}/{len(matches)} — ID {match_id}...")
        try:
            events = load_events(match_id)
            shots = extract_shots(events)
            shots['match_id'] = match_id
            events['match_id'] = match_id
            save_raw(events, 'statsbomb', f'{SEASON_NAME}/events', f'{match_id}.json')
            all_shots.append(shots)
        except Exception as e:
            print(f"  ✗ Error en partido {match_id}: {e}")
            continue

    df_shots = pd.concat(all_shots, ignore_index=True)
    save_raw(df_shots, 'statsbomb', SEASON_NAME, 'shots.json')
    save_raw(matches[['match_id', 'match_date', 'home_team', 'away_team',
                    'home_score', 'away_score', 'competition', 'season']],
            'statsbomb', SEASON_NAME, 'matches.json')
    
    print(f"\n✓ Temporada {SEASON_NAME} completada — {len(df_shots)} tiros")