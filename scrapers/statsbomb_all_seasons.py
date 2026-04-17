from statsbomb_loader import load_matches, load_events, extract_shots, save_raw
from statsbombpy import sb
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

COMPETITION_ID = 11

if __name__ == '__main__':
    competitions = sb.competitions()
    la_liga = competitions[competitions['competition_name'] == 'La Liga']

    for _, comp_row in la_liga.iterrows():
        season_name = comp_row['season_name'].replace('/', '_')
        season_id = comp_row['season_id']

        print(f"\n{'='*50}")
        print(f"Procesando temporada {comp_row['season_name']}...")
        print(f"{'='*50}")

        matches = load_matches(competition_id=COMPETITION_ID, season_id=season_id)
        all_shots = []

        for i, match_id in enumerate(matches['match_id']):
            print(f"Partido {i+1}/{len(matches)} — ID {match_id}...")
            try:
                events = load_events(match_id)
                shots = extract_shots(events)
                shots['match_id'] = match_id
                events['match_id'] = match_id
                save_raw(events, 'statsbomb', f'{season_name}/events', f'{match_id}.json')
                all_shots.append(shots)
            except Exception as e:
                print(f"  ✗ Error en partido {match_id}: {e}")
                continue

        df_shots = pd.concat(all_shots, ignore_index=True)
        save_raw(df_shots, 'statsbomb', season_name, 'shots.json')
        save_raw(matches[['match_id', 'match_date', 'home_team', 'away_team',
                        'home_score', 'away_score', 'competition', 'season']],
                'statsbomb', season_name, 'matches.json')

        print(f"✓ Temporada {comp_row['season_name']} completada — {len(df_shots)} tiros")

    print("\n✓ Todas las temporadas procesadas")