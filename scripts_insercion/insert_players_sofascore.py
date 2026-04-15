# insert_players_optimized.py
import json
from datetime import datetime
import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from db.connection import get_connection
from utils.player_matcher import PlayerMatcher

SOFASCORE_RAW = ROOT / "data/raw/sofascore"

def extract_players_from_lineups(lineup_dir):
    """Extrae jugadores de lineups"""
    jugadores = {}
    for file in lineup_dir.glob("*.json"):
        with open(file, encoding="utf-8") as f:
            data = json.load(f)
            for side in ['home', 'away']:
                if side in data:
                    for p in data[side].get("players", []):
                        pl = p.get("player", {})
                        pid = pl.get("id")
                        if pid and pid not in jugadores:
                            dob_ts = pl.get("dateOfBirthTimestamp")
                            dob = datetime.fromtimestamp(dob_ts).date() if dob_ts else None
                            jugadores[pid] = {
                                "id_sofascore": pid,
                                "name_canonical": pl.get("name") or f"unknown_{pid}",
                                "player_position": p.get("position"),
                                "nationality": pl.get("country", {}).get("name"),
                                "birth_date": dob,
                                "source": "lineup"
                            }
    return jugadores

def extract_players_from_shots(shotmaps_dir):
    """Extrae jugadores de shots (backfill)"""
    jugadores = {}
    shots_file = shotmaps_dir / "shots_all.csv"
    if shots_file.exists():
        df = pd.read_csv(shots_file)
        for _, row in df.iterrows():
            pid = row.get('player_id')
            if pid and pid not in jugadores:
                jugadores[pid] = {
                    "id_sofascore": int(pid),
                    "name_canonical": row.get('player') or f"unknown_{pid}",
                    "player_position": None,
                    "nationality": None,
                    "birth_date": None,
                    "source": "shot_backfill"
                }
    return jugadores

def main():
    matcher = PlayerMatcher(threshold=85)
    all_players = {}
    
    # 1. Primero, cargar todos los jugadores de lineups
    for team_dir in SOFASCORE_RAW.iterdir():
        if team_dir.is_dir():
            lineup_dir = team_dir / "lineups"
            if lineup_dir.exists():
                players = extract_players_from_lineups(lineup_dir)
                all_players.update(players)
    
    print(f"Jugadores desde lineups: {len(all_players)}")
    
    # 2. Segundo, backfill desde shots (solo los que faltan)
    for team_dir in SOFASCORE_RAW.iterdir():
        if team_dir.is_dir():
            shotmaps_dir = team_dir / "shotmaps"
            if shotmaps_dir.exists():
                shots_players = extract_players_from_shots(shotmaps_dir)
                # Solo añadir los que no existen
                for pid, pdata in shots_players.items():
                    if pid not in all_players:
                        all_players[pid] = pdata
    
    print(f"Total jugadores (incluyendo backfill): {len(all_players)}")
    
    # 3. Insertar/Actualizar usando PlayerMatcher
    with get_connection() as conn:
        with conn.cursor() as cur:
            for pid, pdata in all_players.items():
                db_pid, score = matcher.find_match(
                    pdata['name_canonical'], 
                    source='sofascore', 
                    source_id=pid
                )
                
                if db_pid:
                    cur.execute("""
                        UPDATE dim_player
                        SET id_sofascore = %s,
                            name_canonical = COALESCE(dim_player.name_canonical, %s),
                            player_position = COALESCE(dim_player.player_position, %s),
                            nationality = COALESCE(dim_player.nationality, %s),
                            birth_date = COALESCE(dim_player.birth_date, %s)
                        WHERE player_id = %s
                    """, (pid, pdata['name_canonical'], pdata['player_position'], 
                        pdata['nationality'], pdata['birth_date'], db_pid))
                else:
                    cur.execute("""
                        INSERT INTO dim_player (
                            id_sofascore, name_canonical, player_position, nationality, birth_date
                        )
                        VALUES (%s, %s, %s, %s, %s)
                    """, (pid, pdata['name_canonical'], pdata['player_position'], 
                        pdata['nationality'], pdata['birth_date']))
            
            conn.commit()
            
            cur.execute("SELECT COUNT(*) FROM dim_player")
            total = cur.fetchone()[0]
            print(f"Total jugadores en BD: {total}")

if __name__ == "__main__":
    main()