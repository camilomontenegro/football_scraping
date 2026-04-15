import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from db.connection import get_connection

def main():
    print("\n" + "="*50)
    print("INSERTANDO PARTIDOS SOFASCORE")
    print("="*50)
    
    madrid_path = ROOT / "data/raw/sofascore/real_madrid_20_21/matches/matches.csv"
    barca_path = ROOT / "data/raw/sofascore/barcelona_20_21/matches/matches.csv"
    
    if not madrid_path.exists():
        print("Archivo no encontrado: " + str(madrid_path))
        return
    if not barca_path.exists():
        print("Archivo no encontrado: " + str(barca_path))
        return
    
    df_madrid = pd.read_csv(madrid_path)
    df_barca = pd.read_csv(barca_path)
    
    df_matches = pd.concat([df_madrid, df_barca]).drop_duplicates("match_id")
    
    print("Partidos a procesar: " + str(len(df_matches)))
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            
            insertados = 0
            omitidos = 0
            
            for _, row in df_matches.iterrows():
                cur.execute("""
                    INSERT INTO dim_match (
                        id_sofascore,
                        match_date,
                        competition,
                        season,
                        home_team,
                        away_team,
                        home_score,
                        away_score,
                        data_source
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_sofascore) DO NOTHING
                """, (
                    int(row["match_id"]),
                    row["date"],
                    row["competition"],
                    "2020-21",
                    row["home_team"],
                    row["away_team"],
                    int(row["home_score"]) if pd.notna(row["home_score"]) else None,
                    int(row["away_score"]) if pd.notna(row["away_score"]) else None,
                    "sofascore"
                ))
                
                if cur.rowcount > 0:
                    insertados += 1
                else:
                    omitidos += 1
            
            conn.commit()
    
    print("Partidos insertados: " + str(insertados))
    print("Partidos omitidos (ya existian): " + str(omitidos))
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dim_match")
            total = cur.fetchone()[0]
            print("Total partidos en BD: " + str(total))

if __name__ == "__main__":
    main()