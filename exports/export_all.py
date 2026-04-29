import os
import pandas as pd
from sqlalchemy import text
from loaders.common import engine

# Ejecución desde la raiz -> python exports/export_all.py

BASE = os.path.dirname(__file__)
os.makedirs(BASE, exist_ok=True)


def query_df(sql):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def export_all():
    """
    Ejecuta consultas a la base de datos y  guarda los datos, primero en Datagramas y luego en csv 
    """
    
    df = query_df("SELECT * FROM dim_player")
    df.to_csv(os.path.join(BASE, 'players.csv'), index=False)
    print(f"Players: {len(df)} filas")

    df = query_df("SELECT * FROM dim_team")
    df.to_csv(os.path.join(BASE, 'teams.csv'), index=False)
    print(f"Teams: {len(df)} filas")

    df = query_df("SELECT * FROM dim_match")
    df.to_csv(os.path.join(BASE, 'matches.csv'), index=False)
    print(f"Matches: {len(df)} filas")

    df = query_df("SELECT * FROM fact_injuries WHERE season = '24/25'")
    df.to_csv(os.path.join(BASE, 'injuries_2024_25.csv'), index=False)
    print(f"Injuries: {len(df)} filas")

    df = query_df("""
        SELECT fs.* FROM fact_shots fs
        JOIN dim_match dm ON fs.match_id = dm.match_id
        WHERE dm.season = 'LaLiga 24/25'
    """)
    df.to_csv(os.path.join(BASE, 'shots_2024_25.csv'), index=False)
    print(f"Shots: {len(df)} filas")

    df = query_df("""
        SELECT fe.* FROM fact_events fe
        JOIN dim_match dm ON fe.match_id = dm.match_id
        WHERE dm.season = 'LaLiga 24/25'
    """)
    df.to_csv(os.path.join(BASE, 'events_2024_25.csv'), index=False)
    print(f"Events: {len(df)} filas")

    print("--- Exportación completada con éxito ---")

if __name__ == '__main__':
    export_all()
