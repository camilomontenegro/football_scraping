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
    Ejecuta consultas a la base de datos y guarda los datos en CSV.
    Exporta todas las tablas del esquema completo.
    """

    df = query_df("SELECT * FROM dim_competition")
    df.to_csv(os.path.join(BASE, 'competitions.csv'), index=False)
    print(f"Competitions: {len(df)} filas")

    df = query_df("SELECT * FROM dim_player")
    df.to_csv(os.path.join(BASE, 'players.csv'), index=False)
    print(f"Players: {len(df)} filas")

    df = query_df("SELECT * FROM dim_team")
    df.to_csv(os.path.join(BASE, 'teams.csv'), index=False)
    print(f"Teams: {len(df)} filas")

    df = query_df("SELECT * FROM dim_match")
    df.to_csv(os.path.join(BASE, 'matches.csv'), index=False)
    print(f"Matches: {len(df)} filas")

    df = query_df("SELECT * FROM player_review")
    df.to_csv(os.path.join(BASE, 'player_review.csv'), index=False)
    print(f"Player review: {len(df)} filas")

    df = query_df("SELECT * FROM fact_injuries")
    df.to_csv(os.path.join(BASE, 'injuries.csv'), index=False)
    print(f"Injuries: {len(df)} filas")

    df = query_df("SELECT * FROM fact_shots")
    df.to_csv(os.path.join(BASE, 'shots.csv'), index=False)
    print(f"Shots: {len(df)} filas")

    df = query_df("SELECT * FROM fact_events")
    df.to_csv(os.path.join(BASE, 'events.csv'), index=False)
    print(f"Events: {len(df)} filas")

    print("--- Exportación completada con éxito ---")
if __name__ == '__main__':
    export_all()
