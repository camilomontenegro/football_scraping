import sys
from pathlib import Path
from sqlalchemy import text

# Añadir el directorio raíz al path para poder importar loaders
sys.path.insert(0, str(Path(__file__).parent.parent))

from loaders.common import engine

def reset_players():
    print("\n[RESET] Limpiando estado de MDM de jugadores...")
    
    try:
        with engine.begin() as conn:
            # 1. Vaciar la tabla de revisiones
            conn.execute(text("TRUNCATE TABLE player_review RESTART IDENTITY CASCADE;"))
            print("  [OK] Tabla 'player_review' vaciada por completo.")
            
            # 2. Desvincular todas las fuentes secundarias en dim_player
            # Mantenemos id_transfermarkt porque es el "Master" que crea el registro
            conn.execute(text("""
                UPDATE dim_player 
                SET id_sofascore = NULL, 
                    id_understat = NULL, 
                    id_statsbomb = NULL, 
                    id_whoscored = NULL;
            """))
            print("  [OK] IDs de fuentes secundarias desvinculados en 'dim_player'.")
            
        print("\n[OK] ¡Limpieza completada con éxito!")
        print("       Ya puedes volver a cargar los jugadores para que pasen por el nuevo algoritmo.")
        print("       Comando recomendado: python scripts/pipeline_runner.py --load\n")
        
    except Exception as e:
        print(f"\n[ERROR] Ocurrió un error durante la limpieza:\n{e}\n")

if __name__ == "__main__":
    reset_players()
