import json
import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_TM = PROJECT_ROOT / "data" / "raw" / "transfermarkt"

def repair_consolidated():
    log.info("[START] Iniciando reparacion de archivos consolidados de Transfermarkt...")
    
    player_files = list(RAW_TM.glob("**/players.json"))
    injury_files = list(RAW_TM.glob("**/injuries.json"))
    
    log.info(f"Encontrados {len(player_files)} archivos de jugadores y {len(injury_files)} de lesiones.")
    
    all_players = []
    for f in player_files:
        try:
            with open(f, "r", encoding="utf-8") as file:
                data = json.load(file)
                if isinstance(data, list):
                    # Inyectar temporada desde la ruta si no existe
                    # Ruta ejemplo: .../season=2020/...
                    season_part = [p for p in f.parts if "season=" in p]
                    season = int(season_part[0].split("=")[1]) if season_part else None
                    for item in data:
                        if season and "season" not in item:
                            item["season"] = season
                    all_players.extend(data)
        except Exception as e:
            log.error(f"Error procesando {f}: {e}")

    all_injuries = []
    for f in injury_files:
        try:
            with open(f, "r", encoding="utf-8") as file:
                data = json.load(file)
                if isinstance(data, list):
                    all_injuries.extend(data)
        except Exception as e:
            log.error(f"Error procesando {f}: {e}")

    if not all_players:
        log.warning("No se encontraron datos de jugadores para unificar.")
        return

    # Definir directorio oficial
    # En el scraper usamos season=2020-2024 (o similar)
    # Busquemos si ya existe una carpeta season=XXXX-XXXX
    existing_dirs = [d for d in RAW_TM.iterdir() if d.is_dir() and "season=" in d.name and "-" in d.name]
    if existing_dirs:
        target_dir = existing_dirs[0]
    else:
        target_dir = RAW_TM / "season=repaired"
    
    target_dir.mkdir(parents=True, exist_ok=True)
    
    players_path = target_dir / "players_clean.csv"
    injuries_path = target_dir / "injuries_clean.csv"

    # Convertir a DataFrame para transformaciones finales (si fueran necesarias)
    df_p = pd.DataFrame(all_players)
    df_i = pd.DataFrame(all_injuries)

    # Guardar en CSV
    df_p.to_csv(players_path,   index=False, encoding="utf-8-sig")
    df_i.to_csv(injuries_path, index=False, encoding="utf-8-sig")

    log.info(f"[OK] ReparaciÃ³n completada.")
    log.info(f"  - {players_path} ({len(df_p)} registros)")
    log.info(f"  - {injuries_path} ({len(df_i)} registros)")

if __name__ == "__main__":
    repair_consolidated()
