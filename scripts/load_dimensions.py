"""
load_dimensions.py
==================
Cargar dimensiones (dim_player, dim_team, dim_match) de forma individual.

Uso:
    python -m scripts.load_dimensions --teams          # Cargar solo equipos
    python -m scripts.load_dimensions --players        # Cargar solo jugadores
    python -m scripts.load_dimensions --matches        # Cargar solo partidos
    python -m scripts.load_dimensions --all            # Cargar todos
    python -m scripts.load_dimensions                  # Sin args = --all

Flujo recomendado:
    1. python -m scrapers.understat_scraper        # Descargar datos
    2. python -m scripts.load_dimensions --teams   # Cargar dim_team
    3. python -m scripts.load_dimensions --players # Cargar dim_player
    4. python -m scripts.load_dimensions --matches # Cargar dim_match
    5. python -m scripts.pipeline_runner --load-facts # Cargar facts
"""

import argparse
import logging
import sys
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Importar loaders
from loaders.team_loader import load_teams
from loaders.player_loader import load_players
from loaders.match_loader import load_matches


def main():
    parser = argparse.ArgumentParser(
        description="Cargar dimensiones individuales en la base de datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python load_dimensions.py --teams          # Solo equipos
  python load_dimensions.py --players        # Solo jugadores
  python load_dimensions.py --matches        # Solo partidos
  python load_dimensions.py --all            # Todos
  python load_dimensions.py                  # Default = --all
        """
    )
    
    parser.add_argument("--teams", action="store_true", help="Cargar dim_team")
    parser.add_argument("--players", action="store_true", help="Cargar dim_player")
    parser.add_argument("--matches", action="store_true", help="Cargar dim_match")
    parser.add_argument("--all", action="store_true", help="Cargar todo")
    
    args = parser.parse_args()
    
    # Cargar todo si no se especifica nada
    if not any([args.teams, args.players, args.matches, args.all]):
        args.all = True
    
    # Usar engine.begin() para asegurar que los cambios se guarden (COMMIT) automÃ¡ticamente al terminar
    from loaders.common import engine
    
    try:
        with engine.begin() as conn:
            # =====================================================
            # CARGAR EQUIPOS
            # =====================================================
            if args.all or args.teams:
                print("\n" + "=" * 60)
                print("[+] Cargando DIM_TEAM (DimensiÃ³n de Equipos)")
                print("=" * 60)
                load_teams(conn)
                print("[OK] dim_team cargado exitosamente")
            
            # =====================================================
            # CARGAR JUGADORES
            # =====================================================
            if args.all or args.players:
                print("\n" + "=" * 60)
                print("[+] Cargando DIM_PLAYER (DimensiÃ³n de Jugadores)")
                print("=" * 60)
                load_players(conn)
                print("[OK] dim_player cargado exitosamente")
            
            # =====================================================
            # CARGAR PARTIDOS
            # =====================================================
            if args.all or args.matches:
                print("\n" + "=" * 60)
                print("[+] Cargando DIM_MATCH (DimensiÃ³n de Partidos)")
                print("=" * 60)
                load_matches(conn)
                print("[OK] dim_match cargado exitosamente")
            
            print("\n" + "=" * 60)
            print("[OK] DIMENSIONES CARGADAS Y GUARDADAS (COMMIT) EXITOSAMENTE")
            print("=" * 60)
            print("\nProximo paso:")
            print("  python pipeline_runner.py --load-facts")
            
    except Exception as e:
        log.error(f"[FATAL] El proceso de carga fallÃ³ y los cambios han sido revertidos: {e}", exc_info=True)
        return 1
    
    return 0



if __name__ == "__main__":
    sys.exit(main())
