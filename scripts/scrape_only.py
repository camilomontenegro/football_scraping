"""
scrape_only.py
==============
Descargar datos de TODOS los scrapers (sin cargar en BD).

Uso:
    python -m scripts.scrape_only --understat       # Solo Understat
    python -m scripts.scrape_only --sofascore       # Solo SofaScore
    python -m scripts.scrape_only --statsbomb       # Solo StatsBomb
    python -m scripts.scrape_only --transfermarkt   # Solo Transfermarkt
    python -m scripts.scrape_only --all             # Todos los scrapers
    python -m scripts.scrape_only                   # Default = --all

Salida:
    data/raw/understat/
    data/raw/sofascore/
    data/raw/statsbomb/
    data/raw/transfermarkt/

Tiempo estimado:
    - Understat: 15-20 minutos
    - StatsBomb: 5-10 minutos
    - Transfermarkt: 10-15 minutos
    - SofaScore: 2-3 horas
    
DespuÃ©s de descargar:
    python -m scripts.load_dimensions --all
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


async def run_scraper(scraper_name: str, scraper_func):
    """Ejecutar un scraper con manejo de errores."""
    print(f"\n{'=' * 60}")
    print(f"[>] Iniciando {scraper_name.upper()}")
    print(f"{'=' * 60}")
    
    try:
        if asyncio.iscoroutinefunction(scraper_func):
            await scraper_func()
        else:
            scraper_func()
        print(f"[OK] {scraper_name.upper()} completado")
        return True
    except Exception as e:
        log.error(f"[ERROR] Error en {scraper_name}: {e}", exc_info=True)
        return False


async def main():
    parser = argparse.ArgumentParser(
        description="Descargar datos de todos los scrapers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python -m scripts.scrape_only --understat       # Solo Understat (rÃ¡pido)
  python -m scripts.scrape_only --statsbomb       # Solo StatsBomb (muy rÃ¡pido)
  python -m scripts.scrape_only --transfermarkt   # Solo Transfermarkt
  python -m scripts.scrape_only --sofascore       # Solo SofaScore (lento)
  python -m scripts.scrape_only --all             # Todos
  python -m scripts.scrape_only                   # Default = todos
        """
    )
    
    parser.add_argument("--understat", action="store_true", help="Scraper de Understat")
    parser.add_argument("--sofascore", action="store_true", help="Scraper de SofaScore")
    parser.add_argument("--statsbomb", action="store_true", help="Scraper de StatsBomb")
    parser.add_argument("--transfermarkt", action="store_true", help="Scraper de Transfermarkt")
    parser.add_argument("--all", action="store_true", help="Todos los scrapers")
    
    args = parser.parse_args()
    
    # Si no hay args, ejecutar todos
    if not any([args.understat, args.sofascore, args.statsbomb, args.transfermarkt, args.all]):
        args.all = True
    
    # Importar scrapers (lazy import para evitar errores si faltan dependencias)
    try:
        from scrapers.understat_scraper import main as understat_main
        from scrapers.sofascore_scraper import main as sofascore_main
        from scrapers.statsbomb_scraper import main as statsbomb_main
        from scrapers.transfermarkt_scraper import main as transfermarkt_main
    except ImportError as e:
        log.error(f"Error importando scrapers: {e}")
        return 1
    
    # =====================================================
    # UNDERSTAT
    # =====================================================
    if args.all or args.understat:
        success = await run_scraper("understat", understat_main)
        if not success and not args.all:
            return 1
    
    # =====================================================
    # STATSBOMB
    # =====================================================
    if args.all or args.statsbomb:
        success = await run_scraper("statsbomb", statsbomb_main)
        if not success and not args.all:
            return 1
    
    # =====================================================
    # TRANSFERMARKT
    # =====================================================
    if args.all or args.transfermarkt:
        success = await run_scraper("transfermarkt", transfermarkt_main)
        if not success and not args.all:
            return 1
    
    # =====================================================
    # SOFASCORE (Lento, avisar al usuario)
    # =====================================================
    if args.all or args.sofascore:
        if args.sofascore and not args.all:
            print("\n[!] SofaScore es LENTO (~2-3 horas). Â¿Continuar? (s/n)")
            if input().lower() != 's':
                print("Cancelado.")
                return 0
        
        success = await run_scraper("sofascore", sofascore_main)
        if not success and not args.all:
            return 1
    
    # =====================================================
    # RESUMEN
    # =====================================================
    print("\n" + "=" * 60)
    print("[OK] DESCARGA COMPLETADA")
    print("=" * 60)
    
    # Mostrar archivos descargados
    data_dir = Path("data/raw")
    if data_dir.exists():
        print("\n[DATA] Datos disponibles:")
        for source_dir in data_dir.iterdir():
            if source_dir.is_dir():
                files = list(source_dir.rglob("*"))
                print(f"  [+] {source_dir.name}: {len(files)} archivos")
    
    print("\nPrÃ³ximo paso:")
    print("  python -m scripts.load_dimensions --all")
    print("  (para cargar dim_team, dim_player, dim_match)")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
