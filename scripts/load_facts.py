"""
load_facts.py
=============
Cargar facts (fact_shots, fact_events, fact_injuries) de forma individual.

Uso:
    python load_facts.py --shots             # Solo fact_shots
    python load_facts.py --events            # Solo fact_events
    python load_facts.py --injuries          # Solo fact_injuries
    python load_facts.py --all               # Todos los facts
    python load_facts.py                     # Default = --all

Requisitos previos:
    1. Datos descargados: python scrape_only.py
    2. Dimensiones cargadas: python load_dimensions.py --all

Flujo completo recomendado:
    1. python scrape_only.py                 # Descargar datos (30-45 min)
    2. python load_dimensions.py --teams     # Cargar equipos (1-2 min)
    3. python load_dimensions.py --players   # Cargar jugadores (2-5 min)
    4. python load_dimensions.py --matches   # Cargar partidos (1-2 min)
    5. python load_facts.py --shots          # Cargar tiros (5-10 min)
    6. python load_facts.py --events         # Cargar eventos (10-15 min)
    7. python load_facts.py --injuries       # Cargar lesiones (1-2 min)
    
Tiempo total: ~1.5 - 2 horas
"""

import argparse
import logging
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

# Importar fact loaders
from loaders.fact_loader import load_shots, load_events, load_injuries


def main():
    parser = argparse.ArgumentParser(
        description="Cargar facts individuales en la base de datos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python load_facts.py --shots             # Solo tiros
  python load_facts.py --events            # Solo eventos
  python load_facts.py --injuries          # Solo lesiones
  python load_facts.py --all               # Todos
  python load_facts.py                     # Default = --all
        """
    )
    
    parser.add_argument("--shots", action="store_true", help="Cargar fact_shots")
    parser.add_argument("--events", action="store_true", help="Cargar fact_events")
    parser.add_argument("--injuries", action="store_true", help="Cargar fact_injuries")
    parser.add_argument("--all", action="store_true", help="Cargar todo")
    
    args = parser.parse_args()
    
    # Si no hay args, cargar todo
    if not any([args.shots, args.events, args.injuries, args.all]):
        args.all = True
    
    # =====================================================
    # CARGAR TIROS
    # =====================================================
    if args.all or args.shots:
        print("\n" + "=" * 60)
        print("📦 Cargando FACT_SHOTS (Hechos de Tiros)")
        print("=" * 60)
        try:
            load_shots()
            print("[OK] fact_shots cargado exitosamente")
        except Exception as e:
            log.error(f"[ERROR] Error cargando fact_shots: {e}", exc_info=True)
            if not args.all:
                return 1
    
    # =====================================================
    # CARGAR EVENTOS
    # =====================================================
    if args.all or args.events:
        print("\n" + "=" * 60)
        print("📦 Cargando FACT_EVENTS (Hechos de Eventos)")
        print("=" * 60)
        try:
            load_events()
            print("[OK] fact_events cargado exitosamente")
        except Exception as e:
            log.error(f"[ERROR] Error cargando fact_events: {e}", exc_info=True)
            if not args.all:
                return 1
    
    # =====================================================
    # CARGAR LESIONES
    # =====================================================
    if args.all or args.injuries:
        print("\n" + "=" * 60)
        print("📦 Cargando FACT_INJURIES (Hechos de Lesiones)")
        print("=" * 60)
        try:
            load_injuries()
            print("[OK] fact_injuries cargado exitosamente")
        except Exception as e:
            log.error(f"[ERROR] Error cargando fact_injuries: {e}", exc_info=True)
            if not args.all:
                return 1
    
    # =====================================================
    # RESUMEN
    # =====================================================
    print("\n" + "=" * 60)
    print("✅ FACTS CARGADOS EXITOSAMENTE")
    print("=" * 60)
    print("\n¡Base de datos completamente poblada!")
    print("\nPróximo paso:")
    print("  python health_check.py --verbose")
    print("  (para verificar integridad de datos)")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
