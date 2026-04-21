#!/usr/bin/env python3
"""
health_check.py
===============
Script de salud para verificar que el pipeline está listo para ejecutarse.

Uso:
    python health_check.py              # Verificar todo
    python health_check.py --verbose    # Con detalles
    python health_check.py --fix        # Intentar crear directorios
"""

import sys
import logging
import argparse
from utils.health import check_all, check_database, check_data_directories, check_schema

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Health check para Football Data Pipeline"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Output detallado"
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Intentar crear directorios faltantes"
    )
    
    args = parser.parse_args()
    
    if args.fix:
        logger.info("Attempting to fix issues...")
        check_data_directories()
    
    all_ok = check_all(verbose=args.verbose)
    
    if all_ok:
        logger.info("\n[+] All systems GO! Ready to run pipeline.")
        return 0
    else:
        logger.error("\n[FAIL] Some issues found. See above for details.")
        logger.error("\nCommon fixes:")
        logger.error("  - Copy .env.example to .env and fill credentials")
        logger.error("  - Run: python db/setup_db.py")
        logger.error("  - Ensure PostgreSQL is running")
        return 1


if __name__ == "__main__":
    sys.exit(main())
