"""
utils/health.py
================
Health checks para verifidar conectividad y estado del sistema.

Uso:
    from utils.health import check_database, check_all
    
    if not check_database():
        print("Database unavailable")
        exit(1)
"""

import logging
from pathlib import Path
from typing import Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

log = logging.getLogger(__name__)


def check_database(db_url: str = None) -> bool:
    """Verifica que la BD está disponible y accesible.
    
    Args:
        db_url: Connection string (si no se proporciona, usa .env)
    
    Returns:
        True si BD está OK, False si hay problemas
    """
    if db_url is None:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        
        DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_NAME = os.getenv("DB_NAME", "football_db")
        DB_USER = os.getenv("DB_USER", "postgres")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        
        if not DB_PASSWORD:
            log.error("DB_PASSWORD not set")
            return False
        
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    try:
        engine = create_engine(db_url, echo=False)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        log.info("[OK] Database health check passed")
        return True
    
    except OperationalError as e:
        log.error("[FAIL] Database health check failed: %s", e)
        return False
    except Exception as e:
        log.error("[FAIL] Unexpected error in database health check: %s", e)
        return False


def check_data_directories() -> bool:
    """Verifica que existen los directorios de datos.
    
    Returns:
        True si todos los directorios existen o fueron creados
    """
    required_dirs = [
        Path("data/raw/sofascore"),
        Path("data/raw/understat"),
        Path("data/raw/transfermarkt"),
        Path("data/raw/statsbomb"),
        Path("data/raw/whoscored"),
    ]
    
    try:
        for dir_path in required_dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        log.info("[OK] Data directories health check passed")
        return True
    
    except Exception as e:
        log.error("[FAIL] Data directories health check failed: %s", e)
        return False


def check_schema(db_url: str = None) -> Dict[str, bool]:
    """Verifica que todas las tablas principales existen.
    
    Returns:
        Dict con estado de cada tabla: {'dim_team': True, 'dim_player': True, ...}
    """
    if db_url is None:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        
        DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_NAME = os.getenv("DB_NAME", "football_db")
        DB_USER = os.getenv("DB_USER", "postgres")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        
        if not DB_PASSWORD:
            log.error("DB_PASSWORD not set")
            return {}
        
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    required_tables = [
        "dim_team",
        "dim_player",
        "dim_match",
        "player_review",
        "fact_shots",
        "fact_events",
        "fact_injuries",
    ]
    
    results = {}
    
    try:
        engine = create_engine(db_url, echo=False)
        with engine.connect() as conn:
            for table_name in required_tables:
                try:
                    result = conn.execute(
                        text(f"SELECT 1 FROM {table_name} LIMIT 1")
                    )
                    result.fetchone()
                    results[table_name] = True
                except Exception:
                    results[table_name] = False
        
        if all(results.values()):
            log.info("[OK] Schema health check passed")
        else:
            missing = [k for k, v in results.items() if not v]
            log.warning("[WARNING] Missing tables: %s", missing)
        
        return results
    
    except Exception as e:
        log.error("[FAIL] Schema health check failed: %s", e)
        return {table: False for table in required_tables}


def check_all(verbose: bool = True) -> bool:
    """Ejecuta todos los health checks.
    
    Args:
        verbose: Si True, imprime resultados detallados
    
    Returns:
        True si todos los checks pasaron
    """
    if verbose:
        log.info("═" * 60)
        log.info("  RUNNING HEALTH CHECKS")
        log.info("═" * 60)
    
    checks = [
        ("Database Connection", check_database),
        ("Data Directories", check_data_directories),
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        try:
            passed = check_func()
            status = "[PASS]" if passed else "[FAIL]"
            if verbose:
                log.info(f"{check_name:.<40} {status}")
            all_passed = all_passed and passed
        except Exception as e:
            if verbose:
                log.error(f"{check_name:.<40} [ERROR]: {e}")
            all_passed = False
    
    # Schema check returns dict
    schema_results = check_schema()
    if schema_results:
        schema_passed = all(schema_results.values())
        status = "[PASS]" if schema_passed else "[FAIL]"
        if verbose:
            log.info(f"{'Database Schema':.<40} {status}")
            if schema_results and not schema_passed:
                missing = [k for k, v in schema_results.items() if not v]
                for table in missing:
                    log.warning(f"  - Missing table: {table}")
        all_passed = all_passed and schema_passed
    
    if verbose:
        log.info("═" * 60)
        final_status = "[OK] ALL CHECKS PASSED" if all_passed else "[FAIL] SOME CHECKS FAILED"
        log.info(f"  {final_status}")
        log.info("═" * 60)
    
    return all_passed


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s"
    )
    
    if check_all(verbose=True):
        sys.exit(0)
    else:
        sys.exit(1)
