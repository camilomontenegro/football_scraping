"""
loaders/common.py
==================
Conexion a la BD compartida por todos los loaders, mas helpers comunes
(p.ej. lectura tolerante de CSVs).
"""

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

log = logging.getLogger(__name__)

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DB_HOST     = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT_STR = os.getenv("DB_PORT", "5432").strip()
DB_NAME     = os.getenv("DB_NAME", "db_football_completa").strip()
DB_USER     = os.getenv("DB_USER", "postgres").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

if not DB_PASSWORD:
    raise ValueError(
        "DB_PASSWORD environment variable not set. "
        "Copy .env.example to .env and fill in your credentials."
    )

try:
    DB_PORT = int(DB_PORT_STR)
except (ValueError, TypeError):
    DB_PORT = 5432

database_url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
)

engine = create_engine(database_url)


def get_connection():
    return engine.connect()


def safe_read_csv(path) -> Optional[pd.DataFrame]:
    """Lee un CSV; devuelve None si esta vacio o no parseable.

    Util para loaders que pueden encontrar CSVs vacios (cuando un scraper
    no encontro nuevos partidos tras filtrar por `from_date`, por ejemplo).
    Evita que pd.read_csv() reviente con 'No columns to parse from file'.
    """
    try:
        if Path(path).stat().st_size == 0:
            log.info("  - %s vacio, omitiendo", Path(path).name)
            return None
    except Exception:
        pass
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        log.info("  - %s sin columnas, omitiendo", Path(path).name)
        return None
    except Exception as e:
        log.warning("Error leyendo %s: %s", path, e)
        return None
