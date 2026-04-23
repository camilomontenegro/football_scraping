import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pathlib import Path

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
    database=DB_NAME
)

engine = create_engine(database_url)

def get_connection():
    return engine.connect()