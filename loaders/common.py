import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from pathlib import Path

# Load .env with explicit UTF-8 encoding and BOM handling
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    # Read and write back to ensure no BOM issues
    with open(env_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()
    
    # Write back with plain UTF-8 (no BOM)
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(content)

load_dotenv(encoding='utf-8', dotenv_path=env_path)

DB_HOST     = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT_STR = os.getenv("DB_PORT", "5432").strip()
DB_NAME     = os.getenv("DB_NAME", "football_db").strip()
DB_USER     = os.getenv("DB_USER", "postgres").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

if not DB_PASSWORD:
    raise ValueError(
        "DB_PASSWORD environment variable not set. "
        "Copy .env.example to .env and fill in your credentials."
    )

# Convert port to integer safely
try:
    DB_PORT = int(DB_PORT_STR)
except (ValueError, TypeError):
    DB_PORT = 5432

# Use SQLAlchemy URL builder to properly handle encoding
database_url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME
)

engine = create_engine(
    database_url,
    connect_args={"client_encoding": "utf8"}
)

def get_connection():
    return engine.connect()
