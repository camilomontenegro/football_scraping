import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ─────────────────────────────────────────────
# CONEXIÓN A POSTGRESQL
# ─────────────────────────────────────────────

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'football_db')

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear engine
engine = create_engine(DATABASE_URL, echo=False)

# ─────────────────────────────────────────────
# TEST DE CONEXIÓN
# ─────────────────────────────────────────────

def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            print("Conexión a PostgreSQL exitosa")
            return True
    except Exception as e:
        print(f" Error de conexión: {e}")
        print(f"   URL: postgresql+psycopg2://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        return False


if __name__ == "__main__":
    test_connection()