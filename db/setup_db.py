# db/setup_db.py
# Crea la base de datos y carga el schema usando SQLAlchemy.
# Esta conexion  se utlzia solo para crear la base de datos 
# Uso:
#   pip install sqlalchemy psycopg2-binary python-dotenv
#   python db/setup_db.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "football_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    print(" ERROR: DB_PASSWORD must be set in .env file")
    print(" See .env.example for reference")
    raise SystemExit(1)

SQL_PATH = os.path.join(os.path.dirname(__file__), "create_tables.sql")


# ── PASO 1: Crear la base de datos ────────────────────────
# Nos conectamos a 'postgres' (base por defecto) porque
# football_db todavía no existe.
# isolation_level="AUTOCOMMIT" es obligatorio para CREATE DATABASE.
print(" Conectando a PostgreSQL...")

url_postgres = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/postgres"

try:
    engine_postgres = create_engine(url_postgres, isolation_level="AUTOCOMMIT")

    with engine_postgres.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME}
        )
        if result.fetchone():
            print(f" La base de datos '{DB_NAME}' ya existe, continuando...")
        else:
            conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
            print(f" Base de datos '{DB_NAME}' creada correctamente")

except OperationalError as e:
    print(f" Error al conectar con PostgreSQL: {e}")
    print("\n Comprueba que:")
    print("   - PostgreSQL está arrancado")
    print("   - El usuario y contraseña en .env son correctos")
    raise SystemExit(1)


# ── PASO 2: Ejecutar el script SQL ────────────────────────
print(f"\n Ejecutando {SQL_PATH}...")

url_football = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(url_football, isolation_level="AUTOCOMMIT")

    with open(SQL_PATH, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(sql))

    print(" Schema instalado correctamente\n")

except FileNotFoundError:
    print(f" No se encuentra el archivo SQL en: {SQL_PATH}")
    print(" Asegúrate de que create_tables_v4.sql está en la carpeta db/")
    raise SystemExit(1)
except ProgrammingError as e:
    print(f" Error en el SQL: {e}")
    raise SystemExit(1)


# ── PASO 3: Verificar tablas creadas ──────────────────────
try:
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        tablas = [row[0] for row in result]

    print(f" Tablas creadas ({len(tablas)}):")
    for t in tablas:
        print(f"   - {t}")

except Exception as e:
    print(f" No se pudo verificar las tablas: {e}")

print("\n Todo listo. Ya puedes usar football_db.")