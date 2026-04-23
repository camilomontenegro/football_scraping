#!/usr/bin/env python3
"""
Script de prueba para diagnosticar problemas de conexión a la BD.
"""
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

print("[TEST] Iniciando diagnóstico de conexión...")

# Step 1: Verify .env file
print("\n[1] Verificando archivo .env:")
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    print(f"  [OK] .env existe: {env_file}")
    with open(env_file, 'r', encoding='utf-8') as f:
        content = f.read()
        # Don't print password, just verify it's there
        lines = content.strip().split('\n')
        for line in lines:
            if 'DB_PASSWORD' in line:
                print(f"  [OK] DB_PASSWORD está definido: {line[:30]}...")
            else:
                print(f"  [OK] {line}")
else:
    print(f"  [ERROR] .env no existe")
    sys.exit(1)

# Step 2: Load environment variables
print("\n[2] Cargando variables de entorno:")
from dotenv import load_dotenv
load_dotenv(encoding='utf-8')

DB_HOST = os.getenv("DB_HOST", "127.0.0.1").strip()
DB_PORT_STR = os.getenv("DB_PORT", "5432").strip()
DB_NAME = os.getenv("DB_NAME", "football_db").strip()
DB_USER = os.getenv("DB_USER", "postgres").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

print(f"  DB_HOST: {DB_HOST}")
print(f"  DB_PORT: {DB_PORT_STR}")
print(f"  DB_NAME: {DB_NAME}")
print(f"  DB_USER: {DB_USER}")
print(f"  DB_PASSWORD: {'[SET]' if DB_PASSWORD else '[NOT SET]'}")

if not DB_PASSWORD:
    print("  [ERROR] DB_PASSWORD no está configurado")
    sys.exit(1)

# Step 3: Try to create engine
print("\n[3] Creando SQLAlchemy engine:")
try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    
    try:
        DB_PORT = int(DB_PORT_STR)
    except (ValueError, TypeError):
        DB_PORT = 5432
        print(f"  [WARNING] Puerto inválido, usando 5432")
    
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME
    )
    
    print(f"  URL (oculta): postgresql+psycopg2://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    engine = create_engine(
        database_url,
        connect_args={"client_encoding": "utf8"}
    )
    print(f"  [OK] Engine creado exitosamente")
    
except Exception as e:
    print(f"  [ERROR] No se pudo crear engine: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Test connection
print("\n[4] Probando conexión a la BD:")
try:
    from sqlalchemy import text
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print(f"  [OK] Conexión exitosa")
        
except Exception as e:
    print(f"  [ERROR] No se pudo conectar: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n[SUCCESS] Todas las pruebas pasaron correctamente!")
