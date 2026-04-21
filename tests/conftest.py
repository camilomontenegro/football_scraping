"""
tests/conftest.py
=================
Configuración pytest y fixtures compartidas.
"""

import pytest
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar .env para tests
load_dotenv()

# ── DATABASE FIXTURES ────────────────────────────────────

@pytest.fixture(scope="session")
def db_url():
    """Retorna URL de conexión a BD desde .env."""
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "football_db")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    
    if not DB_PASSWORD:
        pytest.skip("DB_PASSWORD not set in .env")
    
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


@pytest.fixture(scope="session")
def engine(db_url):
    """Retorna SQLAlchemy engine para pruebas."""
    try:
        eng = create_engine(db_url, echo=False)
        # Verificar que la BD es accesible
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except Exception as e:
        pytest.skip(f"Cannot connect to database: {e}")


@pytest.fixture
def connection(engine):
    """Proporciona una conexión para cada test."""
    conn = engine.connect()
    yield conn
    conn.close()


# ── DATA FIXTURES ────────────────────────────────────

@pytest.fixture
def sample_team_data():
    """Datos de ejemplo para tests de equipos."""
    return {
        "canonical_name": "Real Madrid CF",
        "country": "Spain",
        "id_sofascore": 100,
    }


@pytest.fixture
def sample_player_data():
    """Datos de ejemplo para tests de jugadores."""
    return {
        "canonical_name": "Cristiano Ronaldo",
        "nationality": "Portugal",
        "birth_date": "1985-02-05",
        "position": "Forward",
        "id_transfermarkt": 5981,
    }


@pytest.fixture
def sample_match_data():
    """Datos de ejemplo para tests de partidos."""
    return {
        "match_date": "2020-09-12",
        "competition": "La Liga",
        "season": "2020/2021",
        "home_score": 3,
        "away_score": 1,
        "data_source": "sofascore",
        "id_sofascore": 12345,
    }


# ── MARKERS ──────────────────────────────────────────

def pytest_configure(config):
    """Registra custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires DB)"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no DB required)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
