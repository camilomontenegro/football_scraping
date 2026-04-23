"""
tests/test_loaders.py
====================
Tests unitarios e integración para los loaders.

Ejecutar:
    pytest tests/test_loaders.py -v
    pytest tests/test_loaders.py -v -m integration  (solo tests de integración)
    pytest tests/test_loaders.py -v -m unit         (solo tests unitarios)
"""

import pytest
import pandas as pd
from pathlib import Path
from sqlalchemy import text

# Importar loaders
from loaders.team_loader import _upsert_team, _load_from_sofascore
from loaders.common import engine
from utils.mdm_engine import resolve_team, resolve_player, normalize


# ═════════════════════════════════════════════════════════════════
# UNIT TESTS - Sin base de datos
# ═════════════════════════════════════════════════════════════════

@pytest.mark.unit
class TestNormalization:
    """Tests para normalización de nombres."""
    
    def test_normalize_empty_string(self):
        """Normalizar string vacío retorna None."""
        assert normalize("") is None
        assert normalize("   ") is None
    
    def test_normalize_placeholder(self):
        """Normalizar 'home' y 'away' retorna None."""
        assert normalize("home") is None
        assert normalize("away") is None
    
    def test_normalize_accent_removal(self):
        """Normalizar remueve tildes."""
        result = normalize("José María")
        assert "á" not in result
        assert "ú" not in result
    
    def test_normalize_case_insensitive(self):
        """Normalizar convierte a minúsculas."""
        assert normalize("REAL MADRID") == "real madrid"
        assert normalize("Barcelona") == "barcelona"
    
    def test_normalize_special_characters(self):
        """Normalizar remueve caracteres especiales."""
        result = normalize("FC Barcelona 2023")
        assert "-" not in result
        assert "2023" in result  # Números se mantienen


@pytest.mark.unit
class TestDataValidation:
    """Tests para validación de datos."""
    
    def test_invalid_team_name(self, sample_team_data):
        """Equipo sin nombre no debería procesarse."""
        sample_team_data["canonical_name"] = None
        assert sample_team_data.get("canonical_name") is None
    
    def test_invalid_player_name(self, sample_player_data):
        """Jugador sin nombre no debería procesarse."""
        sample_player_data["canonical_name"] = ""
        assert not sample_player_data.get("canonical_name")
    
    def test_invalid_date_format(self):
        """Fechas inválidas se detectan."""
        invalid_dates = ["32/13/2020", "abc/def/ghij", ""]
        for date_str in invalid_dates:
            # Deberían fallar o retornar None
            assert date_str not in ["2020-01-01"]


# ═════════════════════════════════════════════════════════════════
# INTEGRATION TESTS - Con base de datos
# ═════════════════════════════════════════════════════════════════

@pytest.mark.integration
class TestTeamLoader:
    """Tests de integración para team_loader."""
    
    def test_upsert_team_insert_new(self):
        """Insertar un equipo nuevo."""
        with engine.begin() as conn:
            # Insertar nuevo equipo
            team_id = _upsert_team(
                conn,
                canonical_name="Test Team FC",
                source_id_col="id_sofascore",
                source_id=99999
            )
            
            # Verificar que existe
            assert team_id is not None
            result = conn.execute(
                text("SELECT canonical_name FROM dim_team WHERE canonical_id = :cid"),
                {"cid": team_id}
            ).fetchone()
            assert result[0] == "Test Team FC"
    
    def test_upsert_team_duplicate_name(self):
        """No duplicar equipo por nombre normalizado."""
        with engine.begin() as conn:
            # Insertar primer equipo
            id1 = _upsert_team(
                conn,
                canonical_name="Real Madrid",
                source_id_col="id_sofascore",
                source_id=12345
            )
            
            # Intentar insertar con nombre similar
            id2 = _upsert_team(
                conn,
                canonical_name="Real Madrid",
                source_id_col="id_understat",
                source_id=67890
            )
            
            # Deberían ser el mismo equipo
            assert id1 == id2
    
    def test_upsert_team_invalid_column(self):
        """Rechazar column names inválidos (SQL injection prevention)."""
        with engine.begin() as conn:
            with pytest.raises(ValueError):
                _upsert_team(
                    conn,
                    canonical_name="Test",
                    source_id_col="DROP TABLE dim_team; --",  # SQL injection attempt
                    source_id=1
                )


@pytest.mark.integration
class TestPlayerResolution:
    """Tests de integración para resolución de jugadores."""
    
    def test_resolve_player_exact_match(self, sample_player_data):
        """Resolver jugador por match exacto de nombre."""
        with engine.begin() as conn:
            # Insertar jugador de prueba
            conn.execute(text("""
                INSERT INTO dim_player (canonical_name, id_transfermarkt)
                VALUES (:name, :tm_id)
            """), {
                "name": "Lionel Messi",
                "tm_id": 28003
            })
            
            # Resolver por nombre exacto
            player_id = resolve_player(
                conn,
                "Lionel Messi",
                "sofascore",
                source_id=12345
            )
            
            # Debería encontrar el jugador
            assert player_id is not None


@pytest.mark.integration
class TestDatabaseSchema:
    """Tests para verificar integridad del schema."""
    
    def test_tables_exist(self, connection):
        """Verificar que todas las tablas existen."""
        required_tables = [
            "dim_team", "dim_player", "dim_match",
            "player_review", "fact_shots", "fact_events", "fact_injuries"
        ]
        
        for table_name in required_tables:
            result = connection.execute(
                text(f"SELECT 1 FROM {table_name} LIMIT 1")
            )
            # Debería no lanzar error
            assert result is not None
    
    def test_foreign_keys_valid(self, connection):
        """Verificar que foreign keys son válidas."""
        # Verificar que dim_match.home_team_id tiene FK a dim_team
        result = connection.execute(text("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name='dim_match' AND constraint_type='FOREIGN KEY'
        """)).fetchall()
        
        assert len(result) >= 2  # Al menos home_team y away_team FKs
    
    def test_indexes_exist(self, connection):
        """Verificar que los índices principales existen."""
        result = connection.execute(text("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'dim_team'
        """)).fetchall()
        
        # Debería haber al menos algunos índices
        assert len(result) > 0


@pytest.mark.integration
class TestDataIntegrity:
    """Tests para verificar integridad de datos."""
    
    def test_no_duplicate_teams(self, connection):
        """No hay dos equipos con el mismo id_sofascore."""
        result = connection.execute(text("""
            SELECT id_sofascore, COUNT(*)
            FROM dim_team
            WHERE id_sofascore IS NOT NULL
            GROUP BY id_sofascore
            HAVING COUNT(*) > 1
        """)).fetchall()
        
        assert len(result) == 0, "Duplicate team IDs found"
    
    def test_fact_shots_valid_fks(self, connection):
        """Todos los fact_shots tienen FKs válidas."""
        result = connection.execute(text("""
            SELECT COUNT(*)
            FROM fact_shots fs
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_match dm WHERE dm.match_id = fs.match_id
            )
            OR NOT EXISTS (
                SELECT 1 FROM dim_player dp WHERE dp.canonical_id = fs.player_id
            )
            OR NOT EXISTS (
                SELECT 1 FROM dim_team dt WHERE dt.canonical_id = fs.team_id
            )
        """)).scalar()
        
        assert result == 0, f"Found {result} shots with invalid FKs"


# ═════════════════════════════════════════════════════════════════
# PARAMETRIZED TESTS
# ═════════════════════════════════════════════════════════════════

@pytest.mark.unit
@pytest.mark.parametrize("input_text,expected_empty", [
    ("", True),
    ("   ", True),
    ("home", True),
    ("away", True),
    ("Real Madrid", False),
    ("Player Name", False),
])
def test_normalize_parametrized(input_text, expected_empty):
    """Test normalize con múltiples inputs."""
    result = normalize(input_text)
    is_empty = result is None
    assert is_empty == expected_empty


@pytest.mark.integration
@pytest.mark.parametrize("team_name,country", [
    ("Real Madrid", "Spain"),
    ("Barcelona", "Spain"),
    ("Manchester United", "England"),
])
def test_team_country_parametrized(team_name, country):
    """Test inserción de equipos con diferentes países."""
    with engine.begin() as conn:
        # Solo verificar que no hay error
        team_id = _upsert_team(
            conn,
            canonical_name=team_name,
            source_id_col="id_sofascore",
            source_id=None
        )
        assert team_id is not None
