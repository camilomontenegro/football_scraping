"""
tests/test_health.py
====================
Tests para health checks.
"""

import pytest
from utils.health import (
    check_database, check_data_directories, 
    check_schema, check_all
)


@pytest.mark.integration
class TestHealthChecks:
    """Tests para funciones de health check."""
    
    def test_check_database_connectivity(self):
        """Verificar que check_database se ejecuta sin error."""
        # Puede pasar o fallar dependiendo si BD está disponible
        result = check_database()
        assert isinstance(result, bool)
    
    def test_check_data_directories(self):
        """Verificar que check_data_directories crea directorios."""
        result = check_data_directories()
        assert isinstance(result, bool)
        assert result is True  # Debería poder crear directorios
    
    def test_check_schema_returns_dict(self):
        """check_schema retorna diccionario con estado de tablas."""
        result = check_schema()
        assert isinstance(result, dict)
        
        # Si la BD está disponible, debería tener entradas
        if result:
            required_tables = [
                "dim_team", "dim_player", "dim_match",
                "player_review", "fact_shots", "fact_events", "fact_injuries"
            ]
            for table in required_tables:
                assert table in result
                assert isinstance(result[table], bool)
    
    def test_check_all_returns_boolean(self):
        """check_all retorna boolean y no lanza excepciones."""
        result = check_all(verbose=False)
        assert isinstance(result, bool)


@pytest.mark.unit
class TestHealthCheckIntegration:
    """Tests que verifican la integración de health checks."""
    
    def test_health_check_can_run_standalone(self):
        """Health checks pueden ejecutarse de forma independiente."""
        # Simplemente verificar que no lanzan excepciones
        try:
            check_data_directories()
        except Exception as e:
            pytest.fail(f"check_data_directories failed: {e}")
    
    def test_all_checks_handle_failures_gracefully(self):
        """check_all maneja fallos de checks individuales."""
        # Debería retornar bool incluso si algún check falla
        result = check_all(verbose=False)
        assert result is not None
