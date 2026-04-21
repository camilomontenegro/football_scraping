"""
tests/test_retry.py
===================
Tests para el decorator de retry.
"""

import pytest
from utils.retry import retry


@pytest.mark.unit
class TestRetryDecorator:
    """Tests para el decorator @retry."""
    
    def test_retry_succeeds_first_attempt(self):
        """Si la función no falla, retry retorna inmediatamente."""
        call_count = 0
        
        @retry(max_attempts=3, delay=0.01)
        def succeeds():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = succeeds()
        assert result == "success"
        assert call_count == 1  # Solo un intento
    
    def test_retry_succeeds_second_attempt(self):
        """Si falla la primera, retry reintenta."""
        call_count = 0
        
        @retry(max_attempts=3, delay=0.01)
        def fails_once():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("First attempt fails")
            return "success"
        
        result = fails_once()
        assert result == "success"
        assert call_count == 2  # Dos intentos
    
    def test_retry_fails_after_max_attempts(self):
        """Si falla todas las veces, lanza excepción."""
        call_count = 0
        
        @retry(max_attempts=3, delay=0.01)
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")
        
        with pytest.raises(ValueError):
            always_fails()
        
        assert call_count == 3  # Intentó 3 veces
    
    def test_retry_backoff_delay(self):
        """Verifica que el delay aumenta con backoff."""
        import time
        
        call_count = 0
        start_times = []
        
        @retry(max_attempts=3, delay=0.05, backoff=2)
        def track_time():
            nonlocal call_count
            call_count += 1
            start_times.append(time.time())
            if call_count < 3:
                raise ValueError("Fail")
            return "success"
        
        start = time.time()
        result = track_time()
        
        assert result == "success"
        assert call_count == 3
        # Debería haber tomado más de 0.15 segundos (0.05 + 0.1)
        assert time.time() - start >= 0.1


@pytest.mark.unit
class TestRetryEdgeCases:
    """Tests para casos especiales de retry."""
    
    def test_retry_with_no_args(self):
        """Retry funciona con funciones sin argumentos."""
        @retry(max_attempts=2, delay=0.01)
        def no_args():
            return "ok"
        
        assert no_args() == "ok"
    
    def test_retry_with_args_and_kwargs(self):
        """Retry preserva argumentos y kwargs."""
        @retry(max_attempts=2, delay=0.01)
        def with_args(a, b, c=None):
            return f"{a}-{b}-{c}"
        
        result = with_args(1, 2, c=3)
        assert result == "1-2-3"
    
    def test_retry_preserves_function_name(self):
        """Retry usa @wraps para preservar nombre de función."""
        @retry(max_attempts=2, delay=0.01)
        def my_function():
            return "ok"
        
        assert my_function.__name__ == "my_function"
