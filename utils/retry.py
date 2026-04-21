"""
utils/retry.py
===============
Decorator para reintentos con backoff exponencial.

Uso:
    @retry(max_attempts=3, delay=1, backoff=2)
    def risky_operation():
        ...
"""

import logging
import time
from functools import wraps
from typing import Callable, Any

log = logging.getLogger(__name__)


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator que reintenta una función con backoff exponencial.
    
    Args:
        max_attempts: Número máximo de intentos (default: 3)
        delay: Pausa inicial en segundos (default: 1.0)
        backoff: Multiplicador para pausa exponencial (default: 2.0)
    
    Ejemplo:
        @retry(max_attempts=3, delay=1, backoff=2)
        def fetch_data():
            return requests.get('https://api.example.com')
        
        # Reintentará hasta 3 veces con pausa de 1s, 2s, 4s
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    log.debug(
                        "Attempt %d/%d for %s",
                        attempt, max_attempts, func.__name__
                    )
                    return func(*args, **kwargs)
                
                except Exception as e:
                    last_exception = e
                    
                    if attempt >= max_attempts:
                        log.error(
                            "Failed after %d attempts for %s: %s",
                            max_attempts, func.__name__, e
                        )
                        raise
                    
                    log.warning(
                        "Attempt %d failed for %s: %s. "
                        "Retrying in %.1f seconds...",
                        attempt, func.__name__, e, current_delay
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator


def retry_async(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator para reintentos en funciones async.
    
    Uso:
        @retry_async(max_attempts=3, delay=1, backoff=2)
        async def fetch_data():
            return await aiohttp_get(url)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            import asyncio
            current_delay = delay
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    log.debug(
                        "Async attempt %d/%d for %s",
                        attempt, max_attempts, func.__name__
                    )
                    return await func(*args, **kwargs)
                
                except Exception as e:
                    last_exception = e
                    
                    if attempt >= max_attempts:
                        log.error(
                            "Async failed after %d attempts for %s: %s",
                            max_attempts, func.__name__, e
                        )
                        raise
                    
                    log.warning(
                        "Async attempt %d failed for %s: %s. "
                        "Retrying in %.1f seconds...",
                        attempt, func.__name__, e, current_delay
                    )
                    
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator
