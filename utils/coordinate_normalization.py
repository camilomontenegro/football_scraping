# ── Sistema de coordenadas ────────────────────────────────────────────────────
# Las coordenadas x e y representan la posición en el campo de juego.
# El sistema de referencia es 0-1 en ambos ejes, donde (0,0) es la esquina
# inferior izquierda y (1,1) la esquina superior derecha.
#
# Cada fuente devuelve las coordenadas en un formato distinto:
#   - Understat  : las coordenadas vienen en formato 0-1 de la api. El scrapper   las deja como están 
#   - WhoScored  : rango 0-100 en origen, normalizadas a 0-1 en el scraper
#   - SofaScore  : rango 0-100, sin normalizar en el scraper → se normaliza  a 0-1 en el loader
#

from typing import Optional

def _normalize_coordinates(x: Optional[float], y: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    """Normaliza coordenadas al rango 0-1. Comprueba si las coordenadas ya vienen normalizadas , en cuyo caso no hace nada.. 
    
    - Understat devuelve las coordenadas ya normalizadas (0-1).

    - WhoScored  devuelve las coordenadas en el formato 0-100.  El scraper  normaliza → llegan en 0-1 → la función las deja como están.

    - SofaScore no normaliza en el scraper → llegan en 0-100 → la función divide entre 100.
    Si x > 1 se asume que no está normalizado → divide entre 100.
    Si x <= 1 ya está normalizado → no toca nada.

    round deja 4 decimales. 
    """
    x_norm = round(x / 100, 4) if x is not None and x > 1 else x
    y_norm = round(y / 100, 4) if y is not None and y > 1 else y
    return x_norm, y_norm