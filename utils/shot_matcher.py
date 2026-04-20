# utils/shot_matcher.py

def es_mismo_tiro(tiro_nuevo, tiros_existentes, umbral_metros=2.0, umbral_minuto=1):
    """
    Determina si un tiro nuevo ya existe en la BD.
    Criterios:
      - Mismo partido (match_id)
      - Mismo jugador (player_id)
      - Minuto similar (±1)
      - Coordenadas similares (distancia < 2 metros)
    Devuelve el shot_id existente si hay match, None si es nuevo.
    """
    import math

    for tiro in tiros_existentes:
        # Verificar minuto
        if abs((tiro_nuevo['minute'] or 0) - (tiro['minute'] or 0)) > umbral_minuto:
            continue

        # Verificar coordenadas
        if tiro_nuevo['x'] is None or tiro['x'] is None:
            continue

        distancia = math.sqrt(
            (tiro_nuevo['x'] - tiro['x']) ** 2 +
            (tiro_nuevo['y'] - tiro['y']) ** 2
        )

        if distancia <= umbral_metros:
            return tiro['shot_id']  # ya existe

    return None  # es nuevo


def prioridad_fuente(fuente):
    """
    Orden de preferencia de fuentes.
    Mayor número = mejor calidad de dato.
    """
    prioridades = {
        'statsbomb': 3,
        'understat':  2,
        'sofascore':  1,
        'whoscored':  1,
    }
    return prioridades.get(fuente, 0)
