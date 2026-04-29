

import re

def normalize_season(raw_season: str) -> str:
    """
    Normaliza cualquier formato de temporada a 'YY/YY'.
    En algunos CSV  lso datos de la temporada para el campo season de DIM_MATCH  
    vienen con texto que no interesa conservar  en los registros 
    Hay que limpiar  el dato y quedarse solo  con la temporada, que es el dato que interesa. 

    Ejemplos:
        "UEFA Champions League 25/26" → "25/26"
        "LaLiga 20/21"                → "20/21"
        "2020/2021"                   → "20/21"
        "2020/21"                     → "20/21"
        "20/21"                       → "20/21"
    """
    
    if not raw_season or not isinstance(raw_season, str):
        return None  # devuelve None en lugar de string vacío para que la BD lo trate como NULL

    match = re.search(r'(\d{2,4})/(\d{2,4})', raw_season)
    if not match:
        return None  # si no encuentra el patrón YY/YY devuelve None

    start = match.group(1)[-2:]
    end   = match.group(2)[-2:]
    return f"{start}/{end}"