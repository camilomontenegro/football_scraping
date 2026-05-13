

import re

def normalize_season(raw_season: str) -> str:
    """
    Normaliza cualquier formato de temporada a 'YYYY/YYYY'.
    En algunos CSV  los datos de la temporada para el campo season de DIM_MATCH  
    vienen con texto que no interesa conservar  en los registros. Hay que limpiar  el dato y quedarse solo  con la temporada, que es el dato que interesa. 
    También pueden venir con formato distinto a YYYY/YYYY

    Ejemplos:
        Normaliza cualquier formato de temporada a 'YYYY/YYYY'.

    Ejemplos:
        "LaLiga 20/21"                → "2020/2021"
        "20/21"                       → "2020/2021"
        "2020/2021"                   → "2020/2021"
        "2020/21"                     → "2020/2021"
        "2021"                        → "2021/2022"
        "UEFA Champions League 25/26" → "2025/2026"
       
    """

    if not raw_season or not isinstance(raw_season, str):
        return None

    # Caso año suelto: "2021" → "2021/2022"
    # Este es el supuesto de understat 
    solo_year = re.match(r'^(\d{4})$', raw_season.strip())
    if solo_year:
        year = int(solo_year.group(1))
        return f"{year}/{year + 1}"

    m = re.search(r'(\d{2,4})/(\d{2,4})', raw_season)
    if not m:
        return None

    start = m.group(1)

    # Si start es YY, expandir a YYYY asumiendo siglo 21
    if len(start) == 2:
        start = "20" + start

    return f"{start}/{int(start) + 1}"