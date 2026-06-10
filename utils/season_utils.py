"""
utils/season_utils.py
======================
Normalización canónica del campo `season` en dim_match y otros sitios.

Hay varios formatos en los que las distintas fuentes guardan la temporada:
    SofaScore : "LaLiga 25/26"
    WhoScored : "25/26"
    Understat : "2025"
    StatsBomb : "2020/2021"
    Champions : "UEFA Champions League 25/26"

Para evitar fragmentar dim_match con duplicados conceptuales se usa
SIEMPRE el formato canónico 'YYYY/YYYY' tanto al cargar (loaders) como
al consultar (pipeline_runner).
"""

import re
from typing import Optional


def normalize_season(raw_season: Optional[str]) -> Optional[str]:
    """Normaliza cualquier formato de temporada a 'YYYY/YYYY'.

    Ejemplos:
        "LaLiga 20/21"                → "2020/2021"
        "20/21"                       → "2020/2021"
        "2020/2021"                   → "2020/2021"
        "2020/21"                     → "2020/2021"
        "2021"                        → "2021/2022"
        "UEFA Champions League 25/26" → "2025/2026"
    """
    if raw_season is None:
        return None
    raw = str(raw_season).strip()
    if not raw:
        return None

    # Caso año suelto: "2021" → "2021/2022"
    # (Understat suele dar el año de inicio).
    solo_year = re.match(r"^(\d{4})$", raw)
    if solo_year:
        year = int(solo_year.group(1))
        return f"{year}/{year + 1}"

    # Caso "YY/YY", "YYYY/YYYY", "YYYY/YY" embebidos en cualquier texto
    m = re.search(r"(\d{2,4})/(\d{2,4})", raw)
    if not m:
        return None

    start = m.group(1)
    if len(start) == 2:
        # Asumimos siglo 21
        start = "20" + start
    return f"{start}/{int(start) + 1}"
