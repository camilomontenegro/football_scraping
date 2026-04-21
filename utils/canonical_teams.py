"""
utils/canonical_teams.py
=========================
Diccionario de normalización de nombres de equipos.

PROPÓSITO:
    Mapea TODAS las variaciones de nombres de equipo que pueden llegar de las
    distintas fuentes (SofaScore, Transfermarkt, Understat, StatsBomb, WhoScored)
    al nombre CANÓNICO establecido por SofaScore (fuente master de equipos).

USO:
    from utils.canonical_teams import normalize_team_name

    canonical = normalize_team_name("fc barcelona")  → "FC Barcelona"
    canonical = normalize_team_name("Barça")          → "FC Barcelona"
    canonical = normalize_team_name("Levante UD")     → "Levante UD"

MANTENIMIENTO:
    Si aparece una variante nueva de un equipo que no se normaliza bien,
    añadir la entrada en el bloque correspondiente al equipo.
    La clave SIEMPRE va en minúsculas sin tildes.
"""

from __future__ import annotations
import re
import unicodedata


# ── Diccionario de normalización ──────────────────────────────────────────────
# Clave: nombre normalizado (minúsculas, sin tildes, sin puntuación)
# Valor: nombre canónico tal y como aparece en SofaScore

_TEAM_ALIASES: dict[str, str] = {

    # ── Real Madrid ───────────────────────────────────────────────────────────
    "real madrid":               "Real Madrid",
    "real madrid cf":            "Real Madrid",
    "real madrid c f":           "Real Madrid",

    # ── FC Barcelona ─────────────────────────────────────────────────────────
    "fc barcelona":              "FC Barcelona",
    "barcelona":                 "FC Barcelona",
    "f c barcelona":             "FC Barcelona",
    "barca":                     "FC Barcelona",
    "barça":                     "FC Barcelona",

    # ── Atlético de Madrid ────────────────────────────────────────────────────
    "atletico de madrid":        "Atlético de Madrid",
    "atletico madrid":           "Atlético de Madrid",
    "atletico":                  "Atlético de Madrid",
    "atl madrid":                "Atlético de Madrid",
    "club atletico de madrid":   "Atlético de Madrid",
    "atlético de madrid":        "Atlético de Madrid",

    # ── Sevilla FC ─────────────────────────────────────────────────────────────
    "sevilla":                   "Sevilla FC",
    "sevilla fc":                "Sevilla FC",
    "fc sevilla":                "Sevilla FC",

    # ── Real Betis ────────────────────────────────────────────────────────────
    "real betis":                "Real Betis",
    "real betis sevilla":        "Real Betis",
    "betis":                     "Real Betis",

    # ── Real Sociedad ─────────────────────────────────────────────────────────
    "real sociedad":             "Real Sociedad",
    "real sociedad san sebastian":"Real Sociedad",
    "sociedad":                  "Real Sociedad",

    # ── Athletic Club ─────────────────────────────────────────────────────────
    "athletic bilbao":           "Athletic Club",
    "athletic club":             "Athletic Club",
    "athletic":                  "Athletic Club",
    "bilbao":                    "Athletic Club",

    # ── Valencia CF ───────────────────────────────────────────────────────────
    "valencia":                  "Valencia CF",
    "valencia cf":               "Valencia CF",
    "fc valencia":               "Valencia CF",

    # ── Villarreal CF ─────────────────────────────────────────────────────────
    "villarreal":                "Villarreal CF",
    "villarreal cf":             "Villarreal CF",
    "fc villarreal":             "Villarreal CF",
    "yellow submarine":          "Villarreal CF",

    # ── Celta de Vigo ─────────────────────────────────────────────────────────
    "celta de vigo":             "Celta de Vigo",
    "celta vigo":                "Celta de Vigo",
    "rc celta":                  "Celta de Vigo",
    "celta":                     "Celta de Vigo",

    # ── CA Osasuna ────────────────────────────────────────────────────────────
    "osasuna":                   "Osasuna",
    "ca osasuna":                "Osasuna",
    "c a osasuna":               "Osasuna",

    # ── Deportivo Alavés ──────────────────────────────────────────────────────
    "deportivo alaves":          "Deportivo Alavés",
    "alaves":                    "Deportivo Alavés",
    "deportivo alavés":          "Deportivo Alavés",
    "sd alaves":                 "Deportivo Alavés",

    # ── Getafe CF ─────────────────────────────────────────────────────────────
    "getafe":                    "Getafe CF",
    "getafe cf":                 "Getafe CF",
    "fc getafe":                 "Getafe CF",

    # ── Granada CF ────────────────────────────────────────────────────────────
    "granada":                   "Granada CF",
    "granada cf":                "Granada CF",
    "granada c f":               "Granada CF",
    "fc granada":                "Granada CF",
    "f c granada":               "Granada CF",

    # ── Levante UD ────────────────────────────────────────────────────────────
    "levante":                   "Levante UD",
    "levante ud":                "Levante UD",
    "ud levante":                "Levante UD",

    # ── Cádiz CF ──────────────────────────────────────────────────────────────
    "cadiz":                     "Cádiz CF",
    "cadiz cf":                  "Cádiz CF",
    "fc cadiz":                  "Cádiz CF",
    "cádiz cf":                  "Cádiz CF",

    # ── Elche CF ──────────────────────────────────────────────────────────────
    "elche":                     "Elche CF",
    "elche cf":                  "Elche CF",
    "fc elche":                  "Elche CF",

    # ── SD Eibar ──────────────────────────────────────────────────────────────
    "eibar":                     "SD Eibar",
    "sd eibar":                  "SD Eibar",

    # ── SD Huesca ─────────────────────────────────────────────────────────────
    "huesca":                    "SD Huesca",
    "sd huesca":                 "SD Huesca",
    "s d huesca":                "SD Huesca",
    "huesca sd":                 "SD Huesca",

    # ── Real Valladolid ───────────────────────────────────────────────────────
    "valladolid":                "Real Valladolid",
    "real valladolid":           "Real Valladolid",
    "real valladolid cf":        "Real Valladolid",

    # ── Girona FC ────────────────────────────────────────────────────────────
    "girona fc":                 "Girona FC",
    "girona":                    "Girona FC",
    "fc girona":                 "Girona FC",

    # ── CD Leganés ───────────────────────────────────────────────────────────
    "leganes":                   "Leganés",
    "cd leganes":                "Leganés",

    # ── UD Las Palmas ─────────────────────────────────────────────────────────
    "las palmas":                "Las Palmas",
    "ud las palmas":             "Las Palmas",

    # ── RCD Mallorca ──────────────────────────────────────────────────────────
    "mallorca":                  "Mallorca",
    "rcd mallorca":              "Mallorca",

    # ── Rayo Vallecano ───────────────────────────────────────────────────────
    "rayo vallecano":            "Rayo Vallecano",

    # ── UD Almería ───────────────────────────────────────────────────────────
    "almeria":                   "Almería",
    "ud almeria":                "Almería",

    # ── RCD Espanyol ─────────────────────────────────────────────────────────
    "espanyol":                  "Espanyol",
    "espanyol barcelona":        "Espanyol",
    "rcd espanyol":              "Espanyol",
}


# ── Función principal ─────────────────────────────────────────────────────────

def _raw_normalize(name: str) -> str:
    """Convierte un nombre a forma comparable:
    minúsculas · sin tildes · solo letras y espacios · espacios simples.
    """
    if not name:
        return ""
    name = name.lower().strip()
    # Eliminar tildes/diacríticos
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Solo letras, dígitos y espacios
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_team_name(raw_name: str) -> str:
    """Devuelve el nombre canónico (SofaScore) para un nombre de equipo cualquiera.

    Flujo:
        1. Normalizar el string (minúsculas, sin tildes, sin puntuación)
        2. Buscar en el diccionario _TEAM_ALIASES
        3. Si no está → devolver el raw_name original limpio (Title Case)

    Args:
        raw_name: Nombre del equipo tal como viene de cualquier fuente.

    Returns:
        Nombre canónico o raw_name capitalizado si no hay alias conocido.
    """
    if not raw_name:
        return raw_name

    key = _raw_normalize(raw_name)
    canonical = _TEAM_ALIASES.get(key)
    if canonical:
        return canonical

    # Fallback: devolver el raw_name limpio (sin cambiar la capitalización original)
    return raw_name.strip()


def get_canonical_name(normalized_name: str) -> str:
    """Compatibilidad con el API anterior. Usar normalize_team_name() en código nuevo."""
    return _TEAM_ALIASES.get(normalized_name, normalized_name)
