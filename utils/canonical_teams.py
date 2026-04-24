"""
utils/canonical_teams.py
=========================
Diccionario de normalizaciÃ³n de nombres de equipos.

PROPÃ“SITO:
    Mapea TODAS las variaciones de nombres de equipo que pueden llegar de las
    distintas fuentes (SofaScore, Transfermarkt, Understat, StatsBomb, WhoScored)
    al nombre CANÃ“NICO establecido por SofaScore (fuente master de equipos).

USO:
    from utils.canonical_teams import normalize_team_name

    canonical = normalize_team_name("fc barcelona")  â†’ "FC Barcelona"
    canonical = normalize_team_name("BarÃ§a")          â†’ "FC Barcelona"
    canonical = normalize_team_name("Levante UD")     â†’ "Levante UD"

MANTENIMIENTO:
    Si aparece una variante nueva de un equipo que no se normaliza bien,
    aÃ±adir la entrada en el bloque correspondiente al equipo.
    La clave SIEMPRE va en minÃºsculas sin tildes.
"""

from __future__ import annotations
import re
import unicodedata


# â”€â”€ Diccionario de normalizaciÃ³n â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Clave: nombre normalizado (minÃºsculas, sin tildes, sin puntuaciÃ³n)
# Valor: nombre canÃ³nico tal y como aparece en SofaScore

_TEAM_ALIASES: dict[str, str] = {

    # â”€â”€ Real Madrid â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "real madrid":               "Real Madrid",
    "real madrid cf":            "Real Madrid",
    "real madrid c f":           "Real Madrid",

    # â”€â”€ FC Barcelona â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "fc barcelona":              "FC Barcelona",
    "barcelona":                 "FC Barcelona",
    "f c barcelona":             "FC Barcelona",
    "barca":                     "FC Barcelona",
    "barÃ§a":                     "FC Barcelona",

    # â”€â”€ AtlÃ©tico de Madrid â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "atletico de madrid":        "AtlÃ©tico de Madrid",
    "atletico madrid":           "AtlÃ©tico de Madrid",
    "atletico":                  "AtlÃ©tico de Madrid",
    "atl madrid":                "AtlÃ©tico de Madrid",
    "club atletico de madrid":   "AtlÃ©tico de Madrid",
    "atlÃ©tico de madrid":        "AtlÃ©tico de Madrid",

    # â”€â”€ Sevilla FC â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "sevilla":                   "Sevilla FC",
    "sevilla fc":                "Sevilla FC",
    "fc sevilla":                "Sevilla FC",

    # â”€â”€ Real Betis â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "real betis":                "Real Betis",
    "real betis sevilla":        "Real Betis",
    "betis":                     "Real Betis",

    # â”€â”€ Real Sociedad â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "real sociedad":             "Real Sociedad",
    "real sociedad san sebastian":"Real Sociedad",
    "sociedad":                  "Real Sociedad",

    # â”€â”€ Athletic Club â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "athletic bilbao":           "Athletic Club",
    "athletic club":             "Athletic Club",
    "athletic":                  "Athletic Club",
    "bilbao":                    "Athletic Club",

    # â”€â”€ Valencia CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "valencia":                  "Valencia CF",
    "valencia cf":               "Valencia CF",
    "fc valencia":               "Valencia CF",

    # â”€â”€ Villarreal CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "villarreal":                "Villarreal CF",
    "villarreal cf":             "Villarreal CF",
    "fc villarreal":             "Villarreal CF",
    "yellow submarine":          "Villarreal CF",

    # â”€â”€ Celta de Vigo â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "celta de vigo":             "Celta de Vigo",
    "celta vigo":                "Celta de Vigo",
    "rc celta":                  "Celta de Vigo",
    "celta":                     "Celta de Vigo",

    # â”€â”€ CA Osasuna â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "osasuna":                   "Osasuna",
    "ca osasuna":                "Osasuna",
    "c a osasuna":               "Osasuna",

    # â”€â”€ Deportivo AlavÃ©s â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "deportivo alaves":          "Deportivo AlavÃ©s",
    "alaves":                    "Deportivo AlavÃ©s",
    "deportivo alavÃ©s":          "Deportivo AlavÃ©s",
    "sd alaves":                 "Deportivo AlavÃ©s",

    # â”€â”€ Getafe CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "getafe":                    "Getafe CF",
    "getafe cf":                 "Getafe CF",
    "fc getafe":                 "Getafe CF",

    # â”€â”€ Granada CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "granada":                   "Granada CF",
    "granada cf":                "Granada CF",
    "granada c f":               "Granada CF",
    "fc granada":                "Granada CF",
    "f c granada":               "Granada CF",

    # â”€â”€ Levante UD â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "levante":                   "Levante UD",
    "levante ud":                "Levante UD",
    "ud levante":                "Levante UD",

    # â”€â”€ CÃ¡diz CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "cadiz":                     "CÃ¡diz CF",
    "cadiz cf":                  "CÃ¡diz CF",
    "fc cadiz":                  "CÃ¡diz CF",
    "cÃ¡diz cf":                  "CÃ¡diz CF",

    # â”€â”€ Elche CF â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "elche":                     "Elche CF",
    "elche cf":                  "Elche CF",
    "fc elche":                  "Elche CF",

    # â”€â”€ SD Eibar â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "eibar":                     "SD Eibar",
    "sd eibar":                  "SD Eibar",

    # â”€â”€ SD Huesca â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "huesca":                    "SD Huesca",
    "sd huesca":                 "SD Huesca",
    "s d huesca":                "SD Huesca",
    "huesca sd":                 "SD Huesca",

    # â”€â”€ Real Valladolid â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "valladolid":                "Real Valladolid",
    "real valladolid":           "Real Valladolid",
    "real valladolid cf":        "Real Valladolid",

    # â”€â”€ Girona FC â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "girona fc":                 "Girona FC",
    "girona":                    "Girona FC",
    "fc girona":                 "Girona FC",

    # â”€â”€ CD LeganÃ©s â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "leganes":                   "LeganÃ©s",
    "cd leganes":                "LeganÃ©s",

    # â”€â”€ UD Las Palmas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "las palmas":                "Las Palmas",
    "ud las palmas":             "Las Palmas",

    # â”€â”€ RCD Mallorca â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "mallorca":                  "Mallorca",
    "rcd mallorca":              "Mallorca",

    # â”€â”€ Rayo Vallecano â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "rayo vallecano":            "Rayo Vallecano",

    # â”€â”€ UD AlmerÃ­a â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "almeria":                   "AlmerÃ­a",
    "ud almeria":                "AlmerÃ­a",

    # â”€â”€ RCD Espanyol â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "espanyol":                  "Espanyol",
    "espanyol barcelona":        "Espanyol",
    "rcd espanyol":              "Espanyol",

# ── Champions League — WhoScored ──────────────────────────────────────
    "ajax":                  "Ajax de Ámsterdam",        # WS: Ajax
    "arsenal":               "Arsenal FC",               # WS: Arsenal
    "atalanta":              "Atalanta de Bérgamo",      # WS: Atalanta
    "bayern":                "Bayern Múnich",            # WS: Bayern
    "benfica":               "SL Benfica",               # WS: Benfica
    "bodoe glimt":           "Bodø/Glimt",               # WS: Bodoe/Glimt
    "borussia m gladbach":   "Borussia Mönchengladbach", # WS: Borussia M.Gladbach
    "brest":                 "Stade Brestois 29",        # WS: Brest
    "celtic":                "Celtic FC",                # WS: Celtic
    "chelsea":               "Chelsea FC",               # WS: Chelsea
    "club brugge":           "Club Brujas KV",           # WS: Club Brugge
    "copenhagen":            "FC Copenhague",            # WS: Copenhagen
    "eintracht frankfurt":   "Eintracht Fráncfort​​",    # WS: Eintracht Frankfurt
    "inter":                 "Inter de Milán",           # WS: Inter
    "juventus":              "Juventus de Turín",        # WS: Juventus
    "lazio":                 "SS Lazio",                 # WS: Lazio
    "leverkusen":            "Bayer 04 Leverkusen",      # WS: Leverkusen
    "lille":                 "LOSC Lille",               # WS: Lille
    "liverpool":             "Liverpool FC",             # WS: Liverpool
    "man city":              "Manchester City",          # WS: Man City
    "man utd":               "Manchester United",        # WS: Man Utd
    "monaco":                "AS Mónaco",                # WS: Monaco
    "napoli":                "SSC Nápoles",              # WS: Napoli
    "newcastle":             "Newcastle United",         # WS: Newcastle
    "olympiacos":            "Olympiacos El Pireo",      # WS: Olympiacos
    "porto":                 "FC Oporto",                # WS: Porto
    "psg":                   "París Saint-Germain FC",   # WS: PSG
    "psv":                   "PSV Eindhoven",            # WS: PSV
    "rbl":                   "RB Leipzig",               # WS: RBL
    "salzburg":              "Red Bull Salzburgo",       # WS: Salzburg
    "sporting":              "Sporting de Lisboa",       # WS: Sporting
    "tottenham":             "Tottenham Hotspur",  
    "qarabag fk":            "Qarabag FK",
    "qarabag":               "Qarabag FK",
    "bodoe glimt":           "Bodø/Glimt",

}


# â”€â”€ FunciÃ³n principal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _raw_normalize(name: str) -> str:
    """Convierte un nombre a forma comparable:
    minÃºsculas Â· sin tildes Â· solo letras y espacios Â· espacios simples.
    """
    if not name:
        return ""
    name = name.lower().strip()
    # Eliminar tildes/diacrÃ­ticos
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Solo letras, dÃ­gitos y espacios
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_team_name(raw_name: str) -> str:
    """Devuelve el nombre canÃ³nico (SofaScore) para un nombre de equipo cualquiera.

    Flujo:
        1. Normalizar el string (minÃºsculas, sin tildes, sin puntuaciÃ³n)
        2. Buscar en el diccionario _TEAM_ALIASES
        3. Si no estÃ¡ â†’ devolver el raw_name original limpio (Title Case)

    Args:
        raw_name: Nombre del equipo tal como viene de cualquier fuente.

    Returns:
        Nombre canÃ³nico o raw_name capitalizado si no hay alias conocido.
    """
    if not raw_name:
        return raw_name

    key = _raw_normalize(raw_name)
    canonical = _TEAM_ALIASES.get(key)
    if canonical:
        return canonical

    # Fallback: devolver el raw_name limpio (sin cambiar la capitalizaciÃ³n original)
    return raw_name.strip()


def get_canonical_name(normalized_name: str) -> str:
    """Compatibilidad con el API anterior. Usar normalize_team_name() en cÃ³digo nuevo."""
    return _TEAM_ALIASES.get(normalized_name, normalized_name)
