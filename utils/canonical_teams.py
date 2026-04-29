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
 
    # ── La Liga ───────────────────────────────────────────────────────────────
 
    # Real Madrid
    "real madrid":                  "Real Madrid",
    "real madrid cf":               "Real Madrid",
    "real madrid c f":              "Real Madrid",
 
    # FC Barcelona
    "fc barcelona":                 "FC Barcelona",
    "barcelona":                    "FC Barcelona",
    "f c barcelona":                "FC Barcelona",
    "barca":                        "FC Barcelona",
    "barca":                        "FC Barcelona",
 
    # Atlético de Madrid
    "atletico de madrid":           "Atlético de Madrid",
    "atletico madrid":              "Atlético de Madrid",
    "atletico":                     "Atlético de Madrid",
    "atl madrid":                   "Atlético de Madrid",
    "club atletico de madrid":      "Atlético de Madrid",
    "atletico madrid":              "Atlético de Madrid",
 
    # Sevilla FC
    "sevilla":                      "Sevilla FC",
    "sevilla fc":                   "Sevilla FC",
    "fc sevilla":                   "Sevilla FC",
 
    # Real Betis
    "real betis":                   "Real Betis",
    "real betis sevilla":           "Real Betis",
    "betis":                        "Real Betis",
 
    # Real Sociedad
    "real sociedad":                "Real Sociedad",
    "real sociedad san sebastian":  "Real Sociedad",
    "sociedad":                     "Real Sociedad",
 
    # Athletic Club
    "athletic bilbao":              "Athletic Club",
    "athletic club":                "Athletic Club",
    "athletic":                     "Athletic Club",
    "bilbao":                       "Athletic Club",
 
    # Valencia CF
    "valencia":                     "Valencia CF",
    "valencia cf":                  "Valencia CF",
    "fc valencia":                  "Valencia CF",
 
    # Villarreal CF
    "villarreal":                   "Villarreal CF",
    "villarreal cf":                "Villarreal CF",
    "fc villarreal":                "Villarreal CF",
    "yellow submarine":             "Villarreal CF",
 
    # Celta de Vigo
    "celta de vigo":                "Celta de Vigo",
    "celta vigo":                   "Celta de Vigo",
    "rc celta":                     "Celta de Vigo",
    "celta":                        "Celta de Vigo",
 
    # CA Osasuna
    "osasuna":                      "Osasuna",
    "ca osasuna":                   "Osasuna",
    "c a osasuna":                  "Osasuna",
 
    # Deportivo Alavés
    "deportivo alaves":             "Deportivo Alavés",
    "alaves":                       "Deportivo Alavés",
    "deportivo alaves":             "Deportivo Alavés",
    "sd alaves":                    "Deportivo Alavés",
 
    # Getafe CF
    "getafe":                       "Getafe CF",
    "getafe cf":                    "Getafe CF",
    "fc getafe":                    "Getafe CF",
 
    # Granada CF
    "granada":                      "Granada CF",
    "granada cf":                   "Granada CF",
    "granada c f":                  "Granada CF",
    "fc granada":                   "Granada CF",
    "f c granada":                  "Granada CF",
 
    # Levante UD
    "levante":                      "Levante UD",
    "levante ud":                   "Levante UD",
    "ud levante":                   "Levante UD",
 
    # Cádiz CF
    "cadiz":                        "Cádiz CF",
    "cadiz cf":                     "Cádiz CF",
    "fc cadiz":                     "Cádiz CF",
 
    # Elche CF
    "elche":                        "Elche CF",
    "elche cf":                     "Elche CF",
    "fc elche":                     "Elche CF",
 
    # SD Eibar
    "eibar":                        "SD Eibar",
    "sd eibar":                     "SD Eibar",
 
    # SD Huesca
    "huesca":                       "SD Huesca",
    "sd huesca":                    "SD Huesca",
    "s d huesca":                   "SD Huesca",
    "huesca sd":                    "SD Huesca",
 
    # Real Valladolid
    "valladolid":                   "Real Valladolid",
    "real valladolid":              "Real Valladolid",
    "real valladolid cf":           "Real Valladolid",
 
    # Girona FC
    "girona fc":                    "Girona FC",
    "girona":                       "Girona FC",
    "fc girona":                    "Girona FC",
 
    # Leganés
    "leganes":                      "Leganés",
    "cd leganes":                   "Leganés",
 
    # Las Palmas
    "las palmas":                   "Las Palmas",
    "ud las palmas":                "Las Palmas",
 
    # Mallorca
    "mallorca":                     "Mallorca",
    "rcd mallorca":                 "Mallorca",
 
    # Rayo Vallecano
    "rayo vallecano":               "Rayo Vallecano",
 
    # Almería
    "almeria":                      "Almería",
    "ud almeria":                   "Almería",
 
    # Espanyol
    "espanyol":                     "Espanyol",
    "espanyol barcelona":           "Espanyol",
    "rcd espanyol":                 "Espanyol",
 
 
    # ── Champions League — Transfermarkt → SofaScore ─────────────────────────
    # Estos aliases mapean los nombres en español de Transfermarkt
    # al nombre canónico de SofaScore
 
    "1 fc union berlin":            "1. FC Union Berlin",   # TM: 1.FC Unión Berlín
    "ac milan":                     "Milan",                # TM: AC Milan
    "ac sparta praga":              "AC Sparta Praha",      # TM: AC Sparta Praga
    "ajax de amsterdam":            "AFC Ajax",             # TM: Ajax de Ámsterdam
    "as monaco":                    "AS Monaco",            # TM: AS Mónaco
    "basaksehir fk":                "Başakşehir FK",        # TM: Basaksehir FK
    "bayern munich":                "FC Bayern München",    # TM: Bayern Múnich
    "besiktas jk":                  "Beşiktaş JK",         # TM: Besiktas JK
    "club brujas kv":               "Club Brugge KV",       # TM: Club Brujas KV
    "estrella roja de belgrado":    "FK Crvena zvezda",     # TM: Estrella Roja de Belgrado
    "fc copenhague":                "FC København",         # TM: FC Copenhague
    "fc dinamo de kiev":            "Dynamo Kyiv",          # TM: FC Dinamo de Kiev
    "fc oporto":                    "FC Porto",             # TM: FC Oporto
    "fc sheriff tiraspol":          "Sheriff Tiraspol",     # TM: FC Sheriff Tiraspol
    "fc viktoria plzen":            "FC Viktoria Plzeň",    # TM: FC Viktoria Plzen
    "fk krasnodar":                 "FC Krasnodar",         # TM: FK Krasnodar
    "lokomotiv moscu":              "Lokomotiv Moscow",     # TM: Lokomotiv Moscú
    "malmoe ff":                    "Malmö FF",             # TM: Malmoe FF
    "olympiacos el pireo":          "Olympiacos FC",        # TM: Olympiacos El Pireo
    "olympique de marsella":        "Olympique de Marseille", # TM: Olympique de Marsella
    "paris saint germain fc":       "Paris Saint-Germain",  # TM: París Saint-Germain FC
    "rangers fc":                   "Rangers",              # TM: Rangers FC
    "red bull salzburgo":           "Red Bull Salzburg",    # TM: Red Bull Salzburgo
    "royal amberes fc":             "Royal Antwerp FC",     # TM: Royal Amberes FC
    "sc braga":                     "Sporting Braga",       # TM: SC Braga
    "slovan bratislava":            "ŠK Slovan Bratislava", # TM: Slovan Bratislava
    "sporting de lisboa":           "Sporting CP",          # TM: Sporting de Lisboa
    "stade brestois 29":            "Dynamo Brest",         # TM: Stade Brestois 29
    "stade rennais fc":             "Stade Rennais",        # TM: Stade Rennais FC
    "vfl wolfsburgo":               "VfL Wolfsburg",        # TM: VfL Wolfsburgo
    "zenit de san petersburgo":     "Zenit St. Petersburg", # TM: Zenit de San Petersburgo
    "bolonia":                      "Bologna",              # TM: Bolonia
    "atalanta de bergamo":          "Atalanta",             # TM: Atalanta de Bérgamo
    "inter de milan":               "Inter",                # TM: Inter de Milán
    "juventus de turin":            "Juventus",             # TM: Juventus de Turín
    "ss lazio":                     "Lazio",                # TM: SS Lazio
    "losc lille":                   "Lille",                # TM: LOSC Lille
    "manchester city":              "Manchester City",
    "manchester united":            "Manchester United",
    "liverpool fc":                 "Liverpool",            # TM: Liverpool FC
    "chelsea fc":                   "Chelsea",              # TM: Chelsea FC
    "arsenal fc":                   "Arsenal",              # TM: Arsenal FC
    "tottenham hotspur":            "Tottenham Hotspur",
    "newcastle united":             "Newcastle United",
    "bayer 04 leverkusen":          "Bayer 04 Leverkusen",
    "borussia monchengladbach":     "Borussia M'gladbach",  # TM: Borussia Mönchengladbach
    "eintracht francfort":          "Eintracht Frankfurt",  # TM: Eintracht Fráncfort
    "celtic fc":                    "Celtic",               # TM: Celtic FC
    "sl benfica":                   "Benfica",              # TM: SL Benfica
    "psv eindhoven":                "PSV Eindhoven",
    "rb leipzig":                   "RB Leipzig",
    "ssc napoles":                  "Napoli",               # TM: SSC Nápoles
 
 
    # ── Champions League — WhoScored → SofaScore ──────────────────────────────
    # Estos aliases mapean los nombres cortos de WhoScored
    # al nombre canónico de SofaScore
 
    "ajax":                         "AFC Ajax",             # WS: Ajax
    "arsenal":                      "Arsenal",              # WS: Arsenal
    "atalanta":                     "Atalanta",             # WS: Atalanta
    "bayern":                       "FC Bayern München",    # WS: Bayern
    "benfica":                      "Benfica",              # WS: Benfica
    "bodoe glimt":                  "Bodø/Glimt",           # WS: Bodoe/Glimt
    "borussia m gladbach":          "Borussia M'gladbach",  # WS: Borussia M.Gladbach
    "brest":                        "Dynamo Brest",         # WS: Brest
    "celtic":                       "Celtic",               # WS: Celtic
    "chelsea":                      "Chelsea",              # WS: Chelsea
    "club brugge":                  "Club Brugge KV",       # WS: Club Brugge
    "copenhagen":                   "FC København",         # WS: Copenhagen
    "eintracht frankfurt":          "Eintracht Frankfurt",  # WS: Eintracht Frankfurt
    "inter":                        "Inter",                # WS: Inter
    "juventus":                     "Juventus",             # WS: Juventus
    "lazio":                        "Lazio",                # WS: Lazio
    "leverkusen":                   "Bayer 04 Leverkusen",  # WS: Leverkusen
    "lille":                        "Lille",                # WS: Lille
    "liverpool":                    "Liverpool",            # WS: Liverpool
    "man city":                     "Manchester City",      # WS: Man City
    "man utd":                      "Manchester United",    # WS: Man Utd
    "monaco":                       "AS Monaco",            # WS: Monaco
    "napoli":                       "Napoli",               # WS: Napoli
    "newcastle":                    "Newcastle United",     # WS: Newcastle
    "olympiacos":                   "Olympiacos FC",        # WS: Olympiacos
    "porto":                        "FC Porto",             # WS: Porto
    "psg":                          "Paris Saint-Germain",  # WS: PSG
    "psv":                          "PSV Eindhoven",        # WS: PSV
    "rbl":                          "RB Leipzig",           # WS: RBL
    "salzburg":                     "Red Bull Salzburg",    # WS: Salzburg
    "sporting":                     "Sporting CP",          # WS: Sporting
    "tottenham":                    "Tottenham Hotspur",    # WS: Tottenham
    "qarabag fk":                   "Qarabağ FK",           # WS: Qarabag FK
    "qarabag":                      "Qarabağ FK",           # WS: Qarabag
 
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
