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
    "real betis balompie":          "Real Betis",
 
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
             # TM: Stade Brestois 29
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
    "ssc napoles":                  "SSC Napoli",     
    "qarabag fk":                   "Qarabağ FK",
    "qarabag":                      "Qarabağ FK",
          # TM: SSC Nápoles
 
 
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
    # con este equipo brest  hay un problema. whoscored tiene el mismo nombre para dos equipos difrentes.
    # Dynamo brest-> equipo bielorruso
    # En ligue 1 hay otro  que whoscored llama brest pero es un equipo frances llamado Stade Brestois
    # comento la linea de momento ya que ya esta cargado en la bd
    #"brest":                        "Dynamo Brest",         # WS: Brest
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
    "liverpool":                    "Liverpool FC",         # WS: Liverpool
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
    "fk bodo glimt":                "Bodø/Glimt",

     
    # ── Premier League ────────────────────────────────────────────────────────────

    # Transfermarkt
    "afc bournemouth":              "Bournemouth",
    "arsenal fc":                   "Arsenal",
    "brentford fc":                 "Brentford",
    "burnley fc":                   "Burnley",
    "chelsea fc":                   "Chelsea",
    "everton fc":                   "Everton",
    "fulham fc":                    "Fulham",
    "southampton fc":               "Southampton",
    "watford fc":                   "Watford",
    "wolverhampton wanderers":      "Wolverhampton",
    "sunderland afc":               "Sunderland",

    # WhoScored y Understat (comparten muchos nombres cortos)
    "brighton":                     "Brighton & Hove Albion",
    "ipswich":                      "Ipswich Town",
    "leeds":                        "Leeds United",
    "leicester":                    "Leicester City",
    "liverpool":                    "Liverpool FC",
    "luton":                        "Luton Town",
    "man city":                     "Manchester City",
    "man utd":                      "Manchester United",
    "newcastle":                    "Newcastle United",
    "norwich":                      "Norwich City",
    "sheff utd":                    "Sheffield United",
    "wba":                          "West Bromwich Albion",
    "west ham":                     "West Ham United",
    "wolves":                       "Wolverhampton",
    "sheffield united":             "Sheffield United",
    "west bromwich albion":         "West Bromwich Albion",
    "newcastle united":             "Newcastle United",
    "manchester city":              "Manchester City",
    "manchester united":            "Manchester United",

    # ── Ligue 1 ───────────────────────────────────────────────────────────────────

    # Transfermarkt → SofaScore
    "ac ajaccio":                   "Ajaccio",
    "aj auxerre":                   "Auxerre",
    "as monaco":                    "AS Monaco",
    "as saint etienne":             "Saint-Étienne",
    "angers sco":                   "Angers",
    "clermont foot 63":             "Clermont Foot",
    "dijon fco":                    "Dijon",
    "estac troyes":                 "Troyes",
    "fc lorient":                   "Lorient",
    "fc metz":                      "Metz",
    "fc nantes":                    "Nantes",
    "girondins de burdeos":         "Bordeaux",
    "losc lille":                   "Lille",
    "le havre ac":                  "Le Havre",
    "montpellier hsc":              "Montpellier",
    "nimes olympique":              "Nîmes Olympique",
    "ogc niza":                     "Nice",
    "olympique de lyon":            "Olympique Lyonnais",
    "olympique de marsella":        "Olympique de Marseille",
    "paris saint germain fc":       "Paris Saint-Germain",
    "paris saint germain":          "Paris Saint-Germain",
    "racing club de estrasburgo":   "RC Strasbourg",
    "stade brestois 29":            "Stade Brestois",
    "stade rennais fc":             "Stade Rennais",
    "toulouse fc":                  "Toulouse",

    # WhoScored → SofaScore
    "brest":                        "Stade Brestois",
    "lens":                         "RC Lens",
    "lyon":                         "Olympique Lyonnais",
    "marseille":                    "Olympique de Marseille",
    "monaco":                       "AS Monaco",
    "nimes":                        "Nîmes Olympique",
    "psg":                          "Paris Saint-Germain",
    "reims":                        "Stade de Reims",
    "rennes":                       "Stade Rennais",
    "saint etienne":                "Saint-Étienne",
    "strasbourg":                   "RC Strasbourg",

    # Understat → SofaScore
    "ajaccio":                      "Ajaccio",
    "lille":                        "Lille",
    
    # ── Bundesliga ───────────────────────────────────────────────────────────────────

        # Transfermarkt → SofaScore
    "1 fc heidenheim 1846":         "1. FC Heidenheim",
    "1 fsv mainz 05":               "1. FSV Mainz 05",
    "fc augsburgo":                 "FC Augsburg",
    "fc colonia":                   "1. FC Köln",
    "hertha berlin":                "Hertha BSC",
    "sc friburgo":                  "SC Freiburg",
    "sv darmstadt 98":              "Darmstadt 98",
    "tsg 1899 hoffenheim":          "TSG Hoffenheim",
    "vfl bochum":                   "VfL Bochum 1848",

    # WhoScored → SofaScore
    "augsburg":                     "FC Augsburg",
    "bochum":                       "VfL Bochum 1848",
    "darmstadt":                    "Darmstadt 98",
    "fc heidenheim":                "1. FC Heidenheim",
    "fc koln":                      "1. FC Köln",
    "freiburg":                     "SC Freiburg",
    "greuther fuerth":              "SpVgg Greuther Fürth",
    "hoffenheim":                   "TSG Hoffenheim",
    "mainz":                        "1. FSV Mainz 05",
    "schalke":                      "FC Schalke 04",
    "st pauli":                     "FC St. Pauli",
    "stuttgart":                    "VfB Stuttgart",
    "union berlin":                 "1. FC Union Berlin",
    "werder bremen":                "SV Werder Bremen",
    "wolfsburg":                    "VfL Wolfsburg",

    # Understat → SofaScore
    "bayer leverkusen":             "Bayer 04 Leverkusen",
    "fc cologne":                   "1. FC Köln",
    "mainz 05":                     "1. FSV Mainz 05",
    "rasenballsport leipzig":       "RB Leipzig",
    "schalke 04":                   "FC Schalke 04",

    # ── Serie A ───────────────────────────────────────────────────────────────────

    # Transfermarkt → SofaScore
    "ac milan":                     "Milan",
    "ac monza":                     "Monza",
    "atalanta de bergamo":          "Atalanta",
    "bolonia":                      "Bologna",
    "como 1907":                    "Como",
    "empoli fc":                    "Empoli",
    "fc crotone":                   "Crotone",
    "frosinone calcio":             "Frosinone",
    "genova":                       "Genoa",
    "inter de milan":               "Inter",
    "juventus de turin":            "Juventus",
    "ss lazio":                     "Lazio",
    "ssc napoles":                  "SSC Napoli",
    "torino fc":                    "Torino",
    "uc sampdoria":                 "Sampdoria",
    "us cremonese":                 "Cremonese",
    "us lecce":                     "Lecce",
    "us salernitana 1919":          "Salernitana",
    "us sassuolo":                  "Sassuolo",
    "venezia fc":                   "Venezia",

    # Understat → SofaScore
    "ac milan":                     "Milan",
    "napoli":                       "SSC Napoli",
    "parma calcio 1913":            "Parma",
    "roma":                         "AS Roma",
    "verona":                       "Hellas Verona",


    # ── Eredivisie ────────────────────────────────────────────────────────────────

    # Transfermarkt → SofaScore
    "ajax de amsterdam":            "AFC Ajax",
    "excelsior rotterdam":          "Excelsior",
    "fc twente enschede":           "FC Twente",
    "go ahead eagles deventer":     "Go Ahead Eagles",
    "sc cambuur leeuwarden":        "SC Cambuur",
    "vitesse arnhem":               "Vitesse",

    # WhoScored → SofaScore
    "ajax":                         "AFC Ajax",
    "cambuur":                      "SC Cambuur",
    "heracles":                     "Heracles Almelo",
    "twente":                       "FC Twente",
    "willem ii":                    "Willem II Tilburg",


    # ── Primeira Liga ─────────────────────────────────────────────────────────────

    # Transfermarkt → SofaScore
    "avs futebol":                  "AVS - Futebol SAD",
    "b sad":                        "B-SAD",
    "boavista fc":                  "Boavista",
    "cd santa clara":               "Santa Clara",
    "cd tondela":                   "Tondela",
    "casa pia ac":                  "Casa Pia",
    "fc famalicao":                 "Famalicão",
    "fc oporto":                    "FC Porto",
    "fc pacos de ferreira":         "Paços de Ferreira",
    "fc vizela":                    "Vizela",
    "gd chaves":                    "Chaves",
    "gd estoril praia":             "Estoril Praia",
    "gil vicente fc":               "Gil Vicente",
    "moreirense fc":                "Moreirense",
    "rio ave fc":                   "Rio Ave",
    "sc braga":                     "Sporting Braga",
    "sl benfica":                   "Benfica",
    "sporting de lisboa":           "Sporting CP",
    "vitoria guimaraes sc":         "Vitória SC",

    # WhoScored → SofaScore
    "arouca":                       "FC Arouca",
    "braga":                        "Sporting Braga",
    "estoril":                      "Estoril Praia",
    "estrela da amadora":           "CF Estrela Amadora",
    "famalicao":                    "Famalicão",
    "farense":                      "SC Farense",
    "maritimo":                     "CS Marítimo",
    "nacional":                     "CD Nacional",
    "pacos de ferreira":            "Paços de Ferreira",
    "porto":                        "FC Porto",
    "portimonense":                 "Portimonense SAD",
    "sporting":                     "Sporting CP",
    "vitoria de guimaraes":         "Vitória SC",

    # ── Europa League ─────────────────────────────────────────────────────────────

    # Transfermarkt → SofaScore
    "aek atenas fc":                "AEK Athens",
    "bodo glimt":                   "Bodø/Glimt",
    "fk bodo glimt":                "Bodø/Glimt",
    "cfr cluj":                     "CFR 1907 Cluj",
    "cska moscu":                   "CSKA Moscow",
    "dynamo brest":                 "Dynamo Brest",
    "fc spartak de moscu":          "FC Spartak Moscow",
    "fc zurich":                    "FC Zürich",
    "fk tsc backa topola":          "FK TSC Bačka Topola",
    "hapoel beer sheva":            "Hapoel Be'er Sheva",
    "hjk helsinki":                 "HJK",
    "kaa gante":                    "KAA Gent",
    "krc genk":                     "KRC Genk",
    "lask":                         "LASK",
    "lech poznan":                  "Lech Poznań",
    "legia de varsovia":            "Legia Warszawa",
    "legia warsaw":                 "Legia Warszawa",
    "ludogorets razgrad":           "Ludogorets",
    "maccabi tel aviv":             "Maccabi Tel Aviv",
    "molde fk":                     "Molde FK",
    "omonia nicosia":               "Omonia Nicosia",
    "panathinaikos":                "Panathinaikos FC",
    "paok de salonica fc":          "PAOK",
    "qarabag fk":                   "Qarabağ FK",
    "qarabag":                      "Qarabağ FK",
    "rakow czestochowa":            "Raków Częstochowa",
    "rsc anderlecht":               "RSC Anderlecht",
    "royale union saint gilloise":  "Royale Union Saint-Gilloise",
    "servette fc":                  "Servette FC",
    "sivasspor":                    "Sivasspor",
    "sk rapid viena":               "SK Rapid Wien",
    "sk slavia praga":              "SK Slavia Praha",
    "sk sturm graz":                "SK Sturm Graz",
    "standard de lieja":            "Standard Liège",
    "trabzonspor":                  "Trabzonspor",
    "union saint gilloise":         "Royale Union Saint-Gilloise",
    "wolfsberger ac":               "Wolfsberger AC",
    "zorya lugansk":                "Zoria Luhansk",
    "zorya luhansk":                "Zoria Luhansk",
    "fc rfs":                       "RFS",

    # WhoScored → SofaScore
    "anderlecht":                   "RSC Anderlecht",
    "bodoe glimt":                  "Bodø/Glimt",
    "brann":                        "SK Brann",
    "cska moscow":                  "CSKA Moscow",
    "elfsborg":                     "IF Elfsborg",
    "fc fcsb":                      "FCSB",
    "fenerbahce":                   "Fenerbahçe",
    "ferencvaros":                  "Ferencváros TC",
    "genk":                         "KRC Genk",
    "ludogorets razgrad":           "Ludogorets",
    "maccabi tel aviv":             "Maccabi Tel Aviv",
    "molde":                        "Molde FK",
    "paok thessaloniki fc":         "PAOK",
    "panathinaikos":                "Panathinaikos FC",
    "rapid vienna":                 "SK Rapid Wien",
    "rfs":                          "RFS",
    "fk bodo glimt":                "Bodø/Glimt",  
    "slavia prague":                "SK Slavia Praha",
    "spartak moscow":               "FC Spartak Moscow",
    "union st gilloise":            "Royale Union Saint-Gilloise",
    "wolfsberger ac":               "Wolfsberger AC",
    "young boys":                   "BSC Young Boys",
    "zurich":                       "FC Zürich",

    "kryvbas":                                 "FC Kryvbas Kryvyi Rih",
    "llapi":                                   "KF Llapi",
    "panevezys":                               "FK Panevėžys",
    "zira":                                    "Zirə FK",
    "corvinul hunedoara":                      "FC Corvinul Hunedoara",
    "jagiellonia bialystok":                   "Jagiellonia Białystok",
    "paksi se":                                "Paksi FC",
    "tobol kostanay":                          "FC Tobol",
    "maccabi petah tikva":                     "Maccabi Petach Tikva",
    "wisla krakow":                            "Wisła Kraków",
    "anderlecht":                              "RSC Anderlecht",
    "elfsborg":                                "IF Elfsborg",
    "cukaricki":                               "FK Čukarički",
    "panathinaikos":                           "Panathinaikos FC",
    "lugano":                                  "FC Lugano",
    "haecken":                                 "BK Häcken",
    "dnipro 1":                                "SC Dnipro-1",
    "pyunik":                                  "Pyunik Yerevan",
    "slovacko":                                "1. FC Slovácko",
    "union st gilloise":                       "Royale Union Saint-Gilloise",
    "fc zuerich":                              "FC Zürich",
    "hearts":                                  "Heart of Midlothian",
    "austria wien":                            "FK Austria Wien",
    "silkeborg":                               "Silkeborg IF",
    "genk":                                    "KRC Genk",
    "spartak moscow":                          "FC Spartak Moscow",
    "broendby if":                             "Brøndby IF",
    "zorya":                                   "Zoria Luhansk",
    "riteriai":                                "FK Riteriai",
    "tsc backa topola":                        "FK TSC Bačka Topola",
    "ararat armenia":                          "FC Ararat-Armenia",
    "gjilani":                                 "SC Gjilani",
    "drita":                                   "FC Drita",
    "prishtina":                               "FC Prishtina",
    "mura":                                    "NŠ Mura",
    "petrocub hincesti":                       "FC Petrocub Hîncesti",
    "shkupi":                                  "KF Shkupi",
    "st joseph s":                             "St Joseph's FC",
    "iberia 1999":                             "FC Iberia 1999",
    "sirens":                                  "Sirens FC",
    "kauno zalgiris":                          "FK Kauno Žalgiris",
    "rigas fs":                                "RFS",
    "alashkert fc":                            "FC Alashkert",
    "kukesi":                                  "FK Kukësi",
    "sumqayit":                                "Sumqayıt FK",
    "dinamo auto":                             "FC Dinamo-Auto",
    "ludogorets razgrad":                      "Ludogorets",
    "iskra":                                   "FK Iskra Danilovgrad",
    "tre penne":                               "SP Tre Penne",
    "laci":                                    "KF Laçi",
    "sfintul gheorghe":                        "FC Sfintul Gheorghe",
    "bala":                                    "Bala Town",
    "lokomotiva":                              "NK Lokomotiva Zagreb",
    "tre fiori":                               "SP Tre Fiori",
    "fc astana":                               "Astana",
    "kaisar kyzylorda":                        "FC Kaysar",
    "barry":                                   "Barry Town",
    "shirak":                                  "FC Shirak Gyumri",
    "floriana":                                "Floriana FC",
    "fola esch":                               "CS Fola Esch",
    "puskas fc":                               "Puskás Akadémia",
    "desna":                                   "Desna Chernihiv",
    "shamakhi":                                "Şamaxı FK",
    "dac 1904 dunajska streda":                "DAC 1904",
    "nomme kalju fc":                          "Nõmme Kalju",
    "differdange 03":                          "Differdange FC 03",
    "zeta":                                    "FK Zeta Golubovci",
    "sutjeska":                                "FK Sutjeska Nikšić",
    "teuta durres":                            "KF Teuta Durrës",
    "shkendija":                               "KF Shkëndija",
    "renova":                                  "FK Renova",
    "sileks":                                  "FK Sileks Kratovo",
    "ordabasy shymkent":                       "FC Ordabasy",
    "ventspils":                               "FK Ventspils",
    "zalgiris vilnius":                        "FK Žalgiris",
    "suduva":                                  "FK Sūduva Marijampolė",
    "valletta":                                "Valletta FC",
    "cfr cluj":                                "CFR 1907 Cluj",
    "linfield":                                "Linfield FC",
    "coleraine":                               "Coleraine FC",
    "zeljeznicar":                             "FK Željezničar",
    "sarajevo":                                "FK Sarajevo",
    "borac banja luka":                        "FK Borac Banja Luka",
    "neftchi":                                 "Neftçi PFK",
    "hapoel beer sheva":                       "Hapoel Be'er Sheva",
    "tirana":                                  "KF Tirana",
    "ki klaksvik":                             "Klaksvíkar Ítróttarfelag",
    "glentoran":                               "Glentoran FC",
    "zrinjski mostar":                         "HŠK Zrinjski Mostar",
    "lokomotivi tbilisi":                      "FC Locomotive Tbilisi",
    "partizan belgrade":                       "FK Partizan",
    "vojvodina":                               "FK Vojvodina",
    "buducnost podgorica":                     "FK Budućnost Podgorica",
    "connah s quay":                           "Connah's Quay Nomads",
    "anorthosis":                              "Anorthosis Famagusta",
    "fci levadia":                             "FCI Levadia Tallinn",
    "aris":                                    "Aris Thessaloniki",
    "dundalk":                                 "Dundalk FC",
    "bohemians":                               "Bohemian FC",
    "derry":                                   "Derry City",
    "sporting charleroi":                      "RC Sporting Charleroi",
    "vaduz":                                   "FC Vaduz",
    "servette":                                "Servette FC",
    "st gallen":                               "FC St. Gallen 1879",
    "maribor":                                 "NK Maribor",
    "olimpija ljubljana":                      "NK Olimpija Ljubljana",
    "celje":                                   "NK Celje",
    "zilina":                                  "MŠK Žilina",
    "ruzomberok":                              "MFK Ružomberok",
    "rostov":                                  "FC Rostov",
    "honka":                                   "FC Honka",
    "inter":                                   "Inter Turku",
    "kups":                                    "Kuopion Palloseura",
    "slovan liberec":                          "FC Slovan Liberec",
    "slavia prague":                           "SK Slavia Praha",
    "jablonec":                                "FK Jablonec",
    "hartberg":                                "TSV Hartberg",
    "rapid wien":                              "SK Rapid Wien",
    "osijek":                                  "NK Osijek",
    "rijeka":                                  "HNK Rijeka",
    "hajduk split":                            "HNK Hajduk Split",
    "fehervar fc":                             "Videoton FC Fehérvár",
    "honved":                                  "Budapest Honvéd FC",
    "breidablik":                              "Breidablik Kópavogur",
    "fh hafnarfjordur":                        "FH Hafnarfjörður",
    "ifk gothenburg":                          "IFK Göteborg",
    "djurgarden":                              "Djurgårdens IF",
    "hammarby":                                "Hammarby IF",
    "sonderjyske":                             "Sønderjyske Fodbold",
    "viking":                                  "Viking FK",
    "rosenborg":                               "Rosenborg BK",
    "molde":                                   "Molde FK",


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



# aqui convendria añadir una funcion  que enriqueza el diccionario _TEAM_ALIASES  automaticamente 
# Ahora mismo, sin esta funcion, antes de hacer la carga hay que añadir manualmente sl diccionario el nombre del equipo en la fuente y el nombre de Sofascore. 
# se llamaria en el team_loader
# Yo lo he intentado, pero no se puede resolver automaticamente todos los nombres de equipos y siempre  hay unos pocos que hay que hacer manualmente 
# def enrich_team_aliases(ss_path: Path): 