"""
Diccionario de Competiciones
=============================
Unifica los IDs de todas las fuentes de datos para cada competición.
Permite que el sistema trabaje con cualquier liga de forma consistente.

Estructura:
    - name: Nombre oficial de la competición
    - country: País de la competición
    - sources: IDs específicos de cada fuente

Uso:
    from wizard.competitions import COMPETITIONS, get_competition

    laliga = get_competition("La Liga")
    tm_id  = laliga["sources"]["transfermarkt"]["league_code"]  # "ES1"
"""
from typing import Dict, Any, Optional

__all__ = [
    "COMPETITIONS",
    "WORKING_COMPETITIONS",
    "WORKING_COMPETITION_NAMES",
    "TRANSFERMARKT_COMPETITION_SLUGS",
    "get_competition",
    "get_competition_by_country",
    "get_source_ids",
    "get_source_config",
    "get_competition_slug_transfermarkt",
    "get_season_start_year",
    "get_available_seasons",
    "list_competitions",
    "get_all_sources",
    # Legacy aliases kept for backwards compat with old scripts.competitions consumers.
    "LALIGA",
    "LALIGA_IDS",
]

TRANSFERMARKT_COMPETITION_SLUGS: Dict[str, str] = {
    "La Liga": "laliga",
    "Segunda División": "laliga2",
    "Premier League": "premier-league",
    "Championship": "championship",
    "Bundesliga": "bundesliga",
    "Serie A": "serie-a",
    "Ligue 1": "ligue-1",
    "Primeira Liga": "liga-portugal",
    "Eredivisie": "eredivisie",
    "Champions League": "uefa-champions-league",
    "Europa League": "europa-league",
    "Europa Conference League": "uefa-europa-conference-league",
    "FIFA Club World Cup": "fifa-club-world-cup",
    "FIFA World Cup": "weltmeisterschaft",
    "European Championship": "europameisterschaft",
    "Copa America": "copa-america",
}

WORKING_COMPETITIONS: Dict[str, list[str]] = {
    "Ligas nacionales": [
        "La Liga",
        "Premier League",
        "Bundesliga",
        "Serie A",
        "Ligue 1",
        "Primeira Liga",
        "Eredivisie",
    ],
    "Torneos continentales": [
        "Champions League",
        "Europa League",
        "Europa Conference League",
        "European Championship",
        "Copa America",
    ],
    "Torneos intercontinentales": [
        "FIFA World Cup",
        "FIFA Club World Cup",
    ],
}

WORKING_COMPETITION_NAMES = {
    name
    for names in WORKING_COMPETITIONS.values()
    for name in names
}

# ═══════════════════════════════════════════════════════════════════════
# DICCIONARIO DE COMPETICIONES
# ═══════════════════════════════════════════════════════════════════════

COMPETITIONS: Dict[str, Dict[str, Any]] = {
    "La Liga": {
        "name": "LaLiga",
        "country": "Spain",
        "country_code": "ES",
        "sources": {
            "transfermarkt": {"league_code": "ES1", "name": "LaLiga"},
            "sofascore": {"tournament_id": 8, "name": "LaLiga"},
            "understat": {"league": "La_Liga", "name": "La Liga"},
            "statsbomb": {"competition_id": 11, "name": "La Liga"},
            "whoscored": {
                "region_id": 206,
                "tournament_id": 4,
                "name": "LaLiga",
                "slug": "espa%C3%B1a-laliga",
            },
        },
    },
    "Segunda División": {
        "name": "Segunda División",
        "country": "Spain",
        "country_code": "ES",
        "sources": {
            "transfermarkt": {"league_code": "ES2", "name": "LaLiga2"},
            "sofascore": {"tournament_id": 54, "name": "LaLiga 2"},
            "understat": {"league": None, "name": "La Liga"},
            "statsbomb": {"competition_id": None, "name": "Segunda División"},
            "whoscored": {"region_id": 206, "tournament_id": 72, "name": "Segunda División"},
        },
    },
    "Premier League": {
        "name": "Premier League",
        "country": "England",
        "country_code": "GB",
        "sources": {
            "transfermarkt": {"league_code": "GB1", "name": "Premier League"},
            "sofascore": {"tournament_id": 17, "name": "Premier League"},
            "understat": {"league": "EPL", "name": "Premier League"},
            "statsbomb": {"competition_id": 2, "name": "Premier League"},
            "whoscored": {
                "region_id": 252,
                "tournament_id": 2,
                "name": "Premier League",
                "slug": "inglaterra-premier-league",
            },
        },
    },
    "Championship": {
        "name": "Championship",
        "country": "England",
        "country_code": "GB",
        "sources": {
            "transfermarkt": {"league_code": "GB2", "name": "Championship"},
            "sofascore": {"tournament_id": 18, "name": "Championship"},
            "understat": {"league": None, "name": "Championship"},
            "statsbomb": {"competition_id": None, "name": "Championship"},
            "whoscored": {"region_id": 252, "tournament_id": 17, "name": "Championship"},
        },
    },
    "Bundesliga": {
        "name": "Bundesliga",
        "country": "Germany",
        "country_code": "DE",
        "sources": {
            "transfermarkt": {"league_code": "L1", "name": "Bundesliga"},
            "sofascore": {"tournament_id": 35, "name": "Bundesliga"},
            "understat": {"league": "Bundesliga", "name": "Bundesliga"},
            "statsbomb": {"competition_id": 3, "name": "Bundesliga"},
            "whoscored": {
                "region_id": 81,
                "tournament_id": 3,
                "name": "Bundesliga",
                "slug": "alemania-bundesliga",
            },
        },
    },
    "Serie A": {
        "name": "Serie A",
        "country": "Italy",
        "country_code": "IT",
        "sources": {
            "transfermarkt": {"league_code": "IT1", "name": "Serie A"},
            "sofascore": {"tournament_id": 23, "name": "Serie A"},
            "understat": {"league": "Serie_A", "name": "Serie A"},
            "statsbomb": {"competition_id": 4, "name": "Serie A"},
            "whoscored": {
                "region_id": 106,
                "tournament_id": 13,
                "name": "Serie A",
                "slug": "italia-serie-a",
            },
        },
    },
    "Ligue 1": {
        "name": "Ligue 1",
        "country": "France",
        "country_code": "FR",
        "sources": {
            "transfermarkt": {"league_code": "FR1", "name": "Ligue 1"},
            "sofascore": {"tournament_id": 34, "name": "Ligue 1"},
            "understat": {"league": "Ligue_1", "name": "Ligue 1"},
            "statsbomb": {"competition_id": 7, "name": "Ligue 1"},
            "whoscored": {
                "region_id": 74,
                "tournament_id": 11,
                "name": "Ligue 1",
                "slug": "francia-ligue-1",
            },
        },
    },
    "Primeira Liga": {
        "name": "Primeira Liga",
        "country": "Portugal",
        "country_code": "PT",
        "sources": {
            "transfermarkt": {"league_code": "PO1", "name": "Primeira Liga"},
            "sofascore": {"tournament_id": 238, "name": "Primeira Liga"},
            "understat": {"league": "Primeira_Liga", "name": "Primeira Liga"},
            "statsbomb": {"competition_id": None, "name": "Primeira Liga"},
            "whoscored": {"region_id": 178, "tournament_id": 187, "name": "Primeira Liga"},
        },
    },
    "Eredivisie": {
        "name": "Eredivisie",
        "country": "Netherlands",
        "country_code": "NL",
        "sources": {
            "transfermarkt": {"league_code": "NL1", "name": "Eredivisie"},
            "sofascore": {"tournament_id": 37, "name": "Eredivisie"},
            "understat": {"league": "Eredivisie", "name": "Eredivisie"},
            "statsbomb": {"competition_id": 8, "name": "Eredivisie"},
            "whoscored": {"region_id": 155, "tournament_id": 10, "name": "Eredivisie"},
        },
    },
    "Champions League": {
        "name": "UEFA Champions League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "CL", "name": "Champions League"},
            "sofascore": {"tournament_id": 7, "name": "Champions League"},
            "understat": {"league": "Champions_League", "name": "Champions League"},
            "statsbomb": {"competition_id": 16, "name": "Champions League"},
            "whoscored": {"region_id": 250, "tournament_id": 12, "name": "Champions League"},
        },
    },
    "Europa League": {
        "name": "UEFA Europa League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "EL", "name": "Europa League"},
            "sofascore": {"tournament_id": 679, "name": "Europa League"},
            "understat": {"league": "Europa_League", "name": "Europa League"},
            "statsbomb": {"competition_id": 17, "name": "Europa League"},
            "whoscored": {"region_id": 250, "tournament_id": 30, "name": "Europa League"},
        },
    },
    "Europa Conference League": {
        "name": "UEFA Europa Conference League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "ECL", "name": "Conference League"},
            "sofascore": {"tournament_id": 17015, "name": "Europa Conference League"},
            "understat": {"league": "Conference_League", "name": "Conference League"},
            "statsbomb": {"competition_id": 37, "name": "Europa Conference League"},
            "whoscored": {"region_id": 2, "tournament_id": 1504, "name": "Europa Conference League"},
        },
    },
    # ─── International / national-team competitions (WhoScored region 247) ───
    "FIFA World Cup": {
        "name": "FIFA World Cup",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "WM26", "name": "FIFA World Cup"},
            "sofascore": {"tournament_id": 16, "name": "FIFA World Cup"},
            "understat": {"league": None, "name": "FIFA World Cup"},
            "statsbomb": {"competition_id": 43, "name": "FIFA World Cup"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 36,
                "name": "FIFA World Cup",
                "slug": "internacional-fifa-world-cup",
                "season_format": "single",
            },
        },
    },
    "European Championship": {
        "name": "UEFA European Championship",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "EM", "name": "European Championship"},
            "sofascore": {"tournament_id": 1, "name": "European Championship"},
            "understat": {"league": None, "name": "European Championship"},
            "statsbomb": {"competition_id": 55, "name": "UEFA Euro"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 124,
                "name": "European Championship",
                "slug": "internacional-european-championship",
                "season_format": "single",
            },
        },
    },
    "Copa America": {
        "name": "Copa America",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "CAM", "name": "Copa America"},
            "sofascore": {"tournament_id": 133, "name": "Copa America"},
            "understat": {"league": None, "name": "Copa America"},
            "statsbomb": {"competition_id": None, "name": "Copa America"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 94,
                "name": "Copa America",
                "slug": "internacional-copa-america",
                "season_format": "single",
            },
        },
    },
    "Africa Cup of Nations": {
        "name": "Africa Cup of Nations",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "AFCN", "name": "Africa Cup of Nations"},
            "sofascore": {"tournament_id": 132, "name": "Africa Cup of Nations"},
            "understat": {"league": None, "name": "Africa Cup of Nations"},
            "statsbomb": {"competition_id": None, "name": "Africa Cup of Nations"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 104,
                "name": "Africa Cup of Nations",
                "slug": "internacional-africa-cup-of-nations",
                "season_format": "single",
            },
        },
    },
    "Asian Cup": {
        "name": "AFC Asian Cup",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "AC", "name": "Asian Cup"},
            "sofascore": {"tournament_id": 1437, "name": "Asian Cup"},
            "understat": {"league": None, "name": "Asian Cup"},
            "statsbomb": {"competition_id": None, "name": "Asian Cup"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 166,
                "name": "Asian Cup",
                "slug": "internacional-asian-cup",
                "season_format": "single",
            },
        },
    },
    "UEFA Women's EURO": {
        "name": "UEFA Women's EURO",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "EM-W", "name": "Women's EURO"},
            "sofascore": {"tournament_id": 1670, "name": "Women's EURO"},
            "understat": {"league": None, "name": "Women's EURO"},
            "statsbomb": {"competition_id": None, "name": "Women's EURO"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 775,
                "name": "UEFA Women's EURO",
                "slug": "internacional-uefa-women-s-euro",
                "season_format": "single",
            },
        },
    },
    "FIFA Women's World Cup": {
        "name": "FIFA Women's World Cup",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "WM-W", "name": "FIFA Women's World Cup"},
            "sofascore": {"tournament_id": 290, "name": "FIFA Women's World Cup"},
            "understat": {"league": None, "name": "FIFA Women's World Cup"},
            "statsbomb": {"competition_id": 72, "name": "FIFA Women's World Cup"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 738,
                "name": "FIFA Women's World Cup",
                "slug": "internacional-fifa-women-s-world-cup",
                "season_format": "single",
            },
        },
    },
    "UEFA Nations League A": {
        "name": "UEFA Nations League A",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "UNLA", "name": "UEFA Nations League A"},
            "sofascore": {"tournament_id": 7124, "name": "UEFA Nations League A"},
            "understat": {"league": None, "name": "UEFA Nations League A"},
            "statsbomb": {"competition_id": None, "name": "UEFA Nations League A"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 683,
                "name": "UEFA Nations League A",
                "slug": "internacional-uefa-nations-league-a",
                "season_format": "single",
            },
        },
    },
    "UEFA Nations League B": {
        "name": "UEFA Nations League B",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "UNLB", "name": "UEFA Nations League B"},
            "sofascore": {"tournament_id": 7125, "name": "UEFA Nations League B"},
            "understat": {"league": None, "name": "UEFA Nations League B"},
            "statsbomb": {"competition_id": None, "name": "UEFA Nations League B"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 684,
                "name": "UEFA Nations League B",
                "slug": "internacional-uefa-nations-league-b",
                "season_format": "single",
            },
        },
    },
    "UEFA Nations League C": {
        "name": "UEFA Nations League C",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "UNLC", "name": "UEFA Nations League C"},
            "sofascore": {"tournament_id": 7126, "name": "UEFA Nations League C"},
            "understat": {"league": None, "name": "UEFA Nations League C"},
            "statsbomb": {"competition_id": None, "name": "UEFA Nations League C"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 685,
                "name": "UEFA Nations League C",
                "slug": "internacional-uefa-nations-league-c",
                "season_format": "single",
            },
        },
    },
    "UEFA Nations League D": {
        "name": "UEFA Nations League D",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "UNLD", "name": "UEFA Nations League D"},
            "sofascore": {"tournament_id": 7127, "name": "UEFA Nations League D"},
            "understat": {"league": None, "name": "UEFA Nations League D"},
            "statsbomb": {"competition_id": None, "name": "UEFA Nations League D"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 686,
                "name": "UEFA Nations League D",
                "slug": "internacional-uefa-nations-league-d",
                "season_format": "single",
            },
        },
    },
    "FIFA Club World Cup": {
        "name": "FIFA Club World Cup",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "KLUB", "name": "FIFA Club World Cup"},
            "sofascore": {"tournament_id": 357, "name": "FIFA Club World Cup"},
            "understat": {"league": None, "name": "FIFA Club World Cup"},
            "statsbomb": {"competition_id": None, "name": "FIFA Club World Cup"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 67,
                "name": "FIFA Club World Cup",
                "slug": "internacional-fifa-club-world-cup",
                "season_format": "single",
            },
        },
    },
    "World Cup Qualification UEFA": {
        "name": "World Cup Qualification UEFA",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {"league_code": "WMQE", "name": "World Cup Qualification UEFA"},
            "sofascore": {"tournament_id": 27, "name": "World Cup Qualification UEFA"},
            "understat": {"league": None, "name": "World Cup Qualification UEFA"},
            "statsbomb": {"competition_id": None, "name": "World Cup Qualification UEFA"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 721,
                "name": "World Cup Qualification UEFA",
                "slug": "internacional-world-cup-qualification-uefa",
                "season_format": "single",
            },
        },
    },
    "World Cup Qualification CONMEBOL": {
        "name": "World Cup Qualification CONMEBOL",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "WMQS", "name": "World Cup Qualification CONMEBOL"},
            "sofascore": {"tournament_id": 295, "name": "World Cup Qualification CONMEBOL"},
            "understat": {"league": None, "name": "World Cup Qualification CONMEBOL"},
            "statsbomb": {"competition_id": None, "name": "World Cup Qualification CONMEBOL"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 719,
                "name": "World Cup Qualification CONMEBOL",
                "slug": "internacional-world-cup-qualification-conmebol",
                "season_format": "single",
            },
        },
    },
    "Int. Friendly": {
        "name": "International Friendly",
        "country": "International",
        "country_code": "WW",
        "sources": {
            "transfermarkt": {"league_code": "FS", "name": "Int. Friendly"},
            "sofascore": {"tournament_id": 25, "name": "International Friendly"},
            "understat": {"league": None, "name": "Int. Friendly"},
            "statsbomb": {"competition_id": None, "name": "Int. Friendly"},
            "whoscored": {
                "region_id": 247,
                "tournament_id": 27,
                "name": "Int. Friendly",
                "slug": "internacional-int-friendly",
                "season_format": "single",
            },
        },
    },
}


# ═══════════════════════════════════════════════════════════════════════
# FUNCIONES DE CONSULTA
# ═══════════════════════════════════════════════════════════════════════

def get_competition(name: str) -> Optional[Dict[str, Any]]:
    """Obtiene la configuración de una competición por nombre."""
    return COMPETITIONS.get(name)


def get_competition_by_country(country: str) -> list[Dict[str, Any]]:
    """Obtiene todas las competiciones de un país."""
    return [
        {**comp, "name": name}
        for name, comp in COMPETITIONS.items()
        if comp.get("country") == country
    ]


def get_source_ids(competition_name: str, source: str) -> Dict[str, Any]:
    """Obtiene los IDs de una fuente específica para una competición."""
    comp = get_competition(competition_name)
    if comp and source in comp.get("sources", {}):
        return comp["sources"][source]
    return {}


def get_source_config(competition_name: str, source: str) -> Dict[str, Any]:
    """Alias de get_source_ids para claridad semántica."""
    return get_source_ids(competition_name, source)


def get_competition_slug_transfermarkt(competition_name: str) -> Optional[str]:
    """Slug de Transfermarkt usado en URLs de participantes/plantillas."""
    if not competition_name:
        return None
    if competition_name in TRANSFERMARKT_COMPETITION_SLUGS:
        return TRANSFERMARKT_COMPETITION_SLUGS[competition_name]
    comp = get_competition(competition_name)
    name = (comp or {}).get("sources", {}).get("transfermarkt", {}).get("name")
    if not name:
        return None
    return name.lower().replace(" ", "-")


def get_season_start_year(season: str) -> int:
    """Extrae el año de inicio de una temporada en formato '2024/2025' o '24/25'."""
    if not season:
        return 2024
    part = season.split("/")[0].strip()
    try:
        year = int(part)
        if year < 100:
            year += 2000
        return year
    except ValueError:
        return 2024


def get_available_seasons(start_year: int = 2020, end_year: int = 2024) -> list[str]:
    """Genera la lista de temporadas desde start_year hasta end_year (inclusive)."""
    return [f"{y}/{y + 1}" for y in range(start_year, end_year + 1)]


def list_competitions() -> list[Dict[str, Any]]:
    """Lista las competiciones declaradas como activas en WORKING_COMPETITIONS."""
    return [
        {
            "name": name,
            "country": comp["country"],
            "country_code": comp.get("country_code"),
            "has_transfermarkt": "league_code" in comp.get("sources", {}).get("transfermarkt", {}),
            "has_sofascore": comp.get("sources", {}).get("sofascore", {}).get("tournament_id") is not None,
            "has_understat": bool(comp.get("sources", {}).get("understat", {}).get("league")),
            "has_statsbomb": comp.get("sources", {}).get("statsbomb", {}).get("competition_id") is not None,
        }
        for name, comp in COMPETITIONS.items()
        if name in WORKING_COMPETITION_NAMES
    ]


def get_all_sources() -> list[str]:
    """Lista todas las fuentes de datos disponibles."""
    return ["transfermarkt", "sofascore", "understat", "statsbomb", "whoscored"]


# ═══════════════════════════════════════════════════════════════════════
# COMPATIBILIDAD CON CÓDIGO LEGACY (scripts.competitions)
# ═══════════════════════════════════════════════════════════════════════

LALIGA = COMPETITIONS["La Liga"]
LALIGA_IDS = {
    "transfermarkt": LALIGA["sources"]["transfermarkt"]["league_code"],
    "sofascore": LALIGA["sources"]["sofascore"]["tournament_id"],
    "understat": LALIGA["sources"]["understat"]["league"],
    "statsbomb": LALIGA["sources"]["statsbomb"]["competition_id"],
}