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
    from scripts.competitions import COMPETITIONS, get_competition

    laliga = get_competition("La Liga")
    tm_id  = laliga["sources"]["transfermarkt"]["league_code"]  # "ES1"
"""
from typing import Dict, Any, Optional

# ═══════════════════════════════════════════════════════════════════════
# DICCIONARIO DE COMPETICIONES
# ═══════════════════════════════════════════════════════════════════════

COMPETITIONS: Dict[str, Dict[str, Any]] = {
    # ═══════════════════════════════════════════════════════════════════
    # ESPAÑA
    # ═══════════════════════════════════════════════════════════════════

    "La Liga": {
        "name": "LaLiga",
        "country": "Spain",
        "country_code": "ES",
        "sources": {
            "transfermarkt": {
                "league_code": "ES1",
                "name": "LaLiga",
            },
            "sofascore": {
                "tournament_id": 8,
                "name": "LaLiga",
            },
            "understat": {
                "league": "La_Liga",
                "name": "La Liga",
            },
            "statsbomb": {
                "competition_id": 11,
                "name": "La Liga",
            },
            "whoscored": {
                "region_id": 206,
                "tournament_id": 4,
                "name": "LaLiga",
            },
        },
    },

    "Segunda División": {
        "name": "Segunda División",
        "country": "Spain",
        "country_code": "ES",
        "sources": {
            "transfermarkt": {
                "league_code": "ES2",
                "name": "LaLiga2",
            },
            "sofascore": {
                "tournament_id": 39,
                "name": "LaLiga2",
            },
            "understat": {
                "league": None,
                "name": "La Liga",
            },
            "statsbomb": {
                "competition_id": None,
                "name": "Segunda División",
            },
            "whoscored": {
                "region_id": 206,
                "tournament_id": 72,
                "name": "Segunda División",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # INGLATERRA
    # ═══════════════════════════════════════════════════════════════════

    "Premier League": {
        "name": "Premier League",
        "country": "England",
        "country_code": "GB",
        "sources": {
            "transfermarkt": {
                "league_code": "GB1",
                "name": "Premier League",
            },
            "sofascore": {
                "tournament_id": 2,
                "name": "Premier League",
            },
            "understat": {
                "league": "EPL",
                "name": "Premier League",
            },
            "statsbomb": {
                "competition_id": 2,
                "name": "Premier League",
            },
            "whoscored": {
                "region_id": 252,
                "tournament_id": 2,
                "name": "Premier League",
            },
        },
    },

    "Championship": {
        "name": "Championship",
        "country": "England",
        "country_code": "GB",
        "sources": {
            "transfermarkt": {
                "league_code": "GB2",
                "name": "Championship",
            },
            "sofascore": {
                "tournament_id": 35,
                "name": "Championship",
            },
            "understat": {
                "league": None,
                "name": "Championship",
            },
            "statsbomb": {
                "competition_id": None,
                "name": "Championship",
            },
            "whoscored": {
                "region_id": 252,
                "tournament_id": 17,
                "name": "Championship",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # ALEMANIA
    # ═══════════════════════════════════════════════════════════════════

    "Bundesliga": {
        "name": "Bundesliga",
        "country": "Germany",
        "country_code": "DE",
        "sources": {
            "transfermarkt": {
                "league_code": "L1",
                "name": "Bundesliga",
            },
            "sofascore": {
                "tournament_id": 3,
                "name": "Bundesliga",
            },
            "understat": {
                "league": "Bundesliga",
                "name": "Bundesliga",
            },
            "statsbomb": {
                "competition_id": 3,
                "name": "Bundesliga",
            },
            "whoscored": {
                "region_id": 81,
                "tournament_id": 7,
                "name": "Bundesliga",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # ITALIA
    # ═══════════════════════════════════════════════════════════════════

    "Serie A": {
        "name": "Serie A",
        "country": "Italy",
        "country_code": "IT",
        "sources": {
            "transfermarkt": {
                "league_code": "IT1",
                "name": "Serie A",
            },
            "sofascore": {
                "tournament_id": 4,
                "name": "Serie A",
            },
            "understat": {
                "league": "Serie_A",
                "name": "Serie A",
            },
            "statsbomb": {
                "competition_id": 4,
                "name": "Serie A",
            },
            "whoscored": {
                "region_id": 106,
                "tournament_id": 13,
                "name": "Serie A",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # FRANCIA
    # ═══════════════════════════════════════════════════════════════════

    "Ligue 1": {
        "name": "Ligue 1",
        "country": "France",
        "country_code": "FR",
        "sources": {
            "transfermarkt": {
                "league_code": "FR1",
                "name": "Ligue 1",
            },
            "sofascore": {
                "tournament_id": 5,
                "name": "Ligue 1",
            },
            "understat": {
                "league": "Ligue_1",
                "name": "Ligue 1",
            },
            "statsbomb": {
                "competition_id": 7,
                "name": "Ligue 1",
            },
            "whoscored": {
                "region_id": 74,
                "tournament_id": 11,
                "name": "Ligue 1",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # PORTUGAL
    # ═══════════════════════════════════════════════════════════════════

    "Primeira Liga": {
        "name": "Primeira Liga",
        "country": "Portugal",
        "country_code": "PT",
        "sources": {
            "transfermarkt": {
                "league_code": "PO1",
                "name": "Primeira Liga",
            },
            "sofascore": {
                "tournament_id": 314,
                "name": "Primeira Liga",
            },
            "understat": {
                "league": "Primeira_Liga",
                "name": "Primeira Liga",
            },
            "statsbomb": {
                "competition_id": None,
                "name": "Primeira Liga",
            },
            "whoscored": {
                "region_id": 178,
                "tournament_id": 187,
                "name": "Primeira Liga",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # PAÍSES BAJOS
    # ═══════════════════════════════════════════════════════════════════

    "Eredivisie": {
        "name": "Eredivisie",
        "country": "Netherlands",
        "country_code": "NL",
        "sources": {
            "transfermarkt": {
                "league_code": "NL1",
                "name": "Eredivisie",
            },
            "sofascore": {
                "tournament_id": 9,
                "name": "Eredivisie",
            },
            "understat": {
                "league": "Eredivisie",
                "name": "Eredivisie",
            },
            "statsbomb": {
                "competition_id": 8,
                "name": "Eredivisie",
            },
            "whoscored": {
                "region_id": 155,
                "tournament_id": 10,
                "name": "Eredivisie",
            },
        },
    },

    # ═══════════════════════════════════════════════════════════════════
    # COMPETICIONES EUROPEAS
    # ═══════════════════════════════════════════════════════════════════

    "Champions League": {
        "name": "UEFA Champions League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {
                "league_code": "CL",
                "name": "Champions League",
            },
            "sofascore": {
                "tournament_id": 7,
                "name": "Champions League",
            },
            "understat": {
                "league": "Champions_League",
                "name": "Champions League",
            },
            "statsbomb": {
                "competition_id": 16,
                "name": "Champions League",
            },
            "whoscored": {
                "region_id": 250,
                "tournament_id": 12,
                "name": "Champions League",
            },
        },
    },

    "Europa League": {
        "name": "UEFA Europa League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {
                "league_code": "EL",
                "name": "Europa League",
            },
            "sofascore": {
                "tournament_id": 679,
                "name": "Europa League",
            },
            "understat": {
                "league": "Europa_League",
                "name": "Europa League",
            },
            "statsbomb": {
                "competition_id": 17,
                "name": "Europa League",
            },
            "whoscored": {
                "region_id": 250,
                "tournament_id": 30,
                "name": "Europa League",
            },
        },
    },

    "Europa Conference League": {
        "name": "UEFA Europa Conference League",
        "country": "Europe",
        "country_code": "EU",
        "sources": {
            "transfermarkt": {
                "league_code": "ECL",
                "name": "Conference League",
            },
            "sofascore": {
                "tournament_id": 2050,
                "name": "Europa Conference League",
            },
            "understat": {
                "league": "Conference_League",
                "name": "Conference League",
            },
            "statsbomb": {
                "competition_id": 37,
                "name": "Europa Conference League",
            },
            "whoscored": {
                "region_id": 2,
                "tournament_id": 1504,
                "name": "Europa Conference League",
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


def get_season_start_year(season: str) -> int:
    """Extrae el año de inicio de una temporada en formato '2024/2025' o '24/25'.

    Ejemplos:
        '2024/2025' -> 2024
        '24/25'     -> 2024
        '2024'      -> 2024
    """
    if not season:
        return 2024
    part = season.split("/")[0].strip()
    try:
        year = int(part)
        # Si viene en formato corto (p.ej. '24'), expandir a año completo
        if year < 100:
            year += 2000
        return year
    except ValueError:
        return 2024


def get_available_seasons(start_year: int = 2020, end_year: int = 2024) -> list[str]:
    """Genera la lista de temporadas desde start_year hasta end_year (inclusive).

    Ejemplo: get_available_seasons(2020, 2024) ->
        ['2020/2021', '2021/2022', '2022/2023', '2023/2024', '2024/2025']
    """
    return [f"{y}/{y + 1}" for y in range(start_year, end_year + 1)]


def list_competitions() -> list[Dict[str, Any]]:
    """Lista todas las competiciones disponibles."""
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
    ]


def get_all_sources() -> list[str]:
    """Lista todas las fuentes de datos disponibles."""
    return ["transfermarkt", "sofascore", "understat", "statsbomb", "whoscored"]