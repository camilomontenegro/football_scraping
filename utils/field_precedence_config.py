"""
utils/field_precedence_config.py
==================================
Define qué fuente es autoritativa para cada campo de cada entidad.

Jerarquía:
    - Jugadores: Transfermarkt es master (nombres, fechas de nacimiento, posición)
    - Equipos:   SofaScore es master (nombres canónicos, IDs)
    - Internacional: SofaScore como fuente complementaria para campos que TM no tiene
"""

FIELD_PRECEDENCE: dict[str, dict[str, list[str]]] = {
    "player": {
        # Transfermarkt es la fuente de verdad para datos biográficos de jugadores
        "canonical_name": ["transfermarkt"],
        "nationality":    ["transfermarkt", "sofascore", "statsbomb"],
        "birth_date":     ["transfermarkt", "sofascore", "statsbomb"],
        "position":       ["transfermarkt", "sofascore", "statsbomb"],
    },
    "team": {
        # SofaScore es la fuente de verdad para nombres de equipos
        "canonical_name": ["sofascore"],
        "country":        ["transfermarkt", "sofascore", "statsbomb"],
    },
    "match": {
        # SofaScore es la fuente de verdad para partidos
        "match_date":     ["sofascore", "understat", "statsbomb"],
        "home_score":     ["sofascore", "understat", "statsbomb"],
        "away_score":     ["sofascore", "understat", "statsbomb"],
        "competition":    ["sofascore", "statsbomb"],
    }
}