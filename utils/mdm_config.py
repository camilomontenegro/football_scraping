"""
utils/mdm_config.py
====================
Mapeo de nombres de columnas de ID externo en cada tabla de dimensión,
alineado con el schema real de create_tables.sql.

Tablas de referencia:
    dim_team   → canonical_id (PK), id_sofascore, id_understat, id_statsbomb, id_whoscored, id_transfermarkt
    dim_player → canonical_id (PK), id_sofascore, id_understat, id_statsbomb, id_whoscored, id_transfermarkt
    dim_match  → match_id (PK),     id_sofascore, id_understat, id_statsbomb, id_whoscored, id_transfermarkt
"""

# Columna de ID externo en cada tabla dim por fuente y entidad
SOURCE_ID_FIELDS: dict[str, dict[str, str]] = {
    "sofascore": {
        "team":   "id_sofascore",
        "player": "id_sofascore",
        "match":  "id_sofascore",
    },
    "transfermarkt": {
        "team":   "id_transfermarkt",
        "player": "id_transfermarkt",
        "match":  "id_transfermarkt",
    },
    "understat": {
        "team":   "id_understat",
        "player": "id_understat",
        "match":  "id_understat",
    },
    "statsbomb": {
        "team":   "id_statsbomb",
        "player": "id_statsbomb",
        "match":  "id_statsbomb",
    },
    "whoscored": {
        "team":   "id_whoscored",
        "player": "id_whoscored",
        "match":  "id_whoscored",
    },
}

# PK de cada tabla dim
DIM_PK: dict[str, str] = {
    "team":   "canonical_id",
    "player": "canonical_id",
    "match":  "match_id",
}

# Tabla dim de cada entidad
DIM_TABLE: dict[str, str] = {
    "team":   "dim_team",
    "player": "dim_player",
    "match":  "dim_match",
}

# Campo de nombre canónico en cada tabla
DIM_NAME_FIELD: dict[str, str] = {
    "team":   "canonical_name",
    "player": "canonical_name",
}