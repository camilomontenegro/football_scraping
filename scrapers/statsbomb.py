"""
scrapers/statsbomb.py
=====================
Wrapper sobre statsbombpy para acceder a StatsBomb Open Data (GitHub, libre, sin API key).

Funciones puras — sin acceso a DB.
statsbombpy ya está en requirements.txt.
"""
from __future__ import annotations

import logging

import pandas as pd
from statsbombpy import sb

log = logging.getLogger(__name__)

# StatsBomb Open Data no requiere credenciales
_CREDS = {"user": "", "passwd": ""}


# ─────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────

def list_competitions() -> pd.DataFrame:
    """Retorna DataFrame con todas las competiciones disponibles."""
    try:
        return sb.competitions(creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener competiciones: %s", exc)
        return pd.DataFrame()


def list_matches(competition_id: int, season_id: int) -> pd.DataFrame:
    """Retorna DataFrame con partidos de una competición/temporada."""
    try:
        return sb.matches(
            competition_id=competition_id,
            season_id=season_id,
            creds=_CREDS,
        )
    except Exception as exc:
        log.error("Error al obtener partidos (comp=%d, season=%d): %s",
                competition_id, season_id, exc)
        return pd.DataFrame()


def get_events(match_id: int) -> pd.DataFrame:
    """Retorna DataFrame con todos los eventos de un partido."""
    try:
        return sb.events(match_id=match_id, creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener eventos (match=%d): %s", match_id, exc)
        return pd.DataFrame()


def get_lineups(match_id: int) -> dict:
    """Retorna dict {team_name: [jugador, ...]} con los lineups."""
    try:
        return sb.lineups(match_id=match_id, creds=_CREDS)
    except Exception as exc:
        log.error("Error al obtener lineups (match=%d): %s", match_id, exc)
        return {}
