"""
loaders/team_loader_generico.py
================================
DEPRECATED — usa `loaders.team_loader.load_teams(conn, comp_name=...)`.

Este módulo se mantiene como shim de compatibilidad. Acepta los kwargs antiguos
(`ss_path`, `tm_path`, …) pero los ignora y delega en el loader canónico, que
descubre los CSVs vía `utils.data_paths.iter_clean_csvs` bajo
`data/clean/<comp>/<season>/<source>/teams.csv`.
"""

from __future__ import annotations

import logging
import warnings

from loaders.team_loader import load_teams as _canonical_load_teams

log = logging.getLogger(__name__)


def load_teams(conn, comp_name: str | None = None, **_legacy_paths) -> int:
    """Shim retro-compatible. Los kwargs `*_path` se ignoran."""
    if _legacy_paths:
        warnings.warn(
            "team_loader_generico.load_teams: kwargs *_path están deprecated; "
            "los CSVs se descubren ahora vía data/clean/<comp>/<season>/<source>/teams.csv. "
            "Pasa `comp_name` para filtrar por competición.",
            DeprecationWarning,
            stacklevel=2,
        )
    return _canonical_load_teams(conn, comp_name=comp_name)
