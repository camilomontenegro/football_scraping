"""
loaders/player_loader_generico.py
==================================
DEPRECATED — usa `loaders.player_loader.load_players(conn, comp_name=...)`.

Shim retro-compatible. Acepta kwargs antiguos (`tm_path`, `ss_path`, …) y los
ignora, delegando al loader canónico que lee `data/clean/<comp>/<season>/<source>/players.csv`.
"""

from __future__ import annotations

import logging
import warnings

from loaders.player_loader import load_players as _canonical_load_players

log = logging.getLogger(__name__)


def load_players(conn, comp_name: str | None = None, **_legacy_paths) -> int:
    if _legacy_paths:
        warnings.warn(
            "player_loader_generico.load_players: kwargs *_path están deprecated; "
            "los CSVs se descubren ahora vía data/clean/<comp>/<season>/<source>/players.csv.",
            DeprecationWarning,
            stacklevel=2,
        )
    return _canonical_load_players(conn, comp_name=comp_name)
