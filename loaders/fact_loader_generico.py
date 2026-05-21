"""
loaders/fact_loader_generico.py
================================
DEPRECATED — usa `loaders.fact_loader.{load_shots,load_events,load_injuries}(conn, comp_name=...)`.

Shim retro-compatible. Acepta kwargs antiguos y los ignora.
"""

from __future__ import annotations

import logging
import warnings

from loaders.fact_loader import (
    load_shots    as _canonical_load_shots,
    load_events   as _canonical_load_events,
    load_injuries as _canonical_load_injuries,
)

log = logging.getLogger(__name__)


def _warn_legacy(name: str, kwargs: dict) -> None:
    if not kwargs:
        return
    warnings.warn(
        f"fact_loader_generico.{name}: kwargs {sorted(kwargs)} están deprecated; "
        f"los CSVs se descubren ahora vía data/clean/<comp>/<season>/<source>/.",
        DeprecationWarning,
        stacklevel=3,
    )


def load_shots(conn, comp_name: str | None = None, **_legacy) -> int:
    _warn_legacy("load_shots", _legacy)
    return _canonical_load_shots(conn, comp_name=comp_name)


def load_events(conn, comp_name: str | None = None, **_legacy) -> int:
    _warn_legacy("load_events", _legacy)
    return _canonical_load_events(conn, comp_name=comp_name)


def load_injuries(conn, comp_name: str | None = None, **_legacy) -> int:
    _warn_legacy("load_injuries", _legacy)
    return _canonical_load_injuries(conn, comp_name=comp_name)
