"""
loaders/match_loader_generico.py
=================================
DEPRECATED — usa `loaders.match_loader.load_matches(conn, comp_name=...)`.

Shim retro-compatible.
"""

from __future__ import annotations

import logging
import warnings

from loaders.match_loader import load_matches as _canonical_load_matches

log = logging.getLogger(__name__)


def load_matches(conn, comp_name: str | None = None,
                 competition_id: int | None = None,
                 **_legacy_paths) -> int:
    if _legacy_paths or competition_id is not None:
        warnings.warn(
            "match_loader_generico.load_matches: kwargs *_path y competition_id "
            "están deprecated. Los CSVs se descubren ahora vía "
            "data/clean/<comp>/<season>/<source>/matches.csv.",
            DeprecationWarning,
            stacklevel=2,
        )
    return _canonical_load_matches(conn, comp_name=comp_name)
