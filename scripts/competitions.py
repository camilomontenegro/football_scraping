"""Backwards-compatible shim — re-exports the wizard.competitions surface.

The canonical definition lives in `wizard/competitions.py`. Old callers like
`loaders/competition_loader.py` (`from scripts.competitions import COMPETITIONS`)
continue to resolve through this module.
"""
from wizard.competitions import *  # noqa: F401,F403
