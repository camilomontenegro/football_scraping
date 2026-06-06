"""
dashboard/scanner.py
====================
Per-source detection of seasons available remotely but not yet present in dim_match.

Pipeline constants are defined here directly to avoid importing pipeline_runner.py,
which carries heavy staging/DB dependencies that are not needed in the dashboard.
"""
from __future__ import annotations

import logging
import traceback

import requests

# ── Pipeline constants (mirrors pipeline_runner.py) ──────────────────────────
STATSBOMB_COMPETITION_ID = 11
STATSBOMB_SEASON_ID      = 90
UNDERSTAT_LEAGUE         = "La_Liga"
UNDERSTAT_SEASON         = "2020"
SOFASCORE_TOURNAMENT_ID  = 8
SOFASCORE_SEASON_NAME    = "20/21"
SEASON_LABEL             = "2020/2021"

from dashboard.db import get_seasons_in_db

log = logging.getLogger(__name__)

_LA_LIGA = "La Liga"
_UA = {"User-Agent": "Mozilla/5.0"}
_TIMEOUT_S = 10


def _understat_season_to_label(season: str) -> str:
    """e.g. '2020' -> '2020/2021'."""
    y = int(season)
    return f"{y}/{y + 1}"


def _sofascore_season_to_label(name: str) -> str:
    """e.g. '20/21' -> '2020/2021'. Best-effort; returns input on failure."""
    try:
        a, b = name.split("/")
        a, b = int(a), int(b)
        return f"{2000 + a}/{2000 + b}"
    except Exception:
        return name


def scan_statsbomb() -> list[dict]:
    """
    Best-effort: ask statsbombpy what's available for the configured competition,
    and report seasons that aren't already in the DB.
    """
    try:
        from statsbombpy import sb
    except ImportError:
        return []

    seasons_in_db = {s for (_, s) in get_seasons_in_db()}
    comps = sb.competitions()  # pandas DataFrame

    rows = comps[comps["competition_id"] == STATSBOMB_COMPETITION_ID]
    out: list[dict] = []
    for _, r in rows.iterrows():
        season_label = str(r.get("season_name", ""))
        if season_label and season_label not in seasons_in_db:
            out.append({
                "source": "statsbomb",
                "competition": str(r.get("competition_name", _LA_LIGA)),
                "season": season_label,
                "competition_id": int(r["competition_id"]),
                "season_id": int(r["season_id"]),
            })
    return out


def scan_understat() -> list[dict]:
    """
    The pipeline today pins exactly one Understat league/season. Report it
    if the corresponding label is not already in the DB.
    """
    season_label = _understat_season_to_label(UNDERSTAT_SEASON)
    seasons_in_db = {s for (_, s) in get_seasons_in_db()}
    if season_label in seasons_in_db:
        return []
    return [{
        "source": "understat",
        "competition": _LA_LIGA,
        "season": season_label,
        "league": UNDERSTAT_LEAGUE,
    }]


def scan_sofascore() -> list[dict]:
    """
    Hit the SofaScore seasons endpoint for the pinned tournament and report
    seasons that aren't already in the DB. Network errors are contained.
    """
    url = (
        f"https://api.sofascore.com/api/v1/unique-tournament/"
        f"{SOFASCORE_TOURNAMENT_ID}/seasons"
    )
    try:
        resp = requests.get(url, headers=_UA, timeout=_TIMEOUT_S)
        resp.raise_for_status()
        seasons = resp.json().get("seasons", [])
    except Exception as exc:
        log.warning("sofascore scanner failed: %s", exc)
        return []

    seasons_in_db = {s for (_, s) in get_seasons_in_db()}
    out: list[dict] = []
    for s in seasons:
        name = str(s.get("name") or s.get("year") or "")
        season_label = _sofascore_season_to_label(name)
        if season_label and season_label not in seasons_in_db:
            out.append({
                "source": "sofascore",
                "competition": _LA_LIGA,
                "season": season_label,
                "season_id": int(s.get("id", 0)),
                "tournament_id": SOFASCORE_TOURNAMENT_ID,
            })
    return out


def scan_transfermarkt() -> list[dict]:
    return []


def scan_whoscored() -> list[dict]:
    return []


def scan_all() -> dict:
    """
    Invoke all five scanners with per-source fault isolation.
    """
    result: dict = {
        "statsbomb": [], "understat": [], "sofascore": [],
        "transfermarkt": [], "whoscored": [],
        "_errors": {},
    }
    for name, fn in [
        ("statsbomb",     scan_statsbomb),
        ("understat",     scan_understat),
        ("sofascore",     scan_sofascore),
        ("transfermarkt", scan_transfermarkt),
        ("whoscored",     scan_whoscored),
    ]:
        try:
            result[name] = fn() or []
        except Exception as exc:
            log.error("scanner %s raised: %s", name, exc)
            log.debug(traceback.format_exc())
            result[name] = []
            result["_errors"][name] = f"{type(exc).__name__}: {exc}"
    return result


# Anchor the SEASON_LABEL import so it isn't a dead import (used by callers
# that want the canonical pinned season string).
PINNED_SEASON_LABEL = SEASON_LABEL
