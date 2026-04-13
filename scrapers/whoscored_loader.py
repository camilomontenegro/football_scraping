"""WhoScored loader — fetches match event data via the soccerdata library.

soccerdata handles Cloudflare bypass and Selenium session management internally.
Events are written into the existing fact_events table using the shared ORM models.

Usage:
    # Load specific matches
    python -m scrapers.whoscored_loader --league "ENG-Premier League" --season 2021 --match-ids 1485184 1485185

    # Load all matches in a league/season
    python -m scrapers.whoscored_loader --league "ENG-Premier League" --season 2021
"""
from __future__ import annotations

import argparse
import logging
from typing import List, Optional

import soccerdata

from db.models import Base, FactEvents, get_engine, session_scope
from utils.helpers import normalize_coords
from utils.player_matcher import resolve_player

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_matches(league: str, season: str, match_ids: Optional[List[int]] = None) -> None:
    engine = get_engine()
    Base.metadata.create_all(engine)

    ws = soccerdata.WhoScored(leagues=league, seasons=season)

    log.info("Fetching events — league=%s season=%s match_ids=%s", league, season, match_ids)
    try:
        df = ws.read_events(
            match_id=match_ids if match_ids else None,
            output_fmt="events",
            on_error="skip",
        )
    except Exception as exc:
        log.error("Failed to fetch events: %s", exc)
        return

    if df is None or df.empty:
        log.warning("No event data returned.")
        return

    log.info("Loaded %d events total", len(df))

    with session_scope() as session:
        # Build player cache: resolve each unique (player_id, player_name) once
        player_cache: dict = {}
        for ws_pid, player_name in df[["player_id", "player"]].drop_duplicates().itertuples(index=False):
            if ws_pid not in player_cache and not _is_nan(ws_pid):
                source_player = {
                    "name": str(player_name) if player_name and not _is_nan(player_name) else None,
                    "birth_date": None,
                    "nationality": None,
                    "position": None,
                    "source_id": int(ws_pid),
                    "source_system": "whoscored",
                }
                player_cache[ws_pid] = resolve_player(source_player, session, "id_whoscored")

        for row in df.itertuples(index=False):
            raw_x = getattr(row, "x", None)
            raw_y = getattr(row, "y", None)
            x_m = y_m = None
            if raw_x is not None and not _is_nan(raw_x) and raw_y is not None and not _is_nan(raw_y):
                x_m, y_m = normalize_coords(float(raw_x), float(raw_y), "whoscored")

            end_x = getattr(row, "end_x", None)
            end_y = getattr(row, "end_y", None)
            end_x_m = end_y_m = None
            if end_x is not None and not _is_nan(end_x) and end_y is not None and not _is_nan(end_y):
                end_x_m, end_y_m = normalize_coords(float(end_x), float(end_y), "whoscored")

            ws_pid = getattr(row, "player_id", None)
            canonical_id = player_cache.get(ws_pid) if ws_pid and not _is_nan(ws_pid) else None

            session.add(FactEvents(
                match_id=None,
                player_id=canonical_id,
                event_type=str(getattr(row, "type", "") or ""),
                minute=_int_or_none(getattr(row, "minute", None)),
                second=_int_or_none(getattr(row, "second", None)),
                x=x_m,
                y=y_m,
                end_x=end_x_m,
                end_y=end_y_m,
                outcome=str(getattr(row, "outcome_type", "") or "") or None,
                source="whoscored",
            ))

    log.info("WhoScored loader complete.")


def _is_nan(val) -> bool:
    try:
        import math
        return math.isnan(val)
    except (TypeError, ValueError):
        return False


def _int_or_none(val) -> int | None:
    if val is None or _is_nan(val):
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load WhoScored match events via soccerdata")
    parser.add_argument("--league", required=True, help='League ID, e.g. "ENG-Premier League"')
    parser.add_argument("--season", required=True, help="Season, e.g. 2021 or 20-21")
    parser.add_argument("--match-ids", nargs="+", type=int, help="Optional: specific WhoScored match IDs to load")
    args = parser.parse_args()
    load_matches(args.league, args.season, args.match_ids)
