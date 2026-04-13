"""Multi-signal player entity resolution engine.

Load order contract
-------------------
StatsBomb must seed ``dim_player`` before any other scraper calls
``resolve_player``.  Running a non-StatsBomb scraper against an empty
``dim_player`` is safe but will produce all new-player inserts.

Scoring weights
---------------
- Name similarity (thefuzz token_sort_ratio): 40 pts
- Birth date match (exact=35, ±1 year=15, absent/mismatch=0): 35 pts
- Nationality match (exact): 15 pts
- Position match (exact): 10 pts
Total: 100 pts

Resolution thresholds
---------------------
≥ 85  → auto-match: update source ID column on existing dim_player row
60–84 → review queue: insert into player_review, return None
< 60  → new player: insert into dim_player, return new canonical_id
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from thefuzz import fuzz

from db.models import DimPlayer, PlayerReview


def _name_score(a: dict, b: dict) -> int:
    name_a = a.get("canonical_name") or a.get("name") or ""
    name_b = b.get("canonical_name") or b.get("name") or ""
    if not name_a or not name_b:
        return 0
    ratio = fuzz.token_sort_ratio(name_a.lower(), name_b.lower())
    return round(ratio * 40 / 100)


def _date_score(a: dict, b: dict) -> int:
    da: Optional[date] = a.get("birth_date")
    db_: Optional[date] = b.get("birth_date")
    if not da or not db_:
        return 0
    if isinstance(da, str):
        try:
            da = date.fromisoformat(da[:10])
        except ValueError:
            return 0
    if isinstance(db_, str):
        try:
            db_ = date.fromisoformat(db_[:10])
        except ValueError:
            return 0
    if da == db_:
        return 35
    if abs(da.year - db_.year) <= 1:
        return 15
    return 0


def _nationality_score(a: dict, b: dict) -> int:
    na = (a.get("nationality") or "").strip().lower()
    nb = (b.get("nationality") or "").strip().lower()
    if na and nb and na == nb:
        return 15
    return 0


def _position_score(a: dict, b: dict) -> int:
    pa = (a.get("position") or "").strip().lower()
    pb = (b.get("position") or "").strip().lower()
    if pa and pb and pa == pb:
        return 10
    return 0


def calculate_match_score(player_a: Dict[str, Any], player_b: Dict[str, Any]) -> int:
    """Return a 0–100 integer match score between two player dicts.

    Each dict may contain: ``name``/``canonical_name``, ``birth_date``,
    ``nationality``, ``position``.
    """
    return (
        _name_score(player_a, player_b)
        + _date_score(player_a, player_b)
        + _nationality_score(player_a, player_b)
        + _position_score(player_a, player_b)
    )


def _dim_player_to_dict(row: DimPlayer) -> Dict[str, Any]:
    return {
        "canonical_name": row.canonical_name,
        "birth_date": row.birth_date,
        "nationality": row.nationality,
        "position": row.position,
    }


def resolve_player(
    source_player: Dict[str, Any],
    session,
    source_id_column: str,
) -> Optional[int]:
    """Resolve *source_player* against ``dim_player`` and return a ``canonical_id``.

    Args:
        source_player: Dict with keys ``name``/``canonical_name``, ``birth_date``,
            ``nationality``, ``position``, and ``source_id`` (the ID in the source
            system) plus ``source_system`` (e.g. ``'understat'``).
        session: An active SQLAlchemy session.
        source_id_column: Column name on ``DimPlayer`` to update on auto-match
            (e.g. ``'id_understat'``).

    Returns:
        ``canonical_id`` int on auto-match or new insert; ``None`` when the player
        is placed in the review queue.
    """
    candidates = session.query(DimPlayer).all()

    best_score = -1
    best_candidate: Optional[DimPlayer] = None

    source_dict = {
        "canonical_name": source_player.get("name") or source_player.get("canonical_name", ""),
        "birth_date": source_player.get("birth_date"),
        "nationality": source_player.get("nationality"),
        "position": source_player.get("position"),
    }

    for candidate in candidates:
        score = calculate_match_score(source_dict, _dim_player_to_dict(candidate))
        if score > best_score:
            best_score = score
            best_candidate = candidate

    # --- Auto-match ---
    if best_score >= 85 and best_candidate is not None:
        setattr(best_candidate, source_id_column, source_player.get("source_id"))
        session.flush()
        return best_candidate.canonical_id

    # --- Review queue ---
    if best_score >= 60 and best_candidate is not None:
        review = PlayerReview(
            source_name=source_dict["canonical_name"],
            source_system=source_player.get("source_system"),
            source_id=str(source_player.get("source_id")),
            suggested_canonical_id=best_candidate.canonical_id,
            similarity_score=best_score,
            resolved=False,
        )
        session.add(review)
        session.flush()
        return None

    # --- New player ---
    new_player = DimPlayer(
        canonical_name=source_dict["canonical_name"],
        birth_date=source_dict["birth_date"],
        nationality=source_dict["nationality"],
        position=source_dict["position"],
    )
    setattr(new_player, source_id_column, source_player.get("source_id"))
    session.add(new_player)
    session.flush()
    return new_player.canonical_id
