"""
transform/fact_shots.py
=======================
Consolida tiros de las tres fuentes en fact_shots:
1. stg_sofascore_shots   → source='sofascore'
2. stg_understat_shots   → source='understat'
3. stg_statsbomb_events  → source='statsbomb'  (solo event_type = 'Shot')

Para cada tiro:
- Resuelve player_id y team_id via MDM engine
- Resuelve match_id desde match_external_ids (si existe)
- Inserta con ON CONFLICT (match_id, external_id, data_source) DO NOTHING
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def _resolve_match_id(conn, source: str, ext_id: str) -> int | None:
    """Busca match_id en match_external_ids dado source + external_id."""
    if not ext_id:
        return None
    result = conn.execute(text("""
        SELECT match_id FROM match_external_ids
        WHERE source = :source AND external_id = :ext_id
        LIMIT 1
    """), {"source": source, "ext_id": str(ext_id)}).fetchone()
    return result[0] if result else None


def _resolve_player(conn, name: str | None, source: str) -> int | None:
    if not name:
        return None
    res = resolve(conn, "player", name, source)
    return get_entity_id(res)


def _resolve_team(conn, name: str | None, source: str) -> int | None:
    if not name:
        return None
    res = resolve(conn, "team", name, source)
    return get_entity_id(res)


def _safe_decimal(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _insert_shot(conn, match_id, player_id, team_id, minute, x, y, xg, result, shot_type, situation, data_source, external_id):
    conn.execute(text("""
        INSERT INTO fact_shots (
            match_id, player_id, team_id,
            minute, x, y, xg,
            result, shot_type, situation,
            data_source, external_id
        )
        VALUES (
            :match_id, :player_id, :team_id,
            :minute, :x, :y, :xg,
            :result, :shot_type, :situation,
            :data_source, :external_id
        )
        ON CONFLICT (match_id, external_id, data_source)
        WHERE external_id IS NOT NULL
        DO NOTHING
    """), {
        "match_id":    match_id,
        "player_id":   player_id,
        "team_id":     team_id,
        "minute":      minute,
        "x":           x,
        "y":           y,
        "xg":          xg,
        "result":      result,
        "shot_type":   shot_type,
        "situation":   situation,
        "data_source": data_source,
        "external_id": external_id,
    })


# ─────────────────────────────────────────────────────
# SOFASCORE → fact_shots
# ─────────────────────────────────────────────────────

def _load_sofascore_shots(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, player_name, team_name,
            minute, x, y, xg, result, shot_type
        FROM stg_sofascore_shots
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, player_name, team_name, minute, x, y, xg, result, shot_type = row

        match_id  = _resolve_match_id(conn, "sofascore", str(match_ext) if match_ext else None)
        player_id = _resolve_player(conn, player_name, "sofascore")
        team_id   = _resolve_team(conn, team_name, "sofascore")

        _insert_shot(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            minute=_safe_int(minute),
            x=_safe_decimal(x),
            y=_safe_decimal(y),
            xg=_safe_decimal(xg),
            result=result,
            shot_type=shot_type,
            situation=None,
            data_source="sofascore",
            external_id=str(stg_id),
        )
        inserted += 1

    log.info("fact_shots ← sofascore: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# UNDERSTAT → fact_shots
# ─────────────────────────────────────────────────────

def _load_understat_shots(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, player_name, team_name,
            minute, x, y, xg, result, shot_type, situation
        FROM stg_understat_shots
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, player_name, team_name, minute, x, y, xg, result, shot_type, situation = row

        match_id  = _resolve_match_id(conn, "understat", match_ext)
        player_id = _resolve_player(conn, player_name, "understat")
        team_id   = _resolve_team(conn, team_name, "understat")

        _insert_shot(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            minute=_safe_int(minute),
            x=_safe_decimal(x),
            y=_safe_decimal(y),
            xg=_safe_decimal(xg),
            result=result,
            shot_type=shot_type,
            situation=situation,
            data_source="understat",
            external_id=str(stg_id),
        )
        inserted += 1

    log.info("fact_shots ← understat: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# STATSBOMB → fact_shots (solo eventos tipo 'Shot')
# ─────────────────────────────────────────────────────

def _load_statsbomb_shots(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, event_uuid,
            player_name, team_name, minute,
            raw_json
        FROM stg_statsbomb_events
        WHERE LOWER(event_type) = 'shot'
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, event_uuid, player_name, team_name, minute, raw_json = row

        match_id  = _resolve_match_id(conn, "statsbomb", str(match_ext) if match_ext else None)
        player_id = _resolve_player(conn, player_name, "statsbomb")
        team_id   = _resolve_team(conn, team_name, "statsbomb")

        # Extraer campos del raw_json si están disponibles
        shot_detail = (raw_json or {}).get("shot", {}) if isinstance(raw_json, dict) else {}
        xg       = _safe_decimal(shot_detail.get("statsbomb_xg"))
        result   = (shot_detail.get("outcome") or {}).get("name") if isinstance(shot_detail.get("outcome"), dict) else None
        shot_type = (shot_detail.get("body_part") or {}).get("name") if isinstance(shot_detail.get("body_part"), dict) else None

        loc = raw_json.get("location", []) if isinstance(raw_json, dict) else []
        x = _safe_decimal(loc[0]) if len(loc) > 0 else None
        y = _safe_decimal(loc[1]) if len(loc) > 1 else None

        _insert_shot(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            minute=_safe_int(minute),
            x=x,
            y=y,
            xg=xg,
            result=result,
            shot_type=shot_type,
            situation=None,
            data_source="statsbomb",
            external_id=str(event_uuid) if event_uuid else str(stg_id),
        )
        inserted += 1

    log.info("fact_shots ← statsbomb: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────

def load_fact_shots(conn) -> dict:
    log.info("Iniciando carga de fact_shots...")

    results = {
        "sofascore":  _load_sofascore_shots(conn),
        "understat":  _load_understat_shots(conn),
        "statsbomb":  _load_statsbomb_shots(conn),
    }

    total = sum(results.values())
    log.info("fact_shots TOTAL: %d (ss=%d, us=%d, sb=%d)",
            total, results["sofascore"], results["understat"], results["statsbomb"])
    return results
