"""
transform/fact_events.py
========================
Consolida eventos de las tres fuentes en fact_events:
1. stg_sofascore_events   → source='sofascore'
2. stg_statsbomb_events   → source='statsbomb'
3. stg_whoscored_events   → source='whoscored'

Para cada evento:
- Resuelve player_id y team_id via MDM engine
-Resuelve match_id desde match_external_ids (si existe)
- Inserta con ON CONFLICT (match_id, external_id, data_source) DO NOTHING
"""
from __future__ import annotations

import logging

from sqlalchemy import text

from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# HELPERS (idénticos a fact_shots para consistencia)
# ─────────────────────────────────────────────────────

def _resolve_match_id(conn, source: str, ext_id: str) -> int | None:
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


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_decimal(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _insert_event(conn, match_id, player_id, team_id, event_type, minute, second, x, y, end_x, end_y, outcome, data_source, external_id):
    conn.execute(text("""
        INSERT INTO fact_events (
            match_id, player_id, team_id,
            event_type, minute, second,
            x, y, end_x, end_y,
            outcome, data_source, external_id
        )
        VALUES (
            :match_id, :player_id, :team_id,
            :event_type, :minute, :second,
            :x, :y, :end_x, :end_y,
            :outcome, :data_source, :external_id
        )
        ON CONFLICT (match_id, external_id, data_source)
        WHERE external_id IS NOT NULL
        DO NOTHING
    """), {
        "match_id":    match_id,
        "player_id":   player_id,
        "team_id":     team_id,
        "event_type":  event_type,
        "minute":      minute,
        "second":      second,
        "x":           x,
        "y":           y,
        "end_x":       end_x,
        "end_y":       end_y,
        "outcome":     outcome,
        "data_source": data_source,
        "external_id": external_id,
    })


# ─────────────────────────────────────────────────────
# SOFASCORE → fact_events
# ─────────────────────────────────────────────────────

def _load_sofascore_events(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, player_id_ext, incident_type,
            minute, player_name, team_name
        FROM stg_sofascore_events
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, player_ext, ev_type, minute, player_name, team_name = row

        match_id  = _resolve_match_id(conn, "sofascore", str(match_ext) if match_ext else None)
        player_id = _resolve_player(conn, player_name, "sofascore")
        team_id   = _resolve_team(conn, team_name, "sofascore")

        _insert_event(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            event_type=str(ev_type or ""),
            minute=_safe_int(minute),
            second=None,
            x=None, y=None, end_x=None, end_y=None,
            outcome=None,
            data_source="sofascore",
            external_id=str(stg_id),
        )
        inserted += 1

    log.info("fact_events ← sofascore: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# STATSBOMB → fact_events
# ─────────────────────────────────────────────────────

def _load_statsbomb_events(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, event_uuid, event_type,
            minute, second, player_name, team_name,
            raw_json
        FROM stg_statsbomb_events
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, event_uuid, ev_type, minute, second, player_name, team_name, raw_json = row

        match_id  = _resolve_match_id(conn, "statsbomb", str(match_ext) if match_ext else None)
        player_id = _resolve_player(conn, player_name, "statsbomb")
        team_id   = _resolve_team(conn, team_name, "statsbomb")

        # Coordenadas del raw_json
        x = y = end_x = end_y = outcome = None
        if isinstance(raw_json, dict):
            loc = raw_json.get("location", [])
            if loc and len(loc) >= 2:
                x, y = _safe_decimal(loc[0]), _safe_decimal(loc[1])

            # Coordenadas de destino (pass, shot, carry, etc.)
            for dest_key in ("pass", "shot", "carry"):
                dest = raw_json.get(dest_key, {}) or {}
                end_loc = dest.get("end_location", [])
                if end_loc and len(end_loc) >= 2:
                    end_x = _safe_decimal(end_loc[0])
                    end_y = _safe_decimal(end_loc[1])
                    break

            # Outcome
            out_obj = raw_json.get("shot", raw_json.get("pass", raw_json.get("duel", {}))) or {}
            out = out_obj.get("outcome")
            if isinstance(out, dict):
                outcome = out.get("name")

        _insert_event(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            event_type=str(ev_type or ""),
            minute=_safe_int(minute),
            second=_safe_int(second),
            x=x, y=y, end_x=end_x, end_y=end_y,
            outcome=outcome,
            data_source="statsbomb",
            external_id=str(event_uuid) if event_uuid else str(stg_id),
        )
        inserted += 1

    log.info("fact_events ← statsbomb: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# WHOSCORED → fact_events
# ─────────────────────────────────────────────────────

def _load_whoscored_events(conn) -> int:
    rows = conn.execute(text("""
        SELECT id, match_id_ext, event_id_ext, event_type,
               minute, player_name, team_name, x, y
        FROM stg_whoscored_events
    """)).fetchall()

    inserted = 0
    for row in rows:
        stg_id, match_ext, event_ext, ev_type, minute, player_name, team_name, x_raw, y_raw = row

        match_id  = _resolve_match_id(conn, "whoscored", str(match_ext) if match_ext else None)
        player_id = _resolve_player(conn, player_name, "whoscored")
        team_id   = _resolve_team(conn, team_name, "whoscored")

        _insert_event(
            conn,
            match_id=match_id,
            player_id=player_id,
            team_id=team_id,
            event_type=str(ev_type or ""),
            minute=_safe_int(minute),
            second=None,
            x=_safe_decimal(x_raw), y=_safe_decimal(y_raw),
            end_x=None, end_y=None,
            outcome=None,
            data_source="whoscored",
            external_id=str(event_ext) if event_ext is not None else str(stg_id),
        )
        inserted += 1

    log.info("fact_events ← whoscored: %d", inserted)
    return inserted


# ─────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────

def load_fact_events(conn) -> dict:
    """
    Consolida eventos de las tres fuentes en fact_events.
    Retorna dict con conteos por fuente.
    """
    log.info("Iniciando carga de fact_events...")

    results = {
        "sofascore": _load_sofascore_events(conn),
        "statsbomb": _load_statsbomb_events(conn),
        "whoscored": _load_whoscored_events(conn),
    }

    total = sum(results.values())
    log.info("fact_events TOTAL: %d (ss=%d, sb=%d, ws=%d)",
            total, results["sofascore"], results["statsbomb"], results["whoscored"])
    return results
