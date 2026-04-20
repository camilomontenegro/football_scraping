"""
staging/load_whoscored.py
=========================
Carga eventos de WhoScored (JSON matchCentreData del raw layer)
en stg_whoscored_events.

matchCentreData contiene:
- events: [...] con los eventos del partido
- playerIdNameDictionary: {ws_pid: name} para resolver nombres
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy import text

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/whoscored")


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def _safe_str(val) -> str | None:
    if val is None:
        return None
    return str(val)


def _get_display_name(obj: dict | str | None, key: str = "displayName") -> str | None:
    if isinstance(obj, dict):
        return obj.get(key)
    return _safe_str(obj)


# ─────────────────────────────────────────────────────
# CORE LOADER
# ─────────────────────────────────────────────────────

def load_whoscored_events(
    conn,
    match_id_ext: int,
    match_data: dict,
    batch_id: str,
) -> int:
    """
    Inserta eventos de un partido de WhoScored en stg_whoscored_events.
    Retorna número de filas insertadas.
    """
    events = match_data.get("events", [])
    player_names = match_data.get("playerIdNameDictionary", {})

    # Mapeo de team_id a nombre (si está disponible)
    home_name = (match_data.get("home") or {}).get("name")
    away_name = (match_data.get("away") or {}).get("name")

    inserted = 0

    for ev in events:
        try:
            ev_id_ext = ev.get("id") or ev.get("eventId")

            # Tipo de evento
            ev_type = _get_display_name(ev.get("type"))

            # Jugador
            ws_pid = ev.get("playerId")
            player_name = player_names.get(str(ws_pid)) if ws_pid else None

            # Equipo (WhoScored usa teamId)
            team_id_ws = ev.get("teamId")
            team_name = None
            if team_id_ws is not None:
                # Intento básico de resolución de equipo
                home_id = (match_data.get("home") or {}).get("teamId")
                away_id = (match_data.get("away") or {}).get("teamId")
                if team_id_ws == home_id:
                    team_name = home_name
                elif team_id_ws == away_id:
                    team_name = away_name

            # Minuto
            minute = ev.get("minute")
            minute_str = str(minute) if minute is not None else None

            # Coordenadas
            x = ev.get("x")
            y = ev.get("y")

            conn.execute(text("""
                INSERT INTO stg_whoscored_events (
                    match_id_ext, event_id_ext, event_type,
                    minute, player_name, team_name,
                    x, y, raw_json, batch_id
                )
                VALUES (
                    :match_id, :event_id, :event_type,
                    :minute, :player, :team,
                    :x, :y, :raw, :batch
                )
                ON CONFLICT DO NOTHING
            """), {
                "match_id":   match_id_ext,
                "event_id":   int(ev_id_ext) if ev_id_ext is not None else None,
                "event_type": _safe_str(ev_type),
                "minute":     minute_str,
                "player":     player_name,
                "team":       team_name,
                "x":          str(x) if x is not None else None,
                "y":          str(y) if y is not None else None,
                "raw":        json.dumps(ev, ensure_ascii=False, default=str),
                "batch":      batch_id,
            })
            inserted += 1

        except Exception as exc:
            log.warning("Error insertando evento WhoScored (match=%d, ev=%s): %s",
                        match_id_ext, ev.get("id"), exc)

    return inserted


# ─────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────

def run_whoscored_loader(conn, base_dir: str | Path = RAW_BASE, batch_id: str | None = None) -> int:
    """
    Recorre el raw layer de WhoScored y carga todos los eventos.

    Estructura esperada:
        base_dir/match_{id}/batch_id={batch}/events.json
    """
    base_dir = Path(base_dir)
    total = 0

    event_files = list(base_dir.glob("**/events.json"))
    log.info("Archivos events.json (WhoScored) encontrados: %d", len(event_files))

    for event_file in event_files:
        match_dir_name = event_file.parent.parent.name
        match_id_str = match_dir_name.replace("match_", "")
        try:
            match_id_ext = int(match_id_str)
        except ValueError:
            log.warning("No se pudo parsear match_id de la ruta: %s", event_file)
            continue

        effective_batch = batch_id or event_file.parent.name.replace("batch_id=", "")

        try:
            with open(event_file, encoding="utf-8") as f:
                match_data = json.load(f)

            if not isinstance(match_data, dict):
                log.warning("Formato inesperado en %s", event_file)
                continue

            n = load_whoscored_events(conn, match_id_ext, match_data, effective_batch)
            total += n
            log.info("[OK] match %d → %d eventos", match_id_ext, n)

        except Exception as exc:
            log.error("Error procesando %s: %s", event_file, exc)

    log.info("TOTAL stg_whoscored_events insertados: %d", total)
    return total
