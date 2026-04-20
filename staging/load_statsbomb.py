"""
staging/load_statsbomb.py
=========================
Carga eventos de StatsBomb (JSON del raw layer) en stg_statsbomb_events.

StatsBomb Open Data ya incluye el UUID único por evento (campo 'id'),
que se usa para la deduplicación con ON CONFLICT (event_uuid).
"""
from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from sqlalchemy import text

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/statsbomb")


# ─────────────────────────────────────────────────────
# CORE LOADER
# ─────────────────────────────────────────────────────

def sanitize_nan(obj):
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: sanitize_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_nan(i) for i in obj]
    return obj

def load_statsbomb_events(conn, match_id_ext: int, events: list[dict], batch_id: str) -> int:
    """
    Inserta una lista de eventos de StatsBomb en stg_statsbomb_events.
    Retorna número de filas insertadas.
    """
    inserted = 0

    for ev in events:
        try:
            # El UUID único de StatsBomb
            event_uuid = ev.get("id")  # campo 'id' en statsbombpy

            # Tipo de evento como string
            ev_type = ev.get("type")
            if isinstance(ev_type, dict):
                ev_type = ev_type.get("name", "")

            # Jugador
            player = ev.get("player", {})
            player_name = player.get("name") if isinstance(player, dict) else str(player or "")

            # Equipo
            team = ev.get("team", {})
            team_name = team.get("name") if isinstance(team, dict) else str(team or "")

            minute = ev.get("minute")
            second = ev.get("second")

            conn.execute(text("""
                INSERT INTO stg_statsbomb_events (
                    match_id_ext, event_uuid, event_type,
                    minute, second, player_name, team_name,
                    raw_json, batch_id
                )
                VALUES (
                    :match_id, :uuid, :event_type,
                    :minute, :second, :player, :team,
                    :raw, :batch
                )
                ON CONFLICT (event_uuid)
                WHERE event_uuid IS NOT NULL
                DO NOTHING
            """), {
                "match_id":   match_id_ext,
                "uuid":       str(event_uuid) if event_uuid else None,
                "event_type": str(ev_type or ""),
                "minute":     int(minute) if minute is not None else None,
                "second":     int(second) if second is not None else None,
                "player":     player_name,
                "team":       team_name,
                "raw":        json.dumps(sanitize_nan(ev), ensure_ascii=False, default=str),
                "batch":      batch_id,
            })
            inserted += 1

        except Exception as exc:
            log.warning("Error insertando evento StatsBomb (match=%s, uuid=%s): %s",
                        match_id_ext, ev.get("id"), exc)

    return inserted


# ─────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────

def run_statsbomb_loader(conn, base_dir: str | Path = RAW_BASE, batch_id: str | None = None) -> int:
    """
    Recorre el raw layer de StatsBomb y carga todos los eventos.

    Estructura esperada:
        base_dir/competition_{id}/season_{id}/match_{id}/batch_id={batch}/events.json
    """
    base_dir = Path(base_dir)
    total = 0

    event_files = list(base_dir.glob("**/events.json"))
    log.info("Archivos events.json (StatsBomb) encontrados: %d", len(event_files))

    for event_file in event_files:
        # Inferir match_id desde la ruta
        match_dir_name = event_file.parent.parent.name  # match_{id}
        match_id_ext_str = match_dir_name.replace("match_", "")
        try:
            match_id_ext = int(match_id_ext_str)
        except ValueError:
            log.warning("No se pudo parsear match_id de la ruta: %s", event_file)
            continue

        effective_batch = batch_id or event_file.parent.name.replace("batch_id=", "")

        try:
            with open(event_file, encoding="utf-8") as f:
                events = json.load(f)

            if not isinstance(events, list):
                log.warning("Formato inesperado en %s", event_file)
                continue

            n = load_statsbomb_events(conn, match_id_ext, events, effective_batch)
            total += n
            log.info("[OK] match %d → %d eventos", match_id_ext, n)

        except Exception as exc:
            log.error("Error procesando %s: %s", event_file, exc)

    log.info("TOTAL stg_statsbomb_events insertados: %d", total)
    return total
