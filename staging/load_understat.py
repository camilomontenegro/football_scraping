"""
staging/load_understat.py
=========================
Carga tiros de Understat (JSON del raw layer) en stg_understat_shots.

Patrón idéntico a load_sofascore.py.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from sqlalchemy import text

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/understat")


# ─────────────────────────────────────────────────────
# CORE LOADER
# ─────────────────────────────────────────────────────

def load_understat_shots(conn, match_id_ext: str, shots: list[dict], batch_id: str) -> int:
    """
    Inserta una lista de shots de Understat en stg_understat_shots.
    Retorna número de filas insertadas.
    """
    inserted = 0

    for s in shots:
        try:
            # Understat usa escala 0-1 en coords (porcentaje del campo)
            x_raw = s.get("X") or s.get("x")
            y_raw = s.get("Y") or s.get("y")
            xg_raw = s.get("xG") or s.get("xg")

            x_val  = float(x_raw) if x_raw is not None else None
            y_val  = float(y_raw) if y_raw is not None else None
            xg_val = float(xg_raw) if xg_raw is not None else None

            minute_raw = s.get("minute")
            minute = int(minute_raw) if minute_raw is not None else None

            conn.execute(text("""
                INSERT INTO stg_understat_shots (
                    match_id_ext, player_id_ext, player_name, team_name,
                    h_team, a_team, season,
                    minute, x, y, xg, result, shot_type, situation,
                    raw_json, batch_id
                )
                VALUES (
                    :match_id, :player_id, :player, :team,
                    :h_team, :a_team, :season,
                    :minute, :x, :y, :xg, :result, :shot_type, :situation,
                    :raw, :batch
                )
                ON CONFLICT (match_id_ext, player_id_ext, minute, batch_id)
                WHERE match_id_ext IS NOT NULL
                DO NOTHING
            """), {
                "match_id":  str(match_id_ext),
                "player_id": str(s.get("player_id") or ""),
                "player":    s.get("player"),
                "team":      s.get("h_team") if s.get("h_a") == "h" else s.get("a_team"),
                "h_team":    s.get("h_team"),
                "a_team":    s.get("a_team"),
                "season":    s.get("season"),
                "minute":    minute,
                "x":         x_val,
                "y":         y_val,
                "xg":        xg_val,
                "result":    s.get("result"),
                "shot_type": s.get("shotType"),
                "situation": s.get("situation"),
                "raw":       json.dumps(s, ensure_ascii=False),
                "batch":     batch_id,
            })
            inserted += 1

        except Exception as exc:
            log.warning("Error insertando shot (match=%s): %s", match_id_ext, exc)

    return inserted


# ─────────────────────────────────────────────────────
# ORQUESTADOR
# ─────────────────────────────────────────────────────

def run_understat_loader(conn, base_dir: str | Path = RAW_BASE, batch_id: str | None = None) -> int:
    """
    Recorre el raw layer de Understat y carga todos los shots encontrados.

    Estructura esperada:
        base_dir/season={year}/match_{id}/batch_id={batch}/shots.json
    """
    base_dir = Path(base_dir)
    total = 0

    shot_files = list(base_dir.rglob("shots.json"))
    log.info("Archivos shots.json encontrados: %d", len(shot_files))

    for shot_file in shot_files:
        match_id_ext = shot_file.parent.parent.name.replace("match_", "")

        # Inferir batch_id desde la ruta si no viene como parámetro
        effective_batch = batch_id or shot_file.parent.name.replace("batch_id=", "")

        try:
            with open(shot_file, encoding="utf-8") as f:
                shots = json.load(f)

            if not isinstance(shots, list):
                log.warning("Formato inesperado en %s", shot_file)
                continue

            n = load_understat_shots(conn, match_id_ext, shots, effective_batch)
            total += n
            log.info("[OK] match %s → %d shots", match_id_ext, n)

        except Exception as exc:
            log.error("Error procesando %s: %s", shot_file, exc)

    log.info("TOTAL stg_understat_shots insertados: %d", total)
    return total
