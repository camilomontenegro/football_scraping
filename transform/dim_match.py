"""
transform/dim_match.py
======================
Poblar dim_match a partir de los JSON de partidos guardados por sofascore_extract.

Flujo:
data/raw/sofascore/season=*/matches_batch_*.json
    → dim_match (match_date, season_id, home_team_id, away_team_id, scores)
    → match_external_ids (source='sofascore', external_id=sofascore_match_id)
"""
from __future__ import annotations

import glob
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from utils.mdm_engine import resolve
from utils.mdm_helpers import get_entity_id

log = logging.getLogger(__name__)

RAW_BASE = Path("data/raw/sofascore")


# ─────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────

def _ts_to_date(ts: int | None):
    """Unix timestamp → date string 'YYYY-MM-DD'."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


def _score(score_obj, key="current") -> int | None:
    if isinstance(score_obj, dict):
        val = score_obj.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                pass
    return None


def _find_match_jsons(base: Path) -> list[Path]:
    """Retorna todos los matches_batch_*.json encontrados recursivamente."""
    return list(base.glob("**/matches_batch_*.json"))


# ─────────────────────────────────────────────────────
# CORE
# ─────────────────────────────────────────────────────

def load_dim_match(conn, base_dir: str | Path = RAW_BASE) -> int:
    """
    Lee los ficheros de matches de SofaScore y rellena dim_match.
    Devuelve número de filas insertadas.
    """
    base_dir = Path(base_dir)
    json_files = _find_match_jsons(base_dir)

    if not json_files:
        log.warning("No se encontraron matches_batch_*.json en %s", base_dir)
        return 0

    # Obtener season_id de LaLiga 2020/2021
    season_id = conn.execute(text("""
        SELECT season_id FROM dim_season
        WHERE label = '2020/2021'
        LIMIT 1
    """)).scalar()

    if not season_id:
        log.error("dim_season no tiene la fila '2020/2021'. Ejecuta primero load_dim_season.")
        return 0

    inserted = 0

    for jf in json_files:
        try:
            with open(jf, encoding="utf-8") as f:
                matches = json.load(f)
        except Exception as e:
            log.warning("Error leyendo %s: %s", jf, e)
            continue

        if not isinstance(matches, list):
            continue

        for m in matches:
            ext_id = str(m.get("id", ""))
            if not ext_id:
                continue

            # CRÍTICO: Sólo cargar a la dimensión los partidos de los que efectivamente hayamos guardado el detalle
            match_folders = list(base_dir.rglob(f"match_{ext_id}"))
            if not match_folders:
                continue

            match_date = _ts_to_date(m.get("startTimestamp"))
            if not match_date:
                continue

            home_name = (m.get("homeTeam") or {}).get("name")
            away_name = (m.get("awayTeam") or {}).get("name")

            if not home_name or not away_name:
                continue

            # ── Resolver equipos via MDM ──────────────────────
            home_res = resolve(conn, "team", home_name, "sofascore")
            away_res = resolve(conn, "team", away_name, "sofascore")
            home_team_id = get_entity_id(home_res)
            away_team_id = get_entity_id(away_res)

            home_score = _score(m.get("homeScore"))
            away_score = _score(m.get("awayScore"))

            # ── Insertar en dim_match ─────────────────────────
            match_id = conn.execute(text("""
                INSERT INTO dim_match (
                    match_date, season_id,
                    home_team_id, away_team_id,
                    home_score, away_score,
                    data_source
                )
                VALUES (
                    :match_date, :season_id,
                    :home_team_id, :away_team_id,
                    :home_score, :away_score,
                    'sofascore'
                )
                ON CONFLICT (season_id, match_date, home_team_id, away_team_id, data_source)
                DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score
                RETURNING match_id
            """), {
                "match_date": match_date,
                "season_id": season_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": home_score,
                "away_score": away_score,
            }).scalar()

            if match_id:
                # ── Registrar external ID ────────────────────
                conn.execute(text("""
                    INSERT INTO match_external_ids (match_id, source, external_id)
                    VALUES (:mid, 'sofascore', :ext)
                    ON CONFLICT DO NOTHING
                """), {"mid": match_id, "ext": ext_id})

                inserted += 1

    log.info("dim_match: %d partidos insertados/actualizados desde SofaScore", inserted)
    return inserted
