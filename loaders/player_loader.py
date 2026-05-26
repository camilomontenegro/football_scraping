"""
loaders/player_loader.py
=========================
Carga dim_player y player_review desde los CSV producidos por los scrapers en
`data/clean/<comp>/<season>/<source>/<table>.csv`.

FUENTES Y FASES:
    Fase 1 — Transfermarkt (MASTER): `transfermarkt/players.csv`
        → INSERT dim_player(canonical_name, id_transfermarkt, nationality,
                            birth_date, position)
        → ON CONFLICT (id_transfermarkt) DO UPDATE con COALESCE.

    Fase 2 — SofaScore:  `sofascore/players.csv`     → id_sofascore
    Fase 3 — Understat:  `understat/players.csv`     → id_understat
    Fase 4 — StatsBomb:  `statsbomb/players.csv`     → id_statsbomb
    Fase 5 — WhoScored:  `whoscored/players.csv`     → id_whoscored

Schema destino:
    dim_player:    canonical_id, canonical_name, nationality, birth_date, position,
                   id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
    player_review: id, source_name, source_system, source_id,
                   suggested_canonical_id, similarity_score, resolved
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.mdm_engine import resolve_player
from utils.data_paths import iter_clean_csvs

log = logging.getLogger(__name__)


def _read_clean(source: str, filename: str,
                competition: Optional[str] = None) -> list[pd.DataFrame]:
    """Lee todos los `data/clean/[<comp>/]*/<source>/<filename>.csv`."""
    dfs: list[pd.DataFrame] = []
    for f in iter_clean_csvs(competition=competition, source=source, filename=filename):
        try:
            dfs.append(pd.read_csv(f))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)
    return dfs


def _ensure_date(val) -> Optional[str]:
    """Asegura que el valor sea un string de fecha (YYYY-MM-DD).
    Maneja milisegundos (epochs) que Pandas a veces genera.
    """
    if val is None or str(val).strip().lower() in ("nan", "none", ""):
        return None
    
    # Si viene como número (milisegundos)
    if isinstance(val, (int, float)):
        try:
            # Convertir milisegundos a objeto date
            from datetime import datetime
            return datetime.fromtimestamp(val / 1000.0).date().isoformat()
        except Exception:
            return None
            
    # Si ya es string, devolver primeros 10 caracteres
    return str(val)[:10]



# ── FASE 1: Transfermarkt como master ──────────────────────────────────────

def _load_phase1_transfermarkt(conn, comp_name: Optional[str] = None) -> int:
    """Crea los registros canónicos de jugadores desde Transfermarkt."""
    dfs = _read_clean("transfermarkt", "players", competition=comp_name)
    if not dfs:
        log.warning("player_loader fase 1: no se encontró players.csv de TM")
        return 0

    seen_ids: dict[int, dict] = {}
    for df in dfs:
        for row in df.to_dict("records"):
            tid = row.get("player_id") or row.get("id_transfermarkt")
            if tid is None:
                continue
            try:
                tid = int(tid)
            except (TypeError, ValueError):
                continue
            if tid not in seen_ids:
                seen_ids[tid] = row

    count = 0
    for tid_raw, row in seen_ids.items():
        sp_name = f"player_{tid_raw}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))
        try:
            name  = row.get("player_name") or row.get("canonical_name")
            nat   = row.get("nationality") or None
            birth = _ensure_date(row.get("birth_date"))
            pos   = row.get("position") or None
            tid   = row.get("player_id") or row.get("id_transfermarkt") or tid_raw
            if not name or not tid:
                conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
                continue
            conn.execute(text("""
                INSERT INTO dim_player
                    (canonical_name, nationality, birth_date, position, id_transfermarkt)
                VALUES
                    (:name, :nat, :birth, :pos, :tid)
                ON CONFLICT (id_transfermarkt) WHERE id_transfermarkt IS NOT NULL
                DO UPDATE SET
                    canonical_name = EXCLUDED.canonical_name,
                    nationality    = COALESCE(dim_player.nationality, EXCLUDED.nationality),
                    birth_date     = COALESCE(dim_player.birth_date,  EXCLUDED.birth_date),
                    position       = COALESCE(dim_player.position,    EXCLUDED.position)
            """), {"name": name, "nat": nat, "birth": birth, "pos": pos, "tid": tid})
            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            count += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.error("Error inserting player %s: %s", tid_raw, e)
            continue

    log.info("dim_player ← Transfermarkt (fase 1): %d jugadores", count)
    return count


def _link_source_phase(conn, source: str, id_col: str, name_col: str,
                       comp_name: Optional[str], fase_label: str) -> tuple[int, int]:
    """Enlace genérico fuente → dim_player usando resolve_player()."""
    dfs = _read_clean(source, "players", competition=comp_name)
    if not dfs:
        log.info("player_loader %s: no hay players.csv de %s", fase_label, source)
        return 0, 0

    # Column names for team context vary by source
    _TEAM_NAME_COLS = ("team_name", "team", "team_slug")
    _TEAM_ID_COLS   = ("team_id", "team_id_ss", "team_id_tm",
                       "whoscored_team_id", "team_id_sb")

    linked = queued = 0
    seen: set = set()
    for df in dfs:
        for _, row in df.iterrows():
            ext_id = row.get(id_col)
            ext_name = row.get(name_col) or row.get("canonical_name") or row.get("player_name")
            if ext_id is None or not ext_name:
                continue
            try:
                ext_id_norm = int(ext_id)
            except (TypeError, ValueError):
                ext_id_norm = ext_id
            if ext_id_norm in seen:
                continue
            seen.add(ext_id_norm)

            # Extract team context for player_review disambiguation
            t_name = None
            for col in _TEAM_NAME_COLS:
                v = row.get(col)
                if v is not None and str(v).strip():
                    t_name = str(v).strip()
                    break
            t_id = None
            for col in _TEAM_ID_COLS:
                v = row.get(col)
                if v is not None and str(v).strip():
                    t_id = str(v).strip()
                    break

            try:
                cid = resolve_player(
                    conn, ext_name, source, source_id=ext_id_norm,
                    team_name=t_name, team_id=t_id,
                )
            except Exception as e:
                log.warning("resolve_player(%s, %s): %s", source, ext_name, e)
                cid = None
            if cid:
                linked += 1
            else:
                queued += 1

    log.info("dim_player ← %s (%s): %d enlazados | %d encolados",
             source, fase_label, linked, queued)
    return linked, queued


def _load_phase2_sofascore(conn, comp_name: Optional[str] = None) -> tuple[int, int]:
    return _link_source_phase(conn, "sofascore", "id_sofascore", "canonical_name",
                              comp_name, "fase 2")


def _load_phase3_understat(conn, comp_name: Optional[str] = None) -> tuple[int, int]:
    return _link_source_phase(conn, "understat", "understat_player_id", "player_name",
                              comp_name, "fase 3")


def _load_phase4_statsbomb(conn, comp_name: Optional[str] = None) -> tuple[int, int]:
    return _link_source_phase(conn, "statsbomb", "id_statsbomb", "canonical_name",
                              comp_name, "fase 4")


def _load_phase5_whoscored(conn, comp_name: Optional[str] = None) -> tuple[int, int]:
    return _link_source_phase(conn, "whoscored", "whoscored_player_id", "player_name",
                              comp_name, "fase 5")


def load_players(conn, comp_name: str = None) -> int:
    """Carga dim_player en 5 fases respetando la jerarquía de fuentes."""
    log.info(f"[START] Cargando dim_player ({comp_name or 'todas'})...")

    # Fase 1 — TM como master (crea los registros canónicos)
    _load_phase1_transfermarkt(conn, comp_name)

    # IMPORTANTE: Limpiar caché MDM porque la fase 1 inserta nuevos jugadores
    from utils.mdm_engine import clear_player_cache
    clear_player_cache()

    # Fase 2 — SofaScore (enlace por nombre)
    _load_phase2_sofascore(conn, comp_name)

    # Fase 3 — Understat (enlace por nombre)
    _load_phase3_understat(conn, comp_name)

    # Fase 4 — StatsBomb (enlace por nombre)
    _load_phase4_statsbomb(conn, comp_name)

    # Fase 5 — WhoScored (enlace por nombre)
    _load_phase5_whoscored(conn, comp_name)

    # Reporte final
    total = conn.execute(text("SELECT COUNT(*) FROM dim_player")).scalar()
    pending_review = conn.execute(
        text("SELECT COUNT(*) FROM player_review WHERE resolved = FALSE")
    ).scalar()

    log.info("[OK] dim_player completado — %d jugadores canónicos | %d pendientes en player_review",
             total, pending_review)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_players(conn)
