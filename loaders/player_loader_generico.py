"""
loaders/player_loader.py
=========================
Carga dim_player y player_review desde los archivos producidos por los scrapers.

FUENTES Y FASES:
    Fase 1 — Transfermarkt (MASTER):
        data/raw/transfermarkt/**/players_clean.csv
        → INSERT dim_player(canonical_name, id_transfermarkt, nationality, birth_date, position)
        → ON CONFLICT (id_transfermarkt) DO UPDATE (COALESCE para no pisar datos existentes)

    Fase 2 — SofaScore (enlace por nombre):
        data/raw/sofascore/**/players.csv
        → Para cada (id_sofascore, canonical_name):
            a. Si ya existe dim_player con id_sofascore = X → skip
            b. Buscar dim_player por nombre exacto → UPDATE id_sofascore
            c. Buscar fuzzy → INSERT player_review (resolved=False)
            d. Sin match    → INSERT player_review para revisión manual

    Fase 3 — Understat (enlace por nombre):
        data/raw/understat/understat_players_laliga.csv
        → Mismo proceso que Fase 2 pero con id_understat

Schema destino:
    dim_player:    canonical_id, canonical_name, nationality, birth_date, position,
                   id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
    player_review: id, source_name, source_system, source_id,
                   suggested_canonical_id, similarity_score, resolved
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.mdm_engine import resolve_player

log = logging.getLogger(__name__)



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

def _load_phase1_transfermarkt(conn, tm_path: Path) -> int:
    """Crea los registros canónicos de jugadores desde Transfermarkt.

    Transfermarkt es la fuente de verdad para:
        - canonical_name, nationality, birth_date, position, id_transfermarkt

    Returns:
        Número de jugadores insertados/actualizados.
    """
    files = list(tm_path.glob("**/players_clean.csv"))
    

    if not files:
        log.warning("player_loader fase 1: no se encontró players_clean.csv en %s", tm_path)
        return 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading file %s: %s", f, e)
            continue

    # Deduplicar por ID (el más reciente si hay duplicados)
    seen_ids: dict[int, dict] = {}
    for row in all_rows:
        tid = row.get("player_id") or row.get("id_transfermarkt")
        if tid is None:
            continue
        try:
            tid = int(tid)
        except (ValueError, TypeError):
            continue
        if tid not in seen_ids:
            seen_ids[tid] = row

    count = 0
    for tid_raw, row in seen_ids.items():
        # Savepoint para no abortar toda la transacción si un jugador falla
        sp_name = f"player_{tid_raw}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))
        
        try:
            # El scraper usa 'player_name' y 'player_id'
            name    = row.get("player_name") or row.get("canonical_name")
            nat     = row.get("nationality")    or None
            birth   = _ensure_date(row.get("birth_date"))
            pos     = row.get("position")       or None
            tid     = row.get("player_id") or row.get("id_transfermarkt") or tid_raw

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
            log.error("Error inserting player %d: %s", tid, e)
            continue

    log.info("dim_player ← Transfermarkt (fase 1): %d jugadores", count)
    return count


# ── FASE 2: Linkear IDs de SofaScore ───────────────────────────────────────

def _load_phase2_sofascore(conn, ss_path: Path) -> tuple[int, int]:
    """Enlaza id_sofascore a jugadores existentes de TM via resolución de nombre.

    - Match exacto  → UPDATE dim_player.id_sofascore
    - Match fuzzy   → INSERT player_review (resolved=False)
    - Sin match     → INSERT player_review para revisión

    Returns:
        (linked, queued) — enlaces directos y encolados en player_review.
    """
    
    files = list(ss_path.glob("**/players.csv"))

    if not files:
        log.info("player_loader fase 2: no hay players.csv de SofaScore")
        return 0, 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)

    # Deduplicar por id_sofascore
    seen: dict[int, dict] = {}
    for row in all_rows:
        sid = row.get("id_sofascore")
        if sid is None:
            continue
        try:
            sid = int(sid)
        except (ValueError, TypeError):
            continue
        if sid not in seen:
            seen[sid] = row

    linked = queued = 0
    for sid, row in seen.items():
        name = row.get("canonical_name")
        if not name:
            continue

        canonical_id = resolve_player(conn, name, "sofascore", source_id=sid)
        if canonical_id:
            linked += 1
        else:
            queued += 1

    log.info("dim_player ← SofaScore (fase 2): %d enlazados | %d encolados en player_review", linked, queued)
    return linked, queued


# ── FASE 3: Linkear IDs de Understat ──────────────────────────────────────

def _load_phase3_understat(conn, us_path: Path) -> tuple[int, int]:
    """Enlaza id_understat a jugadores existentes de TM via resolución de nombre.

    Returns:
        (linked, queued)
    """
    f = us_path / "understat_players_laliga.csv"
    if not f.exists():
        log.info("player_loader fase 3: no hay understat_players_laliga.csv")
        return 0, 0

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.warning("Error leyendo %s: %s", f, e)
        return 0, 0

    linked = queued = 0
    seen: set[int] = set()
    for _, row in df.iterrows():
        us_id   = row.get("understat_player_id")
        us_name = row.get("player_name")
        if not us_id or not us_name:
            continue
        try:
            us_id = int(us_id)
        except (ValueError, TypeError):
            continue
        if us_id in seen:
            continue
        seen.add(us_id)

        canonical_id = resolve_player(conn, us_name, "understat", source_id=us_id)
        if canonical_id:
            linked += 1
        else:
            queued += 1

    log.info("dim_player ← Understat (fase 3): %d enlazados | %d encolados en player_review", linked, queued)
    return linked, queued


# ── FASE 4: Linkear IDs de StatsBomb ───────────────────────────────────────

def _load_phase4_statsbomb(conn, sb_path) -> tuple[int, int]:
    """Enlaza id_statsbomb a jugadores existentes de TM via resolución de nombre."""
    
    files = list(sb_path.glob("**/players.csv"))

    if not files:
        log.info("player_loader fase 4: no hay players.csv de StatsBomb")
        return 0, 0

    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)

    # Deduplicar por id_statsbomb
    seen: dict[str, dict] = {}
    for row in all_rows:
        sb_id = row.get("id_statsbomb")
        if not sb_id:
            continue
        if sb_id not in seen:
            seen[sb_id] = row

    linked = queued = 0
    for sb_id, row in seen.items():
        name = row.get("canonical_name")
        if not name:
            continue

        canonical_id = resolve_player(conn, name, "statsbomb", source_id=sb_id)
        if canonical_id:
            linked += 1
        else:
            queued += 1

    log.info("dim_player ← StatsBomb (fase 4): %d enlazados | %d encolados en player_review", linked, queued)
    return linked, queued


# ── FASE 5: Linkear IDs de WhoScored ───────────────────────────────────────

def _load_phase5_whoscored(conn,ws_path:Path) -> tuple[int, int]:
    """Enlaza id_whoscored a jugadores existentes de TM via resolución de nombre."""
        
    f = ws_path / "players_clean.csv"
    if not f.exists():
                log.info("player_loader fase 5: no hay players_clean.csv en %s", ws_path)
                return 0, 0

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.warning("Error leyendo %s: %s", f, e)
        return 0, 0

    linked = queued = 0
    seen: set[int] = set()
    for _, row in df.iterrows():
        ws_id   = row.get("whoscored_player_id")
        ws_name = row.get("player_name")
        if not ws_id or not ws_name:
            continue
        try:
            ws_id = int(ws_id)
        except (ValueError, TypeError):
            continue
        if ws_id in seen:
            continue
        seen.add(ws_id)

        canonical_id = resolve_player(conn, ws_name, "whoscored", source_id=ws_id)
        if canonical_id:
            linked += 1
        else:
            queued += 1

    log.info("dim_player ← WhoScored (fase 5): %d enlazados | %d encolados en player_review", linked, queued)
    return linked, queued


# ── Punto de entrada ──────────────────────────────────────────────────────────

def load_players(conn,tm_path: Optional[Path] = None,
    ss_path: Optional[Path] = None,
    us_path: Optional[Path] = None,
    sb_path: Optional[Path] = None,
    ws_path: Optional[Path] = None,) -> int:
    """Carga dim_player en 3 fases respetando la jerarquía de fuentes.

    Returns:
        Número total de jugadores en dim_player al finalizar.
    """
    log.info("[START] Cargando dim_player...")


    if tm_path:
        _load_phase1_transfermarkt(conn, tm_path)
    if ss_path:
        _load_phase2_sofascore(conn, ss_path)
    if us_path:
        _load_phase3_understat(conn, us_path)
    if sb_path:
        _load_phase4_statsbomb(conn, sb_path)
    if ws_path:
        _load_phase5_whoscored(conn, ws_path)

    # Reporte final
    total = conn.execute(text("SELECT COUNT(*) FROM dim_player")).scalar()
    pending_review = conn.execute(
        text("SELECT COUNT(*) FROM player_review WHERE resolved = FALSE")
    ).scalar()

    log.info("[OK] dim_player completado — %d jugadores canónicos | %d pendientes en player_review",
             total, pending_review)
    return total


