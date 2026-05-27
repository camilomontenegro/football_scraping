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
        → Registra procedencia en player_scrape_provenance.

    Fase 2 — SofaScore:  `sofascore/players.csv`     → id_sofascore
    Fase 3 — Understat:  `understat/players.csv`     → id_understat
    Fase 4 — StatsBomb:  `statsbomb/players.csv`     → id_statsbomb
    Fase 5 — WhoScored:  `whoscored/players.csv`     → id_whoscored

Schema destino:
    dim_player:    canonical_id, canonical_name, nationality, birth_date, position,
                   id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
    player_review: id, source_name, source_system, source_id,
                   suggested_canonical_id, similarity_score, resolved,
                   competition, season, source_team_name, source_team_id
    player_scrape_provenance: trazabilidad comp/temporada/equipo por avistamiento
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from loaders.player_provenance import upsert_player_provenance, ensure_player_provenance_schema
from utils.mdm_engine import resolve_player
from utils.data_paths import iter_clean_csvs, parse_clean_csv_meta, normalize_season

log = logging.getLogger(__name__)

_TEAM_NAME_COLS = ("team_name", "team", "team_slug")
_TEAM_ID_COLS = (
    "team_id", "team_id_ss", "team_id_tm",
    "whoscored_team_id", "team_id_sb", "understat_team_id",
)
_NAME_COLS = ("player_name", "canonical_name", "name", "scraped_name")


def _read_clean(source: str, filename: str,
                competition: Optional[str] = None) -> list[tuple[pd.DataFrame, dict]]:
    """Lee CSVs clean con metadatos de ruta (comp_slug, season, source)."""
    bundles: list[tuple[pd.DataFrame, dict]] = []
    for f in iter_clean_csvs(competition=competition, source=source, filename=filename):
        try:
            meta = parse_clean_csv_meta(Path(f))
            bundles.append((pd.read_csv(f), meta))
        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)
    return bundles


def _ensure_date(val) -> Optional[str]:
    """Asegura que el valor sea un string de fecha (YYYY-MM-DD)."""
    if val is None or str(val).strip().lower() in ("nan", "none", ""):
        return None

    if isinstance(val, (int, float)):
        try:
            from datetime import datetime
            return datetime.fromtimestamp(val / 1000.0).date().isoformat()
        except Exception:
            return None

    return str(val)[:10]


def _season_label(row: dict, meta: dict) -> str:
    raw = row.get("season")
    if raw is not None and str(raw).strip().lower() not in ("nan", "none", ""):
        norm = normalize_season(raw)
        if norm:
            return norm.replace("_", "/")
        s = str(raw).strip()
        return s.replace("_", "/")
    disp = meta.get("season_display") or meta.get("season") or ""
    return disp.replace("_", "/")


def _competition_label(row: dict, meta: dict) -> str:
    comp = row.get("competition")
    if comp is not None and str(comp).strip().lower() not in ("nan", "none", ""):
        return str(comp).strip()
    slug = meta.get("comp_slug") or ""
    return slug.replace("_", " ").title()


def _first_col(row, cols: tuple[str, ...]):
    for col in cols:
        v = row.get(col)
        if v is not None and str(v).strip().lower() not in ("nan", "none", ""):
            return str(v).strip()
    return None


def _player_name(row) -> Optional[str]:
    if hasattr(row, "get"):
        for col in _NAME_COLS:
            v = row.get(col)
            if v is not None and str(v).strip().lower() not in ("nan", "none", ""):
                return str(v).strip()
    return None


def _record_context(row, meta: dict) -> tuple[str, str, Optional[str], Optional[str]]:
    return (
        _competition_label(row, meta),
        _season_label(row, meta),
        _first_col(row, _TEAM_NAME_COLS),
        _first_col(row, _TEAM_ID_COLS),
    )


def _lookup_canonical_by_tm(conn, tid) -> Optional[int]:
    row = conn.execute(
        text("SELECT canonical_id FROM dim_player WHERE id_transfermarkt = :tid LIMIT 1"),
        {"tid": tid},
    ).fetchone()
    return row[0] if row else None


# ── FASE 1: Transfermarkt como master ──────────────────────────────────────

def _load_phase1_transfermarkt(conn, comp_name: Optional[str] = None) -> int:
    """Crea los registros canónicos de jugadores desde Transfermarkt."""
    bundles = _read_clean("transfermarkt", "players", competition=comp_name)
    if not bundles:
        log.warning("player_loader fase 1: no se encontró players.csv de TM")
        return 0

    seen_ids: dict[int, dict] = {}
    seen_meta: dict[int, dict] = {}
    for df, meta in bundles:
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
                seen_meta[tid] = meta

    count = 0
    for tid_raw, row in seen_ids.items():
        meta = seen_meta[tid_raw]
        sp_name = f"player_{tid_raw}"
        conn.execute(text(f"SAVEPOINT {sp_name}"))
        try:
            name = _player_name(row)
            nat = row.get("nationality") or None
            birth = _ensure_date(row.get("birth_date"))
            pos = row.get("position") or None
            tid = row.get("player_id") or row.get("id_transfermarkt") or tid_raw
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

            cid = _lookup_canonical_by_tm(conn, tid)
            comp, season, team_name, team_id = _record_context(row, meta)
            upsert_player_provenance(
                conn,
                source_system="transfermarkt",
                source_player_id=tid,
                scraped_name=name,
                competition=comp,
                season=season,
                team_name=team_name,
                team_id=team_id,
                canonical_id=cid,
            )

            conn.execute(text(f"RELEASE SAVEPOINT {sp_name}"))
            count += 1
        except Exception as e:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp_name}"))
            log.error("Error inserting player %s: %s", tid_raw, e)
            continue

    log.info("dim_player ← Transfermarkt (fase 1): %d jugadores", count)
    return count


def _link_source_phase(
    conn,
    source: str,
    id_col: str,
    name_col: str,
    comp_name: Optional[str],
    fase_label: str,
) -> tuple[int, int]:
    """Enlace genérico fuente → dim_player usando resolve_player()."""
    bundles = _read_clean(source, "players", competition=comp_name)
    if not bundles:
        log.info("player_loader %s: no hay players.csv de %s", fase_label, source)
        return 0, 0

    linked = queued = 0
    seen: set = set()
    for df, meta in bundles:
        for _, row in df.iterrows():
            ext_id = row.get(id_col)
            ext_name = row.get(name_col) or _player_name(row)
            if ext_id is None or not ext_name:
                continue
            try:
                ext_id_norm = int(ext_id)
            except (TypeError, ValueError):
                ext_id_norm = ext_id
            if ext_id_norm in seen:
                continue
            seen.add(ext_id_norm)

            row_dict = row.to_dict()
            comp, season, t_name, t_id = _record_context(row_dict, meta)

            try:
                cid = resolve_player(
                    conn, ext_name, source, source_id=ext_id_norm,
                    team_name=t_name, team_id=t_id,
                    competition=comp, season=season,
                )
            except Exception as e:
                log.warning("resolve_player(%s, %s): %s", source, ext_name, e)
                cid = None

            upsert_player_provenance(
                conn,
                source_system=source,
                source_player_id=ext_id_norm,
                scraped_name=ext_name,
                competition=comp,
                season=season,
                team_name=t_name,
                team_id=t_id,
                canonical_id=cid,
            )

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

    ensure_player_provenance_schema(conn)

    _load_phase1_transfermarkt(conn, comp_name)

    from utils.mdm_engine import clear_player_cache
    clear_player_cache()

    _load_phase2_sofascore(conn, comp_name)
    _load_phase3_understat(conn, comp_name)
    _load_phase4_statsbomb(conn, comp_name)
    _load_phase5_whoscored(conn, comp_name)

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
