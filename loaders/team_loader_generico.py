"""
loaders/team_loader.py
=======================
Carga dim_team desde los archivos producidos por los scrapers.

FUENTES (en orden de prioridad):
    1. SofaScore teams.csv  → nombre canónico + id_sofascore  (MASTER)
    2. Transfermarkt players_clean.csv → añade country + id_transfermarkt
    3. Understat teams CSV  → añade id_understat
    4. StatsBomb teams CSV  → añade id_statsbomb

Jerarquía:
    - SofaScore establece el canonical_name definitivo de cada equipo.
    - Las demás fuentes enriquecen la fila con sus IDs externos y country.

Schema destino (dim_team):
    canonical_id, canonical_name, country,
    id_sofascore, id_understat, id_statsbomb, id_whoscored, id_transfermarkt
"""

from __future__ import annotations

import glob
import logging
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from loaders.common import engine
from utils.canonical_teams import normalize_team_name

log = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────

# Allowed column names to prevent SQL injection
_ALLOWED_ID_COLS = {
    "id_sofascore", "id_understat", "id_statsbomb", 
    "id_whoscored", "id_transfermarkt"
}

def _upsert_team(conn, canonical_name: str, source_id_col: str, source_id) -> int:
    """Inserta o actualiza un equipo en dim_team.

    Returns:
        canonical_id del equipo.
    """
    # Validate column name to prevent SQL injection
    if source_id_col not in _ALLOWED_ID_COLS:
        raise ValueError(f"Invalid source_id_col: {source_id_col}")

    # 1. Normalizar el nombre SIEMPRE antes de cualquier operación
    canonical_name = normalize_team_name(canonical_name)
    norm = canonical_name.lower().strip()
    
    # 2. Intentar match por ID de fuente (si se proporcionó)
    if source_id is not None:
        try:
            row = conn.execute(
                text(f"SELECT canonical_id FROM dim_team WHERE {source_id_col} = :sid LIMIT 1"),
                {"sid": int(source_id)},
            ).fetchone()
            if row:
                return row[0]
        except Exception as e:
            log.warning("Error looking up team by %s=%s: %s", source_id_col, source_id, e)

    # 3. Intentar match por nombre normalizado
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n LIMIT 1"),
        {"n": norm},
    ).fetchone()

    if row:
        cid = row[0]
    else:
        # Crear nuevo equipo con el nombre canónico ya normalizado
        cid = conn.execute(
            text("INSERT INTO dim_team (canonical_name) VALUES (:name) RETURNING canonical_id"),
            {"name": canonical_name},
        ).scalar()

    # Actualizar ID externo si se proporcionó
    if source_id is not None:
        try:
            conn.execute(
                text(f"UPDATE dim_team SET {source_id_col} = :sid WHERE canonical_id = :cid AND {source_id_col} IS NULL"),
                {"sid": int(source_id), "cid": cid},
            )
        except Exception as e:
            log.warning("Error updating team %d with %s=%s: %s", cid, source_id_col, source_id, e)

    return cid


# ── Carga por fuente ─────────────────────────────────────────────────────────

def _load_from_sofascore(conn, ss_path: Path) -> int:
    """Lee teams.csv de SofaScore → upsert en dim_team como fuente master."""

    files = list(ss_path.glob("**/teams.csv"))
    
    if not files:
        log.warning("team_loader: no se encontraron teams.csv en %s", ss_path)
        return 0

    count = 0
    all_rows: list[dict] = []
    for f in files:
        try:
            df = pd.read_csv(f)
            all_rows.extend(df.to_dict("records"))
        except Exception as e:
            log.error("Error reading teams file %s: %s", f, e)
            continue

    # Deduplicar por id_sofascore
    seen: set[int] = set()
    for row in all_rows:
        try:
            sid  = row.get("id_sofascore")
            name = row.get("canonical_name")
            if not sid or not name:
                continue
            sid = int(sid)
            if sid in seen:
                continue
            seen.add(sid)
            _upsert_team(conn, name, "id_sofascore", sid)
            count += 1
        except Exception as e:
            log.error("Error processing team from SofaScore: %s", e)
            continue

    log.info("dim_team ← SofaScore: %d equipos", count)
    return count


def _load_from_transfermarkt(conn,tm_path: Path) -> int:
    """Lee players_clean.csv de TM → añade country e id_transfermarkt a dim_team."""
    
    files = list(tm_path.glob("**/teams_clean.csv"))

    if not files:
        log.info("team_loader: no hay players_clean.csv de TM")
        return 0

    # Construir tabla única de equipos TM (team_slug, team_country)
    team_rows: dict[str, dict] = {}
    for f in files:
        try:
            df = pd.read_csv(f)
            for _, row in df.iterrows():

                #slug    = row.get("team_slug")
                name= row.get("team_name")
                country = row.get("team_country") if "team_country" in df.columns else None
                
                #tm_id   = row.get("team_id_tm")
                tm_id = row.get("team_id")
                
                # cambio slug por name
                if name and name not in team_rows:
                    team_rows[name] = {"country": country, "team_id_tm": tm_id}

        except Exception as e:
            log.warning("Error leyendo %s: %s", f, e)

    count = 0
     # cambio slug por name 
    for name, info in team_rows.items():
        tm_id = info.get("team_id_tm")
       
        cid = _upsert_team(conn, name, "id_transfermarkt", tm_id)

        # Enriquecer con country si es necesario
        if info.get("country"):
            conn.execute(
                #-- con COALESCE — solo actualiza si es NULL
                text("UPDATE dim_team SET country = COALESCE(country, :c) WHERE canonical_id = :cid"),
                {"c": info["country"], "cid": cid}
            )
        count += 1

    log.info("dim_team ← Transfermarkt: %d equipos enriquecidos", count)
    return count


def _load_from_understat(conn, us_path: Path) -> int:
    """Lee understat_teams_laliga.csv → añade id_understat a dim_team."""
    f = us_path / "teams_clean.csv"
    
    
    if not f.exists():
        log.info("team_loader: no hay understat_teams_laliga.csv")
        return 0

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.warning("Error leyendo %s: %s", f, e)
        return 0

    count = 0
    for _, row in df.iterrows():
        us_id   = row.get("understat_team_id")
        us_name = row.get("team_name")
        if not us_id or not us_name:
            continue

        _upsert_team(conn, us_name, "id_understat", us_id)
        count += 1

    log.info("dim_team ← Understat: %d equipos", count)
    return count


def _load_from_statsbomb(conn, sb_path: Path) -> int:
    """Lee teams.csv de StatsBomb → añade id_statsbomb a dim_team."""
    files = list(sb_path.glob("**/teams.csv"))
    if not files:
        log.info("team_loader: no hay teams.csv de StatsBomb")
        return 0

    count = 0
    for fp in files:
        try:
            df = pd.read_csv(fp)
        except Exception as e:
            log.warning("Error leyendo %s: %s", fp, e)
            continue

        for _, row in df.iterrows():
            sb_id   = row.get("id_statsbomb")
            sb_name = row.get("canonical_name")
            if not sb_id or not sb_name:
                continue

            _upsert_team(conn, sb_name, "id_statsbomb", sb_id)
            count += 1

    log.info("dim_team ← StatsBomb: %d equipos", count)
    return count


def _load_from_whoscored(conn, ws_path: Path) -> int:

    """Lee whoscored_teams_laliga.csv → añade id_whoscored a dim_team."""
    f = ws_path / "teams_clean.csv"

    if not f.exists():
        log.info("team_loader: no hay whoscored_teams_laliga.csv")
        return 0

    try:
        df = pd.read_csv(f)
    except Exception as e:
        log.warning("Error leyendo %s: %s", f, e)
        return 0

    count = 0
    for _, row in df.iterrows():
        ws_id   = row.get("whoscored_team_id")
        ws_name = row.get("team_name")
        if not ws_id or not ws_name:
            continue

        _upsert_team(conn, ws_name, "id_whoscored", ws_id)
        count += 1

    log.info("dim_team ← WhoScored: %d equipos", count)
    return count


# ── Punto de entrada ──────────────────────────────────────────────────────────

def load_teams(
    conn,
    ss_path: Optional[Path] = None,
    tm_path: Optional[Path] = None,
    us_path: Optional[Path] = None,
    sb_path: Optional[Path] = None,
    ws_path: Optional[Path] = None,) -> int:

    """Carga y enriquece dim_team desde todas las fuentes.

    Orden:
        1. SofaScore  → establece canonical_name e id_sofascore  (MASTER)
        2. Transfermarkt → añade country e id_transfermarkt
        3. Understat  → añade id_understat
        4. StatsBomb  → añade id_statsbomb

    Returns:
        Número total de equipos procesados.
    """
    log.info("[START] Cargando dim_team...")
    total = 0
    if ss_path:
        total += _load_from_sofascore(conn, ss_path)
    if tm_path:
        total += _load_from_transfermarkt(conn, tm_path)
    if us_path:
        total += _load_from_understat(conn, us_path)
    if sb_path:
        total += _load_from_statsbomb(conn, sb_path)
    if ws_path:
        total += _load_from_whoscored(conn, ws_path)
    log.info("[OK] dim_team completado — %d registros procesados", total)
    return total


