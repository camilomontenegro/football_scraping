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

# Usar ruta absoluta basada en la carpeta del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_SS  = PROJECT_ROOT / "data" / "raw" / "sofascore"
RAW_TM  = PROJECT_ROOT / "data" / "raw" / "transfermarkt"
RAW_US  = PROJECT_ROOT / "data" / "raw" / "understat"
RAW_SB  = PROJECT_ROOT / "data" / "raw" / "statsbomb"
RAW_WS  = PROJECT_ROOT / "data" / "raw" / "whoscored"


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

def _load_from_sofascore(conn) -> int:
    """Lee teams.csv de SofaScore → upsert en dim_team como fuente master."""
    files = list(RAW_SS.glob("**/teams.csv"))
    if not files:
        log.warning("team_loader: no se encontraron teams.csv en %s", RAW_SS)
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


def _load_from_transfermarkt(conn) -> int:
    """Lee players_clean.csv de TM → añade country e id_transfermarkt a dim_team."""
    
    # Buscar cualquier archivo de equipos de Transfermarkt (ej. transfermarkt_teams.csv o players_clean.csv si contenía info)
    files = list(RAW_TM.glob("**/*teams*.csv")) + list(RAW_TM.glob("**/players_clean.csv"))

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
                text("UPDATE dim_team SET country = COALESCE(country, :c) WHERE canonical_id = :cid"),
                {"c": info["country"], "cid": cid}
            )
        count += 1

    log.info("dim_team ← Transfermarkt: %d equipos enriquecidos", count)
    return count


def _load_from_understat(conn) -> int:
    """Lee understat_teams_laliga.csv → añade id_understat a dim_team."""
    f = RAW_US / "understat_teams_laliga.csv"
    
    
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


def _load_from_statsbomb(conn) -> int:
    """Lee teams.csv de StatsBomb → añade id_statsbomb a dim_team."""
    files = list(RAW_SB.glob("**/teams.csv"))
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


def _load_from_whoscored(conn) -> int:

    # Buscar cualquier archivo de equipos de WhoScored
    files = list(RAW_WS.glob("**/*teams*.csv"))
    if not files:
        log.info("team_loader: no hay whoscored_teams_laliga.csv")
        return 0

    try:
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
    except Exception as e:
        log.warning("Error leyendo archivos de WhoScored: %s", e)
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

def load_teams(conn) -> int:
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
    total += _load_from_sofascore(conn)
    total += _load_from_transfermarkt(conn)
    total += _load_from_understat(conn)
    total += _load_from_statsbomb(conn)
    total += _load_from_whoscored(conn)
    log.info("[OK] dim_team completado — %d registros procesados", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_teams(conn)
