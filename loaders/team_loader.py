"""
loaders/team_loader.py
=======================
Carga dim_team desde los CSV producidos por los scrapers en
`data/clean/<comp>/<season>/<source>/<table>.csv`.

FUENTES (en orden de prioridad):
    1. SofaScore teams.csv   → nombre canónico + id_sofascore  (MASTER)
    2. Transfermarkt players.csv → añade country + id_transfermarkt
    3. WhoScored teams.csv   → añade id_whoscored
    4. Understat teams.csv   → añade id_understat
    5. StatsBomb teams.csv   → añade id_statsbomb

Schema destino (dim_team):
    canonical_id, canonical_name, country,
    id_sofascore, id_understat, id_statsbomb, id_whoscored, id_transfermarkt
"""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text

from loaders.common import engine, safe_read_csv
from utils.canonical_teams import normalize_team_name
from utils.data_paths import iter_clean_csvs

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

# Filtro de competición que aplica a TODAS las llamadas a _read_clean dentro
# de la misma transacción. Se setea desde load_teams(conn, comp_name=...).
_active_comp_filter: list = [None]


def _read_clean(source: str, filename: str) -> list[pd.DataFrame]:
    """Lee todos los CSV `data/clean/[<comp>/]*/<source>/<filename>.csv`."""
    dfs: list[pd.DataFrame] = []
    comp = _active_comp_filter[0]
    for f in iter_clean_csvs(competition=comp, source=source, filename=filename):
        df = safe_read_csv(f)
        if df is None or df.empty:
            continue
        dfs.append(df)
    return dfs


def _load_from_sofascore(conn) -> int:
    """Upsert SofaScore teams.csv → fuente master del canonical_name."""
    dfs = _read_clean("sofascore", "teams")
    if not dfs:
        log.warning("team_loader: no se encontraron teams.csv de SofaScore")
        return 0

    count = 0
    seen: set[int] = set()
    for df in dfs:
        for row in df.to_dict("records"):
            sid  = row.get("id_sofascore")
            name = row.get("canonical_name")
            if not sid or not name:
                continue
            try:
                sid = int(sid)
            except (TypeError, ValueError):
                continue
            if sid in seen:
                continue
            seen.add(sid)
            try:
                _upsert_team(conn, name, "id_sofascore", sid)
                count += 1
            except Exception as e:
                log.error("Error processing team from SofaScore: %s", e)
    log.info("dim_team ← SofaScore: %d equipos", count)
    return count


def _load_from_transfermarkt(conn) -> int:
    """Añade country e id_transfermarkt a partir de TM `players.csv`."""
    dfs = _read_clean("transfermarkt", "players")
    if not dfs:
        log.info("team_loader: no hay players.csv de TM")
        return 0

    team_rows: dict[str, dict] = {}
    for df in dfs:
        for _, row in df.iterrows():
            name    = row.get("team_name") or row.get("team_slug")
            country = row.get("team_country") if "team_country" in df.columns else None
            tm_id   = row.get("team_id_tm") or row.get("team_id")
            if name and name not in team_rows:
                team_rows[name] = {"country": country, "team_id_tm": tm_id}

    count = 0
    for name, info in team_rows.items():
        cid = _upsert_team(conn, name, "id_transfermarkt", info.get("team_id_tm"))
        if info.get("country"):
            conn.execute(
                text("UPDATE dim_team SET country = COALESCE(country, :c) "
                     "WHERE canonical_id = :cid"),
                {"c": info["country"], "cid": cid},
            )
        count += 1
    log.info("dim_team ← Transfermarkt: %d equipos enriquecidos", count)
    return count


def _load_from_understat(conn) -> int:
    """Añade id_understat desde `teams.csv` de Understat."""
    dfs = _read_clean("understat", "teams")
    if not dfs:
        log.info("team_loader: no hay teams.csv de Understat")
        return 0

    count = 0
    for df in dfs:
        for _, row in df.iterrows():
            us_id   = row.get("understat_team_id") or row.get("id_understat")
            us_name = row.get("team_name") or row.get("name") or row.get("canonical_name")
            if not us_id or not us_name:
                continue
            _upsert_team(conn, us_name, "id_understat", us_id)
            count += 1
    log.info("dim_team ← Understat: %d equipos", count)
    return count


def _load_from_statsbomb(conn) -> int:
    """Añade id_statsbomb desde `teams.csv` de StatsBomb."""
    dfs = _read_clean("statsbomb", "teams")
    if not dfs:
        log.info("team_loader: no hay teams.csv de StatsBomb")
        return 0

    count = 0
    for df in dfs:
        for _, row in df.iterrows():
            sb_id   = row.get("id_statsbomb")
            sb_name = row.get("canonical_name") or row.get("team_name")
            if not sb_id or not sb_name:
                continue
            _upsert_team(conn, sb_name, "id_statsbomb", sb_id)
            count += 1
    log.info("dim_team ← StatsBomb: %d equipos", count)
    return count


def _load_from_whoscored(conn) -> int:
    """Añade id_whoscored desde `teams.csv` de WhoScored."""
    dfs = _read_clean("whoscored", "teams")
    if not dfs:
        log.info("team_loader: no hay teams.csv de WhoScored")
        return 0

    count = 0
    for df in dfs:
        for _, row in df.iterrows():
            ws_id   = row.get("whoscored_team_id") or row.get("id_whoscored")
            ws_name = row.get("team_name") or row.get("name") or row.get("canonical_name")
            if not ws_id or not ws_name:
                continue
            _upsert_team(conn, ws_name, "id_whoscored", ws_id)
            count += 1
    log.info("dim_team ← WhoScored: %d equipos", count)
    return count


# ── Punto de entrada ──────────────────────────────────────────────────────────

def load_teams(conn, comp_name: str | None = None) -> int:
    """Carga y enriquece dim_team desde todas las fuentes.

    Args:
        conn: SQLAlchemy connection.
        comp_name: si se especifica, restringe la búsqueda a
            `data/clean/<comp_slug>/...`. Por defecto procesa todas las
            competiciones encontradas en disco.

    Orden:
        1. SofaScore  → canonical_name e id_sofascore  (MASTER)
        2. Transfermarkt → country e id_transfermarkt
        3. WhoScored  → id_whoscored
        4. Understat  → id_understat
        5. StatsBomb  → id_statsbomb
    """
    log.info("[START] Cargando dim_team... (comp=%s)", comp_name or "todas")
    _active_comp_filter[0] = comp_name
    try:
        total = 0
        total += _load_from_sofascore(conn)
        total += _load_from_transfermarkt(conn)
        total += _load_from_whoscored(conn)
        total += _load_from_understat(conn)
        total += _load_from_statsbomb(conn)
    finally:
        _active_comp_filter[0] = None
    log.info("[OK] dim_team completado — %d registros procesados", total)
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_teams(conn)
