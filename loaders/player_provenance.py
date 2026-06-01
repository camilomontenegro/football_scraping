"""
loaders/player_provenance.py
==============================
Registra de dónde salió cada jugador en cada scrape (competición, temporada, equipo).

Permite distinguir homónimos ("Pedro") y auditar filas pobres en dim_player.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from sqlalchemy import text


_SAVEPOINT_SAFE_RE = re.compile(r"[^A-Za-z0-9_]")


def _safe_savepoint_name(*parts: str) -> str:
    """Construye un nombre de SAVEPOINT que cumple la sintaxis SQL.

    PostgreSQL exige identifiers sin comillas con [A-Za-z_][A-Za-z0-9_$]*.
    Cualquier caracter fuera de [A-Za-z0-9_] se reemplaza por '_'. Trunca a
    50 chars dejando margen al prefijo. Usa byte-slicing tras ASCII-only
    para evitar partir un multibyte UTF-8.
    """
    joined = "_".join(str(p) for p in parts)
    cleaned = _SAVEPOINT_SAFE_RE.sub("_", joined)
    cleaned = cleaned.strip("_")
    if not cleaned or not cleaned[0].isalpha():
        cleaned = "sp_" + cleaned
    return cleaned[:50]

log = logging.getLogger(__name__)

_SCHEMA_READY = False


def ensure_player_provenance_schema(conn) -> None:
    """Crea/actualiza tablas de trazabilidad si la BD es anterior al cambio."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return

    conn.execute(text("""
        ALTER TABLE player_review
            ADD COLUMN IF NOT EXISTS competition VARCHAR(100)
    """))
    conn.execute(text("""
        ALTER TABLE player_review
            ADD COLUMN IF NOT EXISTS season VARCHAR(20)
    """))
    conn.execute(text("""
        ALTER TABLE player_review
            ADD COLUMN IF NOT EXISTS source_team_id VARCHAR(50)
    """))
    conn.execute(text("""
        ALTER TABLE player_review
            ADD COLUMN IF NOT EXISTS source_team_name VARCHAR(150)
    """))
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS player_scrape_provenance (
            id               SERIAL PRIMARY KEY,
            source_system    VARCHAR(50)  NOT NULL,
            source_player_id VARCHAR(50)  NOT NULL,
            scraped_name     VARCHAR(150) NOT NULL,
            competition      VARCHAR(100) NOT NULL DEFAULT '',
            season           VARCHAR(20)  NOT NULL DEFAULT '',
            team_name        VARCHAR(150),
            team_id          VARCHAR(50)  NOT NULL DEFAULT '',
            canonical_id     INTEGER REFERENCES dim_player (canonical_id),
            scraped_at       TIMESTAMP DEFAULT NOW(),
            UNIQUE (source_system, source_player_id, competition, season, team_id)
        )
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_player_provenance_canonical
            ON player_scrape_provenance (canonical_id)
    """))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS idx_player_provenance_name
            ON player_scrape_provenance (LOWER(scraped_name))
    """))
    _SCHEMA_READY = True
    log.info("Schema player_scrape_provenance verificado")


def upsert_player_provenance(
    conn,
    *,
    source_system: str,
    source_player_id,
    scraped_name: str,
    competition: Optional[str] = None,
    season: Optional[str] = None,
    team_name: Optional[str] = None,
    team_id: Optional[str] = None,
    canonical_id: Optional[int] = None,
) -> None:
    """Inserta o actualiza un avistamiento de jugador en un scrape concreto."""
    if source_player_id is None or not scraped_name:
        return

    sid = str(source_player_id).strip()
    if not sid:
        return

    comp = (competition or "").strip()
    seas = (season or "").strip()
    tname = (team_name or "").strip() or None
    tid = str(team_id).strip() if team_id is not None and str(team_id).strip() else ""

    sp = _safe_savepoint_name("prov", source_system, sid)
    try:
        conn.execute(text(f"SAVEPOINT {sp}"))
        conn.execute(
            text("""
                INSERT INTO player_scrape_provenance
                    (source_system, source_player_id, scraped_name,
                     competition, season, team_name, team_id, canonical_id)
                VALUES
                    (:sys, :sid, :name, :comp, :season, :tname, :tid, :cid)
                ON CONFLICT (source_system, source_player_id, competition, season, team_id)
                DO UPDATE SET
                    scraped_name = EXCLUDED.scraped_name,
                    team_name    = COALESCE(EXCLUDED.team_name, player_scrape_provenance.team_name),
                    canonical_id = COALESCE(EXCLUDED.canonical_id, player_scrape_provenance.canonical_id),
                    scraped_at   = NOW()
            """),
            {
                "sys": source_system,
                "sid": sid,
                "name": scraped_name.strip(),
                "comp": comp,
                "season": seas,
                "tname": tname,
                "tid": tid,
                "cid": canonical_id,
            },
        )
        conn.execute(text(f"RELEASE SAVEPOINT {sp}"))
    except Exception as e:
        try:
            conn.execute(text(f"ROLLBACK TO SAVEPOINT {sp}"))
        except Exception:
            pass
        log.warning(
            "player_scrape_provenance(%s, %s, %s %s): %s",
            source_system, sid, comp, seas, e,
        )
