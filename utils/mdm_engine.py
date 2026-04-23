"""
utils/mdm_engine.py
====================
Motor de resolución de entidades (MDM - Master Data Management).

Funciones principales:
    resolve_team(conn, raw_name, source, source_id=None) → int | None
    resolve_player(conn, player_name, source, source_id=None) → int | None

Estrategia de resolución:

    EQUIPOS (dim_team):
        1. Si source_id  → buscar por dim_team.id_{source} (match exacto)
        2. normalizar nombre con canonical_teams.normalize_team_name()
        3. Buscar por LOWER(canonical_name)
        4. Si no existe  → crear dim_team nueva
        5. Actualizar dim_team.id_{source} si era NULL

    JUGADORES (dim_player):
        1. Si source_id  → buscar por dim_player.id_{source} (match exacto)
        2. Buscar por LOWER(canonical_name) exacto
        3. Match exacto  → devolver canonical_id, actualizar id_{source}
        4. Match fuzzy   → insertar en player_review (resolved=False)
        5. Sin match     → insertar en player_review para revisión manual

IMPORTANTE:
    - dim_team NO tiene tabla de alias (se usa canonical_teams.py)
    - dim_player usa player_review para la desambiguación
    - No hay staging tables ni alias tables en el schema actual
"""

from __future__ import annotations

import logging
import re
import unicodedata
from typing import Optional

from sqlalchemy import text

from utils.canonical_teams import normalize_team_name
from utils.mdm_config import SOURCE_ID_FIELDS, DIM_PK, DIM_TABLE

log = logging.getLogger(__name__)


# ── Normalización interna ────────────────────────────────────────────────────

def normalize(name: str) -> Optional[str]:
    """Normaliza un nombre para comparaciones:
    minúsculas · sin tildes · solo letras/dígitos/espacios · espacios simples.

    Devuelve None si el resultado está vacío o es un placeholder ('home', 'away').
    """
    if not name:
        return None
    name = name.lower().strip()
    if name in ("home", "away", ""):
        return None
    # Eliminar diacríticos
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Solo letras, dígitos y espacios
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or None


def _similarity_score(a: str, b: str) -> int:
    """Puntúa la similitud entre dos strings normalizados (0-100).

    Algoritmo simple basado en palabras compartidas, suficiente para
    nombres de jugadores donde la coincidencia suele ser alta o baja.
    """
    if not a or not b:
        return 0
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return 0
    intersection = words_a & words_b
    union        = words_a | words_b
    # Jaccard index * 100
    return int(100 * len(intersection) / len(union))


# ── Resolución de EQUIPOS ────────────────────────────────────────────────────

def resolve_team(
    conn,
    raw_name: str,
    source: str,
    source_id: Optional[int] = None,
) -> Optional[int]:
    """Resuelve un nombre de equipo a un canonical_id de dim_team.

    Args:
        conn:       Conexión SQLAlchemy activa.
        raw_name:   Nombre del equipo tal como viene de la fuente.
        source:     Fuente de datos ('sofascore', 'transfermarkt', etc.).
        source_id:  ID del equipo en la fuente (si se conoce).

    Returns:
        canonical_id de dim_team, o None si no se puede resolver.
    """
    id_col = SOURCE_ID_FIELDS.get(source, {}).get("team")

    # 1. Búsqueda por ID de fuente (más fiable)
    if source_id is not None and id_col:
        row = conn.execute(
            text(f"SELECT canonical_id FROM dim_team WHERE {id_col} = :sid LIMIT 1"),
            {"sid": source_id},
        ).fetchone()
        if row:
            return row[0]

    # 2. Normalizar nombre con el diccionario canónico
    canonical_name = normalize_team_name(raw_name)
    if not canonical_name:
        log.warning("resolve_team: nombre vacío/inválido para '%s'", raw_name)
        return None

    # 3. Buscar por canonical_name en dim_team
    #    Usamos canonical_name.lower() — mantiene tildes igual que LOWER() en PostgreSQL
    row = conn.execute(
        text("SELECT canonical_id FROM dim_team WHERE LOWER(canonical_name) = :n LIMIT 1"),
        {"n": canonical_name.lower()},
    ).fetchone()

    if row:
        canonical_id = row[0]
        # Actualizar ID externo si no estaba registrado
        if source_id is not None and id_col:
            conn.execute(
                text(f"UPDATE dim_team SET {id_col} = :sid WHERE canonical_id = :cid AND {id_col} IS NULL"),
                {"sid": source_id, "cid": canonical_id},
            )
        return canonical_id

    # 4. No existe → crear entrada nueva en dim_team
    canonical_id = conn.execute(
        text("""
            INSERT INTO dim_team (canonical_name)
            VALUES (:name)
            RETURNING canonical_id
        """),
        {"name": canonical_name},
    ).scalar()

    log.info("resolve_team: creado nuevo equipo '%s' (canonical_id=%d)", canonical_name, canonical_id)

    # Guardar ID externo de la fuente
    if source_id is not None and id_col:
        conn.execute(
            text(f"UPDATE dim_team SET {id_col} = :sid WHERE canonical_id = :cid"),
            {"sid": source_id, "cid": canonical_id},
        )

    return canonical_id


# ── Resolución de JUGADORES ──────────────────────────────────────────────────

def resolve_player(
    conn,
    player_name: str,
    source: str,
    source_id: Optional[int] = None,
    similarity_threshold: int = 85,
) -> Optional[int]:
    """Resuelve un nombre de jugador a un canonical_id de dim_player.

    Estrategia:
        1. Búsqueda por ID externo → match definitivo
        2. Búsqueda por nombre exacto (normalizado)
        3. Búsqueda fuzzy → insertar en player_review si similitud ≥ threshold
        4. Sin match → insertar en player_review para revisión manual

    Args:
        conn:                 Conexión SQLAlchemy activa.
        player_name:          Nombre del jugador tal como viene de la fuente.
        source:               Fuente de datos ('sofascore', 'transfermarkt', etc.).
        source_id:            ID del jugador en la fuente (si se conoce).
        similarity_threshold: Mínimo de similitud (0-100) para match fuzzy.

    Returns:
        canonical_id de dim_player si se resuelve con certeza, None en caso contrario.
    """
    id_col = SOURCE_ID_FIELDS.get(source, {}).get("player")

    # 1. Búsqueda por ID de fuente
    if source_id is not None and id_col:
        row = conn.execute(
            text(f"SELECT canonical_id FROM dim_player WHERE {id_col} = :sid LIMIT 1"),
            {"sid": source_id},
        ).fetchone()
        if row:
            return row[0]

    # 2. Búsqueda por nombre exacto normalizado
    norm = normalize(player_name)
    if not norm:
        log.warning("resolve_player: nombre vacío/inválido '%s'", player_name)
        return None

    row = conn.execute(
        text("""
            SELECT canonical_id
            FROM dim_player
            WHERE LOWER(canonical_name) = :n
            LIMIT 1
        """),
        {"n": norm},
    ).fetchone()

    if row:
        canonical_id = row[0]
        # Actualizar ID externo si no estaba registrado
        if source_id is not None and id_col:
            conn.execute(
                text(f"UPDATE dim_player SET {id_col} = :sid WHERE canonical_id = :cid AND {id_col} IS NULL"),
                {"sid": source_id, "cid": canonical_id},
            )
        return canonical_id

    # 3. Búsqueda fuzzy: comparar contra todos los jugadores de dim_player
    #    Para escalabilidad, se hace solo si hay pocos candidatos (nombre parcial)
    first_word = norm.split()[0] if norm.split() else norm
    candidates = conn.execute(
        text("""
            SELECT canonical_id, canonical_name
            FROM dim_player
            WHERE LOWER(canonical_name) LIKE :pattern
            LIMIT 20
        """),
        {"pattern": f"%{first_word}%"},
    ).fetchall()

    best_score = 0
    best_id    = None
    for cand_id, cand_name in candidates:
        score = _similarity_score(norm, normalize(cand_name) or "")
        if score > best_score:
            best_score = score
            best_id    = cand_id

    if best_id and best_score >= similarity_threshold:
        # Match fuzzy con suficiente confianza → actualizar ID si no estaba
        if source_id is not None and id_col:
            conn.execute(
                text(f"UPDATE dim_player SET {id_col} = :sid WHERE canonical_id = :cid AND {id_col} IS NULL"),
                {"sid": source_id, "cid": best_id},
            )
        return best_id

    # 4. Sin match o baja similitud → encolar en player_review
    _queue_player_review(
        conn       = conn,
        source_name= player_name,
        source     = source,
        source_id  = str(source_id) if source_id else None,
        suggested_id = best_id,
        score      = best_score,
    )
    return None


def _queue_player_review(
    conn,
    source_name: str,
    source: str,
    source_id: Optional[str],
    suggested_id: Optional[int],
    score: int,
) -> None:
    """Inserta un registro en player_review para desambiguación manual.

    Usa WHERE NOT EXISTS para evitar duplicados (player_review no tiene
    unique constraint, se usan los índices idx_player_review_source).
    """
    try:
        conn.execute(
            text("""
                INSERT INTO player_review
                    (source_name, source_system, source_id,
                     suggested_canonical_id, similarity_score, resolved)
                SELECT :name, :sys, :sid, :sugg, :score, FALSE
                WHERE NOT EXISTS (
                    SELECT 1 FROM player_review
                    WHERE source_system = :sys AND source_id = :sid
                )
            """),
            {
                "name":  source_name,
                "sys":   source,
                "sid":   source_id,
                "sugg":  suggested_id,
                "score": score,
            },
        )
        log.debug(
            "player_review: '%s' (%s id=%s) → suggested=%s score=%d",
            source_name, source, source_id, suggested_id, score,
        )
    except Exception as e:
        log.warning("Error insertando player_review para '%s': %s", source_name, e)


# ── Helpers públicos de compatibilidad ──────────────────────────────────────

def resolve(conn, entity: str, raw_name: str, source: str, source_id=None):
    """API de compatibilidad con el engine anterior.

    Prefer resolve_team() / resolve_player() en código nuevo.
    """
    if entity == "team":
        cid = resolve_team(conn, raw_name, source, source_id)
        return {"id": cid, "match_type": "resolved", "confidence": 90} if cid else None
    if entity == "player":
        cid = resolve_player(conn, raw_name, source, source_id)
        return {"id": cid, "match_type": "resolved", "confidence": 90} if cid else None
    return None