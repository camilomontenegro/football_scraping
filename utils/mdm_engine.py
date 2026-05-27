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

    # ── Diccionario de Diminutivos Comunes ──
    diminutives = {
        r'\bfran\b': 'francisco',
        r'\bpaco\b': 'francisco',
        r'\bdani\b': 'daniel',
        r'\balex\b': 'alejandro',
        r'\bleo\b': 'lionel',
        r'\bnico\b': 'nicolas',
        r'\bmaxi\b': 'maximiliano',
        r'\bfede\b': 'federico',
        r'\bpepe\b': 'jose',
        r'\bjuanma\b': 'juan manuel',
        r'\bmanu\b': 'manuel',
        r'\brafa\b': 'rafael',
        r'\bgabi\b': 'gabriel',
        r'\bfer\b': 'fernando',
        r'\bjavi\b': 'javier',
        r'\bnacho\b': 'ignacio',
        r'\bandy\b': 'andrew',
        r'\brob\b': 'robert',
    }
    for dim, full in diminutives.items():
        name = re.sub(dim, full, name)

    # ── Diccionario Manual de Alias Especiales (Fútbol) ──
    ALIASES = {
        # Jugadores españoles comunes
        "papakouli diop": "pape diop",
        "joselu": "jose luis mato",
        "koke": "jorge resurreccion",
        "isco": "francisco alarcon",
        "pedri": "pedro gonzalez",
        "gavi": "pablo martin paez gavira",
        "rodri": "rodrigo hernandez",
        "vini jr": "vinicius junior",
        "pepe": "kepler laveran",
        "xavi": "xavier hernandez",
        "busquets": "sergio busquets",
        "pique": "gerard pique",
        "villa": "david villa",
        "torres": "fernando torres",
        "ramos": "sergio ramos",
        "alba": "jordi alba",
        "suarez": "luis suarez",
        "benzema": "karim benzema",
        "ronaldo": "cristiano ronaldo",
        "messi": "lionel messi",
        # Jugadores internacionales
        "gazza": "paul gascoigne",
        "pele": "pele",
        "ronaldinho": "ronaldinho gaucho",
        "neymar": "neymar santos",
        "cr7": "cristiano ronaldo",
        "cr 7": "cristiano ronaldo",
    }
    return ALIASES.get(name, name) or None


def _similarity_score(a: str, b: str) -> int:
    """Puntúa la similitud entre dos strings normalizados (0-100).

    Usa múltiples estrategias:
    1. Token set ratio (RapidFuzz) - permite orden diferente
    2. Coincidencia de palabras completas (subconjuntos)
    3. Análisis estructural (nombre + apellido)
    4. Jaccard Index
    5. Levenshtein (typos)

    Versión mejorada que reduce falsos negativos.
    """
    if not a or not b:
        return 0

    try:
        from rapidfuzz import fuzz
    except ImportError:
        # Fallback si no está instalado
        from difflib import SequenceMatcher
        return int(SequenceMatcher(None, a, b).ratio() * 100)

    words_a = set(a.split())
    words_b = set(b.split())
    list_a = a.split()
    list_b = b.split()

    if not words_a or not words_b:
        return 0

    # ESTRATEGIA 1: Token Set Ratio (permite orden diferente)
    token_set_score = fuzz.token_set_ratio(a, b)

    # ESTRATEGIA 2: Coincidencia de palabras (subconjuntos)
    intersection = words_a & words_b
    union = words_a | words_b

    subset_score = 0
    if len(intersection) == len(words_a) or len(intersection) == len(words_b):
        if len(intersection) >= 2:
            subset_score = 95
        elif len(words_a) == 1 or len(words_b) == 1:
            subset_score = 70

    # ESTRATEGIA 3: Análisis estructural (nombre + apellido)
    structural_score = 0
    if len(list_a) >= 2 and len(list_b) >= 2:
        last_a = list_a[-1]
        last_b = list_b[-1]
        first_a = list_a[0]
        first_b = list_b[0]

        if last_a == last_b:
            structural_score += 50
            if first_a == first_b:
                structural_score += 45  # Total: 95 (exacto)
            elif first_a.startswith(first_b) or first_b.startswith(first_a):
                structural_score += 38  # Total: 88 (similar)
            elif first_a[0] == first_b[0]:
                if len(first_a) == 1 or len(first_b) == 1:
                    structural_score += 38  # Total: 88 (inicial)
                else:
                    structural_score += 15  # Total: 65 (duda)
        else:
            # Comprobar doble apellido (común en español)
            if len(list_a) >= 3 and list_a[-2] == last_b:
                structural_score = 75
            elif len(list_b) >= 3 and list_b[-2] == last_a:
                structural_score = 75

    # ESTRATEGIA 4: Jaccard Index
    jaccard_score = int(100 * len(intersection) / len(union))

    # ESTRATEGIA 5: Levenshtein para typos menores
    levenshtein_score = 0
    if jaccard_score > 30 or a[0] == b[0]:
        from difflib import SequenceMatcher
        seq_ratio = SequenceMatcher(None, a, b).ratio()
        levenshtein_score = int(seq_ratio * 100)
        if levenshtein_score < 75:
            levenshtein_score = 0

    # Combinar todas las estrategias (máximo de todas)
    result_score = max(
        token_set_score,
        subset_score,
        structural_score,
        jaccard_score,
        levenshtein_score,
    )

    # Cortafuegos FLEXIBLE por Apellido Diferente
    if len(list_a) >= 2 and len(list_b) >= 2:
        last_a = list_a[-1]
        last_b = list_b[-1]
        apellidos_coinciden = (last_a == last_b)

        if not apellidos_coinciden:
            # Comprobar doble apellido
            if len(list_a) >= 3 and list_a[-2] == last_b:
                apellidos_coinciden = True
            elif len(list_b) >= 3 and list_b[-2] == last_a:
                apellidos_coinciden = True

        if not apellidos_coinciden:
            if result_score >= 80 and len(intersection) >= 2:
                result_score = min(result_score, 78)
            elif result_score >= 70 and len(intersection) >= 2:
                result_score = min(result_score, 65)
            else:
                result_score = min(result_score, 50)

    return result_score


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


# ── Caché en memoria para JUGADORES ──────────────────────────────────────────

_PLAYER_CACHE = None

def clear_player_cache():
    """Limpia la caché de jugadores (útil después de ingestas masivas)."""
    global _PLAYER_CACHE
    _PLAYER_CACHE = None

def _get_player_cache(conn) -> list[dict]:
    """Obtiene y construye la caché de jugadores si no existe."""
    global _PLAYER_CACHE
    if _PLAYER_CACHE is None:
        log.info("Construyendo caché de jugadores para resolución sin tildes...")
        rows = conn.execute(text("SELECT canonical_id, canonical_name FROM dim_player")).fetchall()
        _PLAYER_CACHE = []
        for cid, cname in rows:
            norm = normalize(cname)
            if norm:
                _PLAYER_CACHE.append({
                    "id": cid,
                    "name": cname,
                    "norm": norm
                })
        log.info("Caché construida: %d jugadores", len(_PLAYER_CACHE))
    return _PLAYER_CACHE


# ── Resolución de JUGADORES ──────────────────────────────────────────────────

def resolve_player(
    conn,
    player_name: str,
    source: str,
    source_id: Optional[int] = None,
    similarity_threshold: int = 85,
    team_name: Optional[str] = None,
    team_id: Optional[str] = None,
    competition: Optional[str] = None,
    season: Optional[str] = None,
) -> Optional[int]:
    """Resuelve un nombre de jugador a un canonical_id de dim_player.

    Estrategia:
        1. Búsqueda por ID externo → match definitivo
        2. Búsqueda por nombre exacto (normalizado) en caché
        3. Búsqueda fuzzy → insertar en player_review si similitud ≥ threshold
        4. Sin match → insertar en player_review para revisión manual

    Args:
        conn:                 Conexión SQLAlchemy activa.
        player_name:          Nombre del jugador tal como viene de la fuente.
        source:               Fuente de datos ('sofascore', 'transfermarkt', etc.).
        source_id:            ID del jugador en la fuente (si se conoce).
        similarity_threshold: Mínimo de similitud (0-100) para match fuzzy.
        team_name:            Nombre del equipo (para contexto en player_review).
        team_id:              ID del equipo en la fuente (para contexto en player_review).
        competition:          Competición del scrape (para contexto en player_review).
        season:               Temporada del scrape (para contexto en player_review).

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

    # 2. Búsqueda por nombre exacto normalizado en caché
    norm = normalize(player_name)
    if not norm:
        log.warning("resolve_player: nombre vacío/inválido '%s'", player_name)
        return None

    cache = _get_player_cache(conn)

    exact_match_id = None
    for p in cache:
        if p["norm"] == norm:
            exact_match_id = p["id"]
            break

    if exact_match_id:
        # Actualizar ID externo si no estaba registrado
        if source_id is not None and id_col:
            conn.execute(
                text(f"UPDATE dim_player SET {id_col} = :sid WHERE canonical_id = :cid AND {id_col} IS NULL"),
                {"sid": source_id, "cid": exact_match_id},
            )
        return exact_match_id

    # 3. Búsqueda fuzzy: comparar contra todos los jugadores en caché
    best_score = 0
    best_id    = None
    
    for p in cache:
        score = _similarity_score(norm, p["norm"])
        if score > best_score:
            best_score = score
            best_id    = p["id"]

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
        team_name  = team_name,
        team_id    = str(team_id) if team_id else None,
        competition= competition,
        season     = season,
    )
    return None


def _queue_player_review(
    conn,
    source_name: str,
    source: str,
    source_id: Optional[str],
    suggested_id: Optional[int],
    score: int,
    team_name: Optional[str] = None,
    team_id: Optional[str] = None,
    competition: Optional[str] = None,
    season: Optional[str] = None,
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
                     suggested_canonical_id, similarity_score, resolved,
                     source_team_id, source_team_name, competition, season)
                SELECT :name, :sys, :sid, :sugg, :score, FALSE,
                       :tid, :tname, :comp, :season
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
                "tid":   team_id,
                "tname": team_name,
                "comp":  competition,
                "season": season,
            },
        )
        log.debug(
            "player_review: '%s' (%s id=%s, %s %s, team=%s) → suggested=%s score=%d",
            source_name, source, source_id, competition, season, team_name, suggested_id, score,
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