import re
import unicodedata
from sqlalchemy import text
from utils.mdm_config import ENTITY_CONFIG


# ─────────────────────────────
# NORMALIZER
# ─────────────────────────────

def normalize(name: str) -> str:
    if not name or name.lower().strip() in ['home', 'away', '']:
        return None

    # Normalización: todo a minúsculas, guiones a espacios, limpiar espacios extra
    name = name.lower().replace("-", " ").strip()
    
    # Excepciones geográficas/comunes (Bilbao -> Club)
    name = name.replace("bilbao", "club")
    
    # Descomposición de caracteres Unicode (para tildes)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    
    # Solo letras y espacios simples
    name = re.sub(r"[^a-z\s]", "", name)
    name = re.sub(r"\s+", " ", name)

    # Ordenar palabras para que 'ud levante' == 'levante ud'
    words = sorted(name.strip().split())
    return " ".join(words)


# ─────────────────────────────
# ALIASES
# ─────────────────────────────

def generate_aliases(name: str):
    norm = normalize(name)
    if not norm:
        return []

    parts = norm.split()

    aliases = {norm, norm.replace(" ", "")}

    if len(parts) >= 2:
        aliases.add(f"{parts[0]} {parts[-1]}")
        aliases.add(parts[-1])

    return list(aliases)


def save_aliases(conn, entity, entity_id, raw_name, source):
    cfg = ENTITY_CONFIG[entity]

    for alias in generate_aliases(raw_name):
        conn.execute(text(f"""
            INSERT INTO {cfg['alias_table']} ({cfg['alias_id_field']}, alias_name, source)
            VALUES (:id, :alias, :source)
            ON CONFLICT DO NOTHING
        """), {
            "id": entity_id,
            "alias": alias,
            "source": source
        })


# ─────────────────────────────
# CORE RESOLVER (FIXED)
# ─────────────────────────────

def resolve(conn, entity: str, raw_name: str, source: str, log=False):
    cfg = ENTITY_CONFIG[entity]

    norm = normalize(raw_name)
    if not norm:
        return None

    # 1. alias exact
    result = conn.execute(text(f"""
        SELECT {cfg['alias_id_field']}
        FROM {cfg['alias_table']}
        WHERE alias_name = :name
        LIMIT 1
    """), {"name": norm}).fetchone()

    if result:
        return {
            "id": result[0],
            "match_type": "alias_exact",
            "confidence": 100
        }

    # 2. alias fuzzy
    result = conn.execute(text(f"""
        SELECT {cfg['alias_id_field']}
        FROM {cfg['alias_table']}
        WHERE alias_name LIKE :pattern
        LIMIT 1
    """), {"pattern": f"%{norm}%"}).fetchone()

    if result:
        entity_id = result[0]
        save_aliases(conn, entity, entity_id, raw_name, source)
        return {
            "id": entity_id,
            "match_type": "alias_fuzzy",
            "confidence": 80
        }

    # 3. dim exact
    result = conn.execute(text(f"""
        SELECT {cfg['id_field']}
        FROM {cfg['dim_table']}
        WHERE LOWER({cfg['name_field']}) = :name
        LIMIT 1
    """), {"name": raw_name.lower()}).fetchone()

    if result:
        return {
            "id": result[0],
            "match_type": "dim_exact",
            "confidence": 95
        }

    # 4. create new
    entity_id = conn.execute(text(f"""
        INSERT INTO {cfg['dim_table']} ({cfg['name_field']})
        VALUES (:name)
        RETURNING {cfg['id_field']}
    """), {"name": raw_name}).scalar()

    save_aliases(conn, entity, entity_id, raw_name, source)

    return {
        "id": entity_id,
        "match_type": "created",
        "confidence": 50
    }