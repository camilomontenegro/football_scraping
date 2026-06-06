import argparse
import re
import sys
import time
import unicodedata
from pathlib import Path

from sqlalchemy import text

# Permitir importaciones relativas cuando el script se ejecuta desde la raíz del repo
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from scrapers.transfermarkt_scraper import get_player_profile, search_player_by_name
from utils.mdm_engine import _similarity_score, normalize


def slugify_name(name: str) -> str:
    """Convierte un nombre a un slug aproximado para Transfermarkt."""
    if not name:
        return ""
    value = unicodedata.normalize("NFKD", name)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().strip()
    value = re.sub(r"['’]", "", value)
    value = re.sub(r"[^a-z0-9\s-]", " ", value)
    value = re.sub(r"\s+", "-", value).strip("-")
    return value


def fetch_profile_with_fallbacks(player_id: str, canonical_name: str) -> dict:
    """Intenta obtener metadata de Transfermarkt usando slugs alternativos."""
    candidates = []
    slug = slugify_name(canonical_name)
    if slug:
        candidates.append(slug)
    candidates.append("spieler")
    candidates.append(str(player_id))

    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        profile = get_player_profile(candidate, player_id)
        if profile and (profile.get("nationality") or profile.get("birth_date")):
            return profile
        # Pequeña espera para evitar bloqueo excesivo si intentamos varias URLs
        time.sleep(1.0)
    return {"nationality": None, "birth_date": None}


def get_candidates(limit: int | None = None) -> list[dict]:
    sql = (
        "SELECT canonical_id, canonical_name, id_transfermarkt, position, nationality, birth_date "
        "FROM dim_player "
        "WHERE id_transfermarkt IS NULL OR position IS NULL OR nationality IS NULL OR birth_date IS NULL "
        "ORDER BY canonical_id"
    )
    if limit:
        sql += " LIMIT :limit"
    query = text(sql)
    with engine.connect() as conn:
        rows = conn.execute(query, {"limit": limit} if limit else {}).fetchall()
    return [dict(row._mapping) for row in rows]


def update_player_metadata(canonical_id: int, updates: dict) -> None:
    set_parts = []
    params = {"cid": canonical_id}
    if updates.get("id_transfermarkt") is not None:
        set_parts.append("id_transfermarkt = :id_transfermarkt")
        params["id_transfermarkt"] = updates["id_transfermarkt"]
    if updates.get("nationality") is not None:
        set_parts.append("nationality = :nationality")
        params["nationality"] = updates["nationality"]
    if updates.get("birth_date") is not None:
        set_parts.append("birth_date = :birth_date")
        params["birth_date"] = updates["birth_date"]
    if not set_parts:
        return

    sql = f"UPDATE dim_player SET {', '.join(set_parts)} WHERE canonical_id = :cid"
    with engine.begin() as conn:
        conn.execute(text(sql), params)


def normalize_search_name(name: str) -> str:
    value = unicodedata.normalize("NFKD", name or "")
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value).strip()
    return value


def find_transfermarkt_candidate(name: str, candidates: list[dict], threshold: int = 80) -> dict | None:
    if not candidates:
        return None
    normalized_name = normalize_search_name(name)
    
    # 1. Match exacto
    exact = [c for c in candidates if normalize_search_name(c["player_name"]) == normalized_name]
    if len(exact) == 1:
        return exact[0]
        
    # 2. Fuzzy matching usando el motor de MDM
    name_for_fuzzy = normalize(name) or normalized_name
    best_score = 0
    best_candidate = None
    
    for c in candidates:
        cand_norm = normalize(c["player_name"]) or normalize_search_name(c["player_name"])
        score = _similarity_score(name_for_fuzzy, cand_norm)
        if score > best_score:
            best_score = score
            best_candidate = c
            
    # Si supera el umbral de confianza, lo aceptamos
    if best_score >= threshold:
        print(f"  -> [FUZZY MATCH] '{name}' coincidi\u00f3 con '{best_candidate['player_name']}' (Score: {best_score}%)")
        return best_candidate
        
    # 3. Fallback: Si solo hay un resultado en TM y no hab\u00eda exacto, lo tomamos
    if len(candidates) == 1:
        print(f"  -> [SINGLE RESULT] Aceptando '{candidates[0]['player_name']}' por ser el \u00fanico resultado devuelto por TM.")
        return candidates[0]
        
    print(f"  -> [AMBIGUO] M\u00faltiples resultados y ninguno super\u00f3 el {threshold}% (Mejor: {best_score}% - {best_candidate['player_name'] if best_candidate else 'N/A'})")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Completa metadata de dim_player usando Transfermarkt cuando falta id_transfermarkt, nationality o birth_date."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Número máximo de jugadores a procesar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No actualiza la base de datos, solo muestra qué se podría completar.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.5,
        help="Segundos a esperar entre peticiones a Transfermarkt.",
    )

    args = parser.parse_args()

    candidates = get_candidates(limit=args.limit)
    if not candidates:
        print("No hay jugadores con metadata faltante o sin id_transfermarkt en dim_player.")
        return

    print(f"Procesando {len(candidates)} jugadores con id_transfermarkt y/o metadata incompleta...\n")
    total_updated = 0
    total_skipped = 0
    total_errors = 0

    for idx, row in enumerate(candidates, start=1):
        canonical_id = row["canonical_id"]
        canonical_name = row["canonical_name"]
        player_id_tm = str(row["id_transfermarkt"]) if row["id_transfermarkt"] else "missing"
        missing_fields = []
        if row["id_transfermarkt"] is None:
            missing_fields.append("id_transfermarkt")
        if row["nationality"] is None:
            missing_fields.append("nationality")
        if row["birth_date"] is None:
            missing_fields.append("birth_date")

        print(f"[{idx}/{len(candidates)}] {canonical_name} (TM {player_id_tm}) - faltan: {', '.join(missing_fields)}")

        try:
            profile = None
            candidate = None
            if row["id_transfermarkt"] is None:
                search_results = search_player_by_name(canonical_name)
                candidate = find_transfermarkt_candidate(canonical_name, search_results)
                if candidate:
                    print(f"  -> Transfermarkt candidate encontrado: {candidate['player_name']} (TM {candidate['player_id']})")
                    profile = get_player_profile(candidate["player_slug"], candidate["player_id"])
                else:
                    print("  -> No se encontró un id_transfermarkt único o exacto por nombre.")
                    profile = {"nationality": None, "birth_date": None}
            else:
                profile = fetch_profile_with_fallbacks(player_id_tm, canonical_name)

            updates = {}
            if row["id_transfermarkt"] is None and candidate:
                updates["id_transfermarkt"] = int(candidate["player_id"])
            if row["nationality"] is None and profile.get("nationality"):
                updates["nationality"] = profile["nationality"]
            if row["birth_date"] is None and profile.get("birth_date"):
                updates["birth_date"] = profile["birth_date"]

            if not updates:
                print("  -> No se encontró metadata nueva en Transfermarkt.")
                total_skipped += 1
            else:
                print(f"  -> Metadata encontrada: {updates}")
                if not args.dry_run:
                    update_player_metadata(canonical_id, updates)
                total_updated += 1
        except Exception as exc:
            print(f"  ERROR: {exc}")
            total_errors += 1

        time.sleep(args.sleep)

    print("\nResumen:")
    print(f"  Actualizados: {total_updated}")
    print(f"  Sin cambios: {total_skipped}")
    print(f"  Errores: {total_errors}")


if __name__ == "__main__":
    main()
