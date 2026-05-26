"""
scripts/reprocess_reviews.py
==============================
Script interactivo para resolver la cola de jugadores pendientes en player_review.

Modos de uso:
  python scripts/reprocess_reviews.py                      # Modo normal (interactivo completo)
  python scripts/reprocess_reviews.py --auto-only          # Solo procesa los casos automáticos (score < 50 o score >= 95)
  python scripts/reprocess_reviews.py --source sofascore   # Filtra solo una fuente concreta
  python scripts/reprocess_reviews.py --batch-auto-new     # Resuelve en masa los casos con score < 25 (claramente nuevos)
  python scripts/reprocess_reviews.py --source whoscored --auto-only  # Se pueden combinar
"""

import sys
import argparse
from pathlib import Path
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from loaders.common import engine
from utils.mdm_engine import _get_player_cache, _similarity_score, normalize
from utils.mdm_config import SOURCE_ID_FIELDS


# ── Helpers visuales ──────────────────────────────────────────────────────────

def make_progress_bar(current, total, width=25):
    if total <= 0:
        return "[" + "-" * width + "]"
    percent = current / total
    filled_len = int(round(width * percent))
    bar = "=" * filled_len + "-" * (width - filled_len)
    return f"[{bar}]"


def print_header(title):
    print(f"\n{'=' * 74}")
    print(f"  {title}")
    print(f"{'=' * 74}")


def print_progress(idx, total_rows, current_resolved, total_global, resolved_this_session, left_pending):
    global_pct = (current_resolved / total_global * 100) if total_global > 0 else 0.0
    bar_str = make_progress_bar(current_resolved, total_global, width=25)
    print(f"\n\033[96m{'=' * 74}\033[0m")
    print(f"\033[96m  PROGRESO COLA GLOBAL:  {current_resolved:,} / {total_global:,} {bar_str} ({global_pct:.2f}%)\033[0m")
    print(f"\033[96m  SESION: Jugador {idx} / {total_rows} ({idx/total_rows*100:.1f}%)  |  Resueltos: +{resolved_this_session}  |  Ignorados: {left_pending}\033[0m")
    print(f"\033[96m{'=' * 74}\033[0m")


# ── Funciones auxiliares mejoradas (V2) ───────────────────────────────────────

def validate_alias_applied(original_name, normalized_name):
    """Verifica si un alias fue aplicado durante la normalización.
    
    Un alias se considera aplicado si la normalización redujo significativamente
    el nombre (ej: 'Joselu' -> 'jose luis mato' → más caracteres pero más válido).
    """
    if not original_name or not normalized_name:
        return False
    
    # Si la versión normalizada es muy diferente, probablemente un alias fue aplicado
    return original_name.lower() not in normalized_name.lower()


def print_final_report(auto_linked, auto_created, left_pending, total_resolved_initial, total_global):
    """Imprime un reporte detallado del estado de resolución tras la sesión."""
    resolved_final = total_resolved_initial + auto_linked + auto_created
    pct_final = (resolved_final / total_global * 100) if total_global > 0 else 0
    pct_linked = (auto_linked / (auto_linked + auto_created) * 100) if (auto_linked + auto_created) > 0 else 0
    
    print("\n" + "=" * 74)
    print("  REPORTE FINAL DE SESION")
    print("=" * 74)
    print(f"  Casos procesados automáticamente:")
    print(f"    • Enlazados con existentes:  {auto_linked:,} ({pct_linked:.1f}%)")
    print(f"    • Creados como nuevos:       {auto_created:,} ({100 - pct_linked:.1f}%)")
    print(f"    • Total esta sesión:         {auto_linked + auto_created:,}")
    print(f"  ")
    print(f"  Estado global:")
    print(f"    • Resueltos ahora:           {resolved_final:,} / {total_global:,} ({pct_final:.2f}%)")
    print(f"    • Pendientes aun:            {left_pending:,}")
    print(f"  ")
    print(f"  Cambio en esta sesión:         +{auto_linked + auto_created:,} resueltos")
    print("=" * 74 + "\n")


# ── Modo BATCH-AUTO-NEW: resolución masiva vía SQL ───────────────────────────

def run_batch_auto_new(source_filter=None):
    """
    MEJORA 4: Resuelve en masa todos los registros con similarity_score < 25
    (claramente jugadores nuevos sin ninguna coincidencia) insertando en dim_player
    y marcando como resolved = TRUE, todo en una sola transacción por lote.
    """
    print_header("MODO BATCH-AUTO-NEW: Resolucion masiva de score < 25")

    where_source = "AND source_system = :src" if source_filter else ""
    params = {"src": source_filter} if source_filter else {}

    with engine.connect() as conn:
        total_candidates = conn.execute(text(f"""
            SELECT COUNT(*) FROM player_review
            WHERE resolved = FALSE
              AND (similarity_score < 25 OR similarity_score IS NULL)
              {where_source}
        """), params).scalar() or 0

    if total_candidates == 0:
        print("\nNo hay registros elegibles para batch-auto-new (score < 25).")
        return

    print(f"\n  Registros elegibles (score < 25): {total_candidates:,}")
    if source_filter:
        print(f"  Filtrando solo fuente: {source_filter}")
    print(f"\n  ATENCION: Esto insertara {total_candidates:,} nuevos jugadores en dim_player")
    print("  y marcara sus player_review como resolved = TRUE.")
    confirm = input("\n  Escriba 'si' para confirmar: ").strip().lower()
    if confirm != 'si':
        print("  Operacion cancelada.")
        return

    where_source_inner = "AND pr.source_system = :src" if source_filter else ""

    processed = 0
    errors = 0
    BATCH_SIZE = 200

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, source_name, source_system, source_id, similarity_score
            FROM player_review
            WHERE resolved = FALSE
              AND (similarity_score < 25 OR similarity_score IS NULL)
              {where_source}
            ORDER BY id
        """), params).fetchall()

    print(f"\n  Procesando {len(rows):,} registros en lotes de {BATCH_SIZE}...")

    for i, (rev_id, source_name, source_system, source_id, sim_score) in enumerate(rows, start=1):
        id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
        if not id_col:
            continue

        try:
            with engine.begin() as conn:
                # Verificar si el source_id ya está asignado (conflicto)
                existing = conn.execute(text(f"""
                    SELECT canonical_id FROM dim_player
                    WHERE {id_col} = :sid LIMIT 1
                """), {"sid": source_id}).fetchone()

                if existing:
                    # Conflicto: ignorar silenciosamente en batch mode
                    errors += 1
                    continue

                # Insertar nuevo jugador
                new_id = conn.execute(text(f"""
                    INSERT INTO dim_player (canonical_name, {id_col})
                    VALUES (:name, :sid)
                    RETURNING canonical_id
                """), {"name": source_name, "sid": source_id}).scalar()

                conn.execute(text("""
                    UPDATE player_review
                    SET resolved = TRUE, canonical_id_assigned = :cid,
                        similarity_score = :score, reviewed_at = NOW()
                    WHERE id = :rid
                """), {"cid": new_id, "score": sim_score or 0, "rid": rev_id})

            processed += 1

            if i % BATCH_SIZE == 0 or i == len(rows):
                pct = i / len(rows) * 100
                print(f"  [{i:,}/{len(rows):,} ({pct:.1f}%)] Procesados: {processed:,} | Conflictos omitidos: {errors}")

        except Exception as e:
            errors += 1

    print(f"\n  COMPLETADO:")
    print(f"    Nuevos jugadores creados:    {processed:,}")
    print(f"    Omitidos por conflicto de ID: {errors:,}")


# ── Lógica principal de resolución ───────────────────────────────────────────

def get_top_candidates(norm, cache, top_n=3):
    """
    MEJORA 2: Devuelve los N mejores candidatos del cache con sus scores.
    """
    scored = []
    for p in cache:
        score = _similarity_score(norm, p["norm"])
        if score > 0:
            scored.append((score, p["id"], p["name"]))
    scored.sort(key=lambda x: -x[0])
    return scored[:top_n]


def get_candidate_metadata(conn, canonical_id):
    """Carga metadata de dim_player para un candidato específico."""
    row = conn.execute(text("""
        SELECT canonical_id, position, nationality, birth_date,
               id_sofascore, id_understat, id_transfermarkt, id_statsbomb, id_whoscored
        FROM dim_player
        WHERE canonical_id = :cid
        LIMIT 1
    """), {"cid": canonical_id}).fetchone()
    return dict(row._mapping) if row else {}


def _last_name_from_normalized(norm_name):
    if not norm_name:
        return None
    parts = norm_name.split()
    return parts[-1] if parts else None


def metadata_confidence_bonus(candidate_meta, source_name=None, candidate_name=None):
    """Puntúa la confianza extra basada en la metadata disponible del jugador.
    
    Versión mejorada (V2) con bonificaciones más significativas.
    """
    if not candidate_meta:
        return 0

    bonus = 0
    
    # Metadata deportiva básica (más peso)
    if candidate_meta.get("position"):
        bonus += 10  # MEJORADO de 3
    if candidate_meta.get("nationality"):
        bonus += 10  # MEJORADO de 3
    if candidate_meta.get("birth_date"):
        bonus += 5   # MEJORADO de 2

    # Nuevo: Validación de edad razonable
    if candidate_meta.get("birth_date"):
        try:
            from datetime import datetime
            age = (datetime.now().date() - candidate_meta["birth_date"]).days / 365.25
            if 16 <= age <= 42:  # Edad de futbolista profesional razonable
                bonus += 5
        except:
            pass

    # IDs externos (más weight)
    external_ids = sum(
        bool(candidate_meta.get(col))
        for col in ("id_sofascore", "id_understat", "id_transfermarkt", "id_statsbomb", "id_whoscored")
    )
    if external_ids >= 3:
        bonus += 5  # MEJORADO de 2
    elif external_ids == 2:
        bonus += 3  # MEJORADO de 1
    elif external_ids == 1:
        bonus += 1

    # Apellido coincide
    if source_name and candidate_name:
        source_last = _last_name_from_normalized(normalize(source_name))
        candidate_last = _last_name_from_normalized(normalize(candidate_name))
        if source_last and candidate_last and source_last == candidate_last:
            bonus += 5  # MEJORADO de 2

    return min(bonus, 30)  # CAP MEJORADO de 12 a 30


def strong_candidate_metadata(candidate_meta, source_name, candidate_name):
    """Determina si la metadata es lo suficientemente sólida como para auto-aceptar el enlace."""
    if not candidate_meta:
        return False

    norm_source = normalize(source_name)
    norm_candidate = normalize(candidate_name)
    if not norm_source or not norm_candidate:
        return False

    source_parts = norm_source.split()
    candidate_parts = norm_candidate.split()
    if len(source_parts) < 2 or len(candidate_parts) < 2:
        return False

    if source_parts[-1] != candidate_parts[-1]:
        return False

    if not (candidate_meta.get("position") and candidate_meta.get("nationality") and candidate_meta.get("birth_date")):
        return False

    external_ids = sum(
        bool(candidate_meta.get(col))
        for col in ("id_sofascore", "id_understat", "id_transfermarkt", "id_statsbomb", "id_whoscored")
    )
    return external_ids >= 3


def link_player(rev_id, best_id, source_id, id_col, best_score, cache, norm, source_name):
    """Enlaza el registro de player_review con un jugador existente en dim_player."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE dim_player SET {id_col} = :sid
            WHERE canonical_id = :cid AND {id_col} IS NULL
        """), {"sid": source_id, "cid": best_id})

        conn.execute(text("""
            UPDATE player_review
            SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
            WHERE id = :rid
        """), {"cid": best_id, "score": best_score, "rid": rev_id})


def create_new_player(rev_id, source_name, source_id, id_col, best_score, cache, norm, with_external_id=True):
    """Inserta un nuevo jugador canónico y resuelve su player_review."""
    with engine.begin() as conn:
        if with_external_id:
            new_id = conn.execute(text(f"""
                INSERT INTO dim_player (canonical_name, {id_col})
                VALUES (:name, :sid)
                RETURNING canonical_id
            """), {"name": source_name, "sid": source_id}).scalar()
        else:
            new_id = conn.execute(text("""
                INSERT INTO dim_player (canonical_name)
                VALUES (:name)
                RETURNING canonical_id
            """), {"name": source_name}).scalar()

        conn.execute(text("""
            UPDATE player_review
            SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
            WHERE id = :rid
        """), {"cid": new_id, "score": best_score, "rid": rev_id})

    cache.append({"id": new_id, "name": source_name, "norm": norm})
    return new_id


def ignore_player(rev_id, best_id, best_score):
    """Deja el registro pendiente, actualizando el score sugerido."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE player_review
            SET suggested_canonical_id = :cid, similarity_score = :score
            WHERE id = :rid
        """), {"cid": best_id, "score": best_score, "rid": rev_id})


# ── Resolución interactiva principal ─────────────────────────────────────────

def reprocess_reviews(auto_only=False, source_filter=None, accept_threshold=85, new_threshold=50, metadata_mode=False, loose_mode=False):
    where_source = "AND source_system = :src" if source_filter else ""
    params_source = {"src": source_filter} if source_filter else {}

    # 1. Cargar cache y estadísticas globales
    with engine.connect() as conn:
        cache = _get_player_cache(conn)

        total_global = conn.execute(text("SELECT COUNT(*) FROM player_review")).scalar() or 0
        total_resolved_initial = conn.execute(text("SELECT COUNT(*) FROM player_review WHERE resolved = TRUE")).scalar() or 0
        total_unresolved_initial = conn.execute(text(f"""
            SELECT COUNT(*) FROM player_review WHERE resolved = FALSE {where_source}
        """), params_source).scalar() or 0

        rows = conn.execute(text(f"""
            SELECT id, source_name, source_system, source_id, similarity_score, suggested_canonical_id
            FROM player_review
            WHERE resolved = FALSE {where_source}
            ORDER BY similarity_score DESC NULLS LAST, id
        """), params_source).fetchall()

    # 2. Mostrar panel de estado inicial
    print_header("REPROCESANDO JUGADORES PENDIENTES")
    pct_initial = (total_resolved_initial / total_global * 100) if total_global > 0 else 0
    bar_initial = make_progress_bar(total_resolved_initial, total_global, width=25)
    print(f"\n  Estado de la cola global:")
    print(f"    Total registros:       {total_global:,}")
    print(f"    Ya resueltos:          {total_resolved_initial:,} ({pct_initial:.2f}%) {bar_initial}")
    print(f"    Pendientes este lote:  {total_unresolved_initial:,}")
    if source_filter:
        print(f"    Fuente filtrada:       {source_filter}")
    if auto_only:
        print(f"    Modo:                  AUTOMATICO (sin prompts interactivos)")
        print(f"    Umbral auto-aceptar:   {accept_threshold}%")
        print(f"    Umbral auto-nuevo:     {new_threshold}%")
        if metadata_mode:
            print(f"    Modo metadata:         ACTIVADO")
    elif metadata_mode:
        print(f"    Modo metadata:         ACTIVADO")
    print()

    if not rows:
        print("  No hay jugadores pendientes de revision.")
        return

    total_rows = len(rows)
    print(f"  Evaluando {total_rows:,} jugadores...\n")

    auto_linked = 0
    auto_created = 0
    left_pending = 0

    for idx, (rev_id, source_name, source_system, source_id, db_score, db_suggested) in enumerate(rows, start=1):
        id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
        if not id_col:
            continue

        norm = normalize(source_name)
        if not norm:
            continue

        # MEJORA DE RENDIMIENTO: Si estamos en auto_only y ya tenemos el score cacheado, 
        # evitamos recalcular contra los 18,000 jugadores (lo que causaba el bloqueo/lentitud extrema)
        if auto_only and db_score is not None:
            best_score = db_score
            best_id = db_suggested
            # Mockeamos top_candidates para que no falle el resto del cdigo
            top_candidates = [(db_score, db_suggested, "Candidato Cacheado")] if best_id else []
        else:
            # MEJORA 2: Obtener top-3 candidatos en lugar de solo el mejor (calcula desde cero)
            top_candidates = get_top_candidates(norm, cache, top_n=3)
            best_score = top_candidates[0][0] if top_candidates else 0
            best_id    = top_candidates[0][1] if top_candidates else None

        is_single_word = len(norm.split()) == 1

        candidate_meta = {}
        metadata_bonus = 0
        adjusted_score = best_score
        strong_metadata = False
        best_candidate_name = top_candidates[0][2] if top_candidates else None
        if best_id and metadata_mode:
            with engine.connect() as conn:
                candidate_meta = get_candidate_metadata(conn, best_id)
            metadata_bonus = metadata_confidence_bonus(candidate_meta, source_name, best_candidate_name)
            adjusted_score = min(100, best_score + metadata_bonus)
            strong_metadata = strong_candidate_metadata(candidate_meta, source_name, best_candidate_name)

        # Determinar si es caso automático
        # IMPORTANTE: En auto-only, NO usar metadata_mode (demasiado permisivo)
        # Validar que el apellido es similar antes de aceptar
        last_name_match = False
        try:
            if best_candidate_name:
                source_last = _last_name_from_normalized(normalize(source_name))
                candidate_last = _last_name_from_normalized(normalize(best_candidate_name))
                last_name_match = source_last and candidate_last and source_last == candidate_last
        except Exception as e:
            # Si hay error en validación de apellido, ser conservador
            last_name_match = False
        
        # En auto-only, ser MÁS conservador (no usar metadata_mode)
        if auto_only:
            metadata_bonus = 0  # No usar metadata en auto-only
            adjusted_score = best_score
        
        # Lógica simplificada: SOLO aceptar si VERY confident
        auto_accept = False
        if best_id:
            # Ultra-safe: 95%+ siempre
            if best_score >= 95:
                auto_accept = True
            # Safe: 85%+ AND (apellido coincide OR score muy alto)
            elif best_score >= accept_threshold and not is_single_word:
                if last_name_match or best_score >= 90:
                    auto_accept = True
        
        auto_new = not auto_accept and best_score < new_threshold

        # En modo --auto-only, considerar loose_mode
        if auto_only and not auto_accept and not auto_new:
            # Si loose_mode=True, aceptar también si score >= 85 Y apellido coincide
            if loose_mode and best_score >= 85 and not is_single_word and last_name_match:
                auto_accept = True
            else:
                left_pending += 1
                continue

        # Mostrar cabecera de progreso
        resolved_this_session = auto_linked + auto_created
        current_resolved = total_resolved_initial + resolved_this_session
        print_progress(idx, total_rows, current_resolved, total_global, resolved_this_session, left_pending)

        # ── COMPROBACIÓN DE CONFLICTO DE ID DUPLICADO (SIMPLIFICADO PARA AUTO-ONLY) ──
        existing_player = None
        try:
            with engine.connect() as conn:
                existing_player = conn.execute(text(f"""
                    SELECT canonical_id, canonical_name
                    FROM dim_player
                    WHERE {id_col} = :sid
                    LIMIT 1
                """), {"sid": source_id}).fetchone()
        except Exception as e:
            print(f"    ⚠️ Error DB: {e}")
            left_pending += 1
            continue

        if existing_player:
            print(f"\033[93m  [CONFLICTO DE ID]\033[0m '{source_id}' ya existe")
            
            # En auto-only: simplemente ignorar (no enlazar automáticamente)
            if auto_only:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE player_review
                            SET suggested_canonical_id = :cid, similarity_score = :score
                            WHERE id = :rid
                        """), {"cid": existing_player.canonical_id, "score": best_score, "rid": rev_id})
                except Exception as e:
                    print(f"    ⚠️ Error actualizar: {e}")
                
                left_pending += 1
                print(f"    --> [IGNORADO] (conflicto ID)")
                continue
            
            # Si no es auto-only, hacer validación más completa
            name_sim = _similarity_score(normalize(source_name), normalize(existing_player.canonical_name))
            
            if name_sim >= 90 or normalize(source_name) == normalize(existing_player.canonical_name):
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE player_review
                            SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
                            WHERE id = :rid
                        """), {"cid": existing_player.canonical_id, "score": best_score, "rid": rev_id})
                except Exception as e:
                    print(f"    ⚠️ Error: {e}")
                else:
                    auto_linked += 1
                    print(f"    --> [ENLAZADO] (similitud {name_sim}%)")
                continue
            
            # Modo interactivo: preguntar
            print(f"    BD: '{existing_player.canonical_name}' (similitud: {name_sim}%)")
            prompt = "  [E]nlazar / [N]uevo / [I]gnorar? (default=I): "
            resp = input(prompt).strip().lower() or 'i'
            
            if resp == 'e':
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE player_review
                            SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
                            WHERE id = :rid
                        """), {"cid": existing_player.canonical_id, "score": best_score, "rid": rev_id})
                    auto_linked += 1
                    print("    --> [ENLAZADO]")
                except Exception as e:
                    print(f"    ⚠️ Error: {e}")
            elif resp == 'n':
                try:
                    new_id = create_new_player(rev_id, source_name, source_id, id_col, best_score, cache, norm, with_external_id=False)
                    auto_created += 1
                    print("    --> [NUEVO]")
                except Exception as e:
                    print(f"    ⚠️ Error crear: {e}")
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE player_review
                            SET suggested_canonical_id = :cid, similarity_score = :score
                            WHERE id = :rid
                        """), {"cid": existing_player.canonical_id, "score": best_score, "rid": rev_id})
                except Exception as e:
                    print(f"    ⚠️ Error: {e}")
                left_pending += 1
                print("    --> [IGNORADO]")
            continue
        # ── FIN CONFLICTO ──

        # MEJORA 2: Mostrar top-3 candidatos
        print(f"  Fuente:  {source_name}  ({source_system})")
        if top_candidates:
            print(f"  Candidatos en BD:")
            for rank, (sc, cid, cname) in enumerate(top_candidates, start=1):
                marker = "  --> " if rank == 1 else "      "
                tag = " [SUGERIDO]" if rank == 1 else ""
                print(f"  {rank}. {marker}{cname} (Similitud: {sc}%){tag}")
                if metadata_mode and rank == 1 and candidate_meta:
                    meta_parts = []
                    if candidate_meta.get("position"):
                        meta_parts.append(candidate_meta["position"])
                    if candidate_meta.get("nationality"):
                        meta_parts.append(candidate_meta["nationality"])
                    if candidate_meta.get("birth_date"):
                        meta_parts.append(str(candidate_meta["birth_date"]))
                    if meta_parts:
                        print(f"       Metadata: {', '.join(meta_parts)} (+{metadata_bonus} pts)")
                    external_ids = sum(
                        bool(candidate_meta.get(col))
                        for col in ("id_sofascore", "id_understat", "id_transfermarkt", "id_statsbomb", "id_whoscored")
                    )
                    print(f"       IDs externos: {external_ids}")
                    if strong_metadata:
                        print("       [METADATA FUERTE] Este caso puede auto-aceptarse en metadata-mode.")
        else:
            print("  Candidatos en BD: NINGUNO")

        # Aplicar lógica automática o interactiva
        if auto_accept:
            resp = 's'
            if best_score >= 95:
                print("  --> [AUTO-ACEPTADO] (score muy alto)")
            else:
                print(f"  --> [AUTO-ACEPTADO] (metadata mode: score ajustado a {adjusted_score}%)")
        elif auto_new:
            resp = 'n'
            print("  --> [AUTO-NUEVO] (similitud muy baja)")
        else:
            # Caso de duda: preguntar al usuario
            if best_score >= 85 and best_id and not is_single_word:
                default = 's'
                prompt = "  Que hacemos? [S]on el mismo / (N)uevo / (I)gnorar (Enter=S): "
            else:
                default = 'i'
                prompt = "  Que hacemos? (S)on el mismo / (N)uevo / [I]gnorar (Enter=I): "

            # MEJORA 2: Permitir elegir candidato alternativo del top-3
            if len(top_candidates) > 1:
                prompt = prompt.rstrip(": ") + " / (2/3) Elegir candidato alternativo: "

            while True:
                resp = input(prompt).strip().lower() or default
                if resp in ['s', 'n', 'i']:
                    break
                # Selección de candidato alternativo
                if resp in ['2', '3']:
                    alt_idx = int(resp) - 1
                    if alt_idx < len(top_candidates):
                        best_score = top_candidates[alt_idx][0]
                        best_id    = top_candidates[alt_idx][1]
                        best_name  = top_candidates[alt_idx][2]
                        print(f"  Cambiado a candidato {resp}: {best_name} ({best_score}%)")
                        resp = 's'
                        break
                    else:
                        print(f"  No hay candidato {resp}. Opciones validas: s, n, i{', 2, 3' if len(top_candidates) > 1 else ''}")
                        continue
                print(f"  Opcion invalida.")

        # Ejecutar acción elegida
        if resp == 's':
            if not best_id:
                print("  No hay sugerencia para enlazar. Creando como nuevo...")
                resp = 'n'
            else:
                link_player(rev_id, best_id, source_id, id_col, best_score, cache, norm, source_name)
                auto_linked += 1
                suggested_name = next((p["name"] for p in cache if p["id"] == best_id), "?")
                print(f"  --> [ENLAZADO] con '{suggested_name}'")

        if resp == 'n':
            new_id = create_new_player(rev_id, source_name, source_id, id_col, best_score, cache, norm, with_external_id=True)
            auto_created += 1
            print(f"  --> [NUEVO JUGADOR CREADO] canonical_id={new_id}")

        if resp == 'i':
            ignore_player(rev_id, best_id, best_score)
            left_pending += 1
            print("  --> [IGNORADO POR AHORA]")

    # Resumen final mejorado
    print_final_report(auto_linked, auto_created, left_pending, total_resolved_initial, total_global)


# ── Punto de entrada CLI ──────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Resuelve la cola de jugadores pendientes en player_review.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--auto-only",
        action="store_true",
        default=False,
        help="Solo procesa los casos automaticos (score >= 95 o score < 50).\n"
             "No muestra ningun prompt interactivo. Ideal para primer pase rapido.",
    )
    parser.add_argument(
        "--accept-threshold",
        type=int,
        default=85,
        metavar="PCT",
        help="Similitud minima para auto-aceptar un candidato (por defecto 85, más conservador).",
    )
    parser.add_argument(
        "--new-threshold",
        type=int,
        default=50,
        metavar="PCT",
        help="Similitud maxima para crear automaticamente un nuevo jugador.",
    )
    parser.add_argument(
        "--loose",
        action="store_true",
        default=False,
        help="Modo loose: En --auto-only, acepta también casos con score >= 75.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        metavar="FUENTE",
        help="Filtra la cola por fuente de datos.\n"
             "Valores validos: sofascore, whoscored, understat, transfermarkt, statsbomb",
    )
    parser.add_argument(
        "--metadata-mode",
        action="store_true",
        default=False,
        help="Usa metadata de dim_player para ajustar la confianza en casos dudosos.",
    )
    parser.add_argument(
        "--batch-auto-new",
        action="store_true",
        default=False,
        help="Modo SQL masivo: resuelve todos los registros con score < 25\n"
             "como nuevos jugadores sin interaccion. Muy rapido para limpiezas grandes.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        if args.batch_auto_new:
            run_batch_auto_new(source_filter=args.source)
        else:
            reprocess_reviews(
                auto_only=args.auto_only,
                source_filter=args.source,
                accept_threshold=args.accept_threshold,
                new_threshold=args.new_threshold,
                metadata_mode=args.metadata_mode,
                loose_mode=args.loose,
            )
    except KeyboardInterrupt:
        print("\n\nProceso cancelado por el usuario. Todo lo resuelto hasta ahora se ha guardado.")
