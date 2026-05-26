#!/usr/bin/env python3
"""
Script SIMPLE y RÁPIDO para procesar automáticamente player_review.
Diseñado para evitar deadlocks y ser lo más directo posible.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from loaders.common import engine
from utils.mdm_engine import normalize
from utils.mdm_config import SOURCE_ID_FIELDS

def simple_auto_only():
    """Procesa automáticamente los casos seguros (95%+ o 85%+ con apellido match).

    Ahora correctamente:
    1. Establece canonical_id_assigned en player_review
    2. Actualiza dim_player con el source ID correspondiente
    """

    print("\n" + "=" * 80)
    print("  MODO AUTO-ONLY SIMPLE (sin deadlocks)")
    print("=" * 80 + "\n")

    processed = 0
    errors = 0
    skipped_no_suggestion = 0

    with engine.connect() as conn:
        # Obtener TODOS los pendientes de una sola vez
        print("Cargando registros pendientes...")
        rows = conn.execute(text("""
            SELECT id, source_name, source_system, source_id, similarity_score,
                   suggested_canonical_id
            FROM player_review
            WHERE resolved = FALSE
            ORDER BY id
        """)).fetchall()

        total = len(rows)
        print(f"   Total pendientes: {total}\n")

        # Procesar cada uno
        for idx, row in enumerate(rows, 1):
            rev_id = row.id
            source_name = row.source_name
            source_system = row.source_system
            source_id = row.source_id
            similarity_score = row.similarity_score or 0
            suggested_cid = row.suggested_canonical_id

            # Mostrar progreso cada 10 registros
            if idx % 10 == 0:
                print(f"  [{idx}/{total}] Procesados: {processed}, Errores: {errors}, Sin sugerencia: {skipped_no_suggestion}")

            # Lógica simple: ¿Se auto-acepta?
            auto_accept = False

            if similarity_score >= 95:
                # Ultra-seguro: 95%+
                auto_accept = True
            elif similarity_score >= 85:
                # Seguro con validación de apellido
                # Obtener el candidato sugerido
                candidate = conn.execute(text("""
                    SELECT canonical_name FROM dim_player
                    WHERE canonical_id = :cid
                    LIMIT 1
                """), {"cid": suggested_cid}).fetchone() if suggested_cid else None

                if candidate:
                    source_last = normalize(source_name).split()[-1] if source_name else ""
                    candidate_last = normalize(candidate.canonical_name).split()[-1] if candidate.canonical_name else ""
                    if source_last and candidate_last and source_last == candidate_last:
                        auto_accept = True

            # Si se acepta, actualizar BD correctamente
            if auto_accept:
                if not suggested_cid:
                    skipped_no_suggestion += 1
                    continue

                id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
                if not id_col:
                    skipped_no_suggestion += 1
                    continue

                try:
                    # 1. Actualizar dim_player con el source ID
                    conn.execute(text(f"""
                        UPDATE dim_player SET {id_col} = :sid
                        WHERE canonical_id = :cid AND {id_col} IS NULL
                    """), {"sid": source_id, "cid": suggested_cid})

                    # 2. Marcar como resuelto CON canonical_id_assigned
                    conn.execute(text("""
                        UPDATE player_review
                        SET resolved = TRUE, canonical_id_assigned = :cid,
                            reviewed_at = NOW()
                        WHERE id = :rid
                    """), {"cid": suggested_cid, "rid": rev_id})
                    processed += 1
                except Exception as e:
                    errors += 1
                    print(f"  [ERROR] {source_name}: {e}")
        
        # Commit de todos los cambios
        conn.commit()
    
    # Resumen final
    print(f"\n" + "=" * 80)
    print(f"  RESUMEN")
    print(f"=" * 80)
    print(f"  - Procesados: {processed}")
    print(f"  - Errores: {errors}")
    print(f"  - Pendientes aun: {total - processed}")
    
    # Verificar estado final en BD
    with engine.connect() as conn:
        final_pending = conn.execute(text(
            "SELECT COUNT(*) FROM player_review WHERE resolved = FALSE"
        )).scalar()
        final_pct = (final_pending / 8605 * 100) if 8605 > 0 else 0
        print(f"  - Estado BD: {final_pending} pendientes ({final_pct:.2f}%)")
    
    print("=" * 80 + "\n")

if __name__ == "__main__":
    try:
        simple_auto_only()
    except KeyboardInterrupt:
        print("\n\nCancelado por usuario.")
    except Exception as e:
        print(f"\nError fatal: {e}")
        import traceback
        traceback.print_exc()
