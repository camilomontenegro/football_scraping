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

def simple_auto_only():
    """Procesa automáticamente los casos seguros (95%+ o 85%+ con apellido match)."""
    
    print("\n" + "=" * 80)
    print("  MODO AUTO-ONLY SIMPLE (sin deadlocks)")
    print("=" * 80 + "\n")
    
    processed = 0
    errors = 0
    
    with engine.connect() as conn:
        # Obtener TODOS los pendientes de una sola vez
        print("📥 Cargando registros pendientes...")
        rows = conn.execute(text("""
            SELECT id, source_name, source_system, source_id, similarity_score
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
            
            # Mostrar progreso cada 10 registros
            if idx % 10 == 0:
                print(f"  [{idx}/{total}] Procesados: {processed}, Errores: {errors}")
            
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
                    WHERE canonical_id = (
                        SELECT suggested_canonical_id FROM player_review WHERE id = :rid
                    )
                    LIMIT 1
                """), {"rid": rev_id}).fetchone()
                
                if candidate:
                    source_last = normalize(source_name).split()[-1] if source_name else ""
                    candidate_last = normalize(candidate.canonical_name).split()[-1] if candidate.canonical_name else ""
                    if source_last and candidate_last and source_last == candidate_last:
                        auto_accept = True
            
            # Si se acepta, actualizar en BD
            if auto_accept:
                try:
                    conn.execute(text("""
                        UPDATE player_review
                        SET resolved = TRUE, reviewed_at = NOW()
                        WHERE id = :rid
                    """), {"rid": rev_id})
                    processed += 1
                except Exception as e:
                    errors += 1
                    print(f"  ❌ Error en {source_name}: {e}")
        
        # Commit de todos los cambios
        conn.commit()
    
    # Resumen final
    print(f"\n" + "=" * 80)
    print(f"  ✅ RESUMEN")
    print(f"=" * 80)
    print(f"  • Procesados: {processed}")
    print(f"  • Errores: {errors}")
    print(f"  • Pendientes aun: {total - processed}")
    
    # Verificar estado final en BD
    with engine.connect() as conn:
        final_pending = conn.execute(text(
            "SELECT COUNT(*) FROM player_review WHERE resolved = FALSE"
        )).scalar()
        final_pct = (final_pending / 8605 * 100) if 8605 > 0 else 0
        print(f"  • Estado BD: {final_pending} pendientes ({final_pct:.2f}%)")
    
    print("=" * 80 + "\n")

if __name__ == "__main__":
    try:
        simple_auto_only()
    except KeyboardInterrupt:
        print("\n\n❌ Cancelado por usuario.")
    except Exception as e:
        print(f"\n❌ Error fatal: {e}")
        import traceback
        traceback.print_exc()
