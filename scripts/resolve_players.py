#!/usr/bin/env python3
"""
resolve_players.py
==================
Script para resolver manualmente casos en player_review.

Permite:
- Aceptar la sugerencia automática
- Rechazar y crear nuevo jugador
- Buscar manualmente y enlazar

Uso:
    python resolve_players.py --interactive    # Modo interactivo
    python resolve_players.py --auto-accept-75 # Aceptar automáticamente sim >= 75%
"""

import argparse
import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from loaders.common import engine

def auto_resolve_high_similarity(threshold=75):
    """Resolver automáticamente jugadores con similitud alta."""
    print(f"\n[AUTO-RESOLVE] Resolviendo con similitud >= {threshold}%...")
    
    with engine.begin() as conn:
        # Obtener candidatos
        result = conn.execute(text(f"""
            SELECT id, suggested_canonical_id, similarity_score
            FROM player_review
            WHERE resolved = FALSE 
              AND similarity_score >= {threshold}
              AND suggested_canonical_id IS NOT NULL
        """))
        
        rows = result.fetchall()
        count = 0
        
        for review_id, canonical_id, similarity in rows:
            # Marcar como resuelto y asignar ID
            conn.execute(text("""
                UPDATE player_review 
                SET resolved = TRUE, 
                    canonical_id_assigned = :canonical_id,
                    reviewed_at = NOW()
                WHERE id = :review_id
            """), {"canonical_id": canonical_id, "review_id": review_id})
            
            count += 1
        
        print(f"  [OK] Resueltos automáticamente: {count} jugadores")
        return count


def interactive_resolve():
    """Modo interactivo para resolver casos."""
    print("\n[INTERACTIVE] Modo interactivo de resolución")
    print("Comandos: (A)ceptar | (R)echazar | (S)altar | (Q)uit | (L)ist\n")
    
    with engine.connect() as conn:
        # Obtener sin resolver, ordenados por similitud
        result = conn.execute(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.source_id,
                pr.suggested_canonical_id,
                dp.canonical_name as suggested_name,
                pr.similarity_score,
                dp.position,
                dp.nationality
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            WHERE pr.resolved = FALSE
            ORDER BY pr.similarity_score DESC, pr.created_at DESC
        """))
        
        rows = result.fetchall()
        
        if not rows:
            print("  [OK] No hay jugadores pendientes")
            return
        
        processed = 0
        for row in rows:
            (review_id, source_name, source_system, source_id, 
             sugg_canonical_id, sugg_name, similarity, position, nationality) = row
            
            print(f"\n[{processed + 1}/{len(rows)}] {source_name}")
            print(f"      Fuente: {source_system} (ID: {source_id})")
            print(f"      Similitud: {similarity}%")
            
            if sugg_canonical_id:
                print(f"      Sugerencia: {sugg_name}")
                if position:
                    print(f"      Posición: {position} | Nacionalidad: {nationality}")
                print(f"      ¿Aceptar sugerencia? (A/R/S/Q/L): ", end="", flush=True)
            else:
                print(f"      [SIN SUGERENCIA]")
                print(f"      ¿Crear nuevo jugador? (A/R/S/Q/L): ", end="", flush=True)
            
            try:
                choice = input().strip().upper()
            except EOFError:
                print("\n[Abortado por EOF]")
                break
            
            if choice == "Q":
                print("\n[OK] Saliendo...")
                break
            
            elif choice == "A":
                # Aceptar sugerencia
                if sugg_canonical_id:
                    with engine.begin() as update_conn:
                        update_conn.execute(text("""
                            UPDATE player_review 
                            SET resolved = TRUE, 
                                canonical_id_assigned = :canonical_id,
                                reviewed_at = NOW()
                            WHERE id = :review_id
                        """), {"canonical_id": sugg_canonical_id, "review_id": review_id})
                    print("      [RESUELTO] Aceptado")
                    processed += 1
                else:
                    print("      [ERROR] No hay sugerencia para aceptar")
            
            elif choice == "R":
                # Rechazar - marcar como sin resolver pero NO asignar
                with engine.begin() as update_conn:
                    update_conn.execute(text("""
                        UPDATE player_review 
                        SET resolved = FALSE, 
                            canonical_id_assigned = NULL,
                            similarity_score = 0,
                            reviewed_at = NOW()
                        WHERE id = :review_id
                    """), {"review_id": review_id})
                print("      [RECHAZADO] Marcado como diferente")
                processed += 1
            
            elif choice == "S":
                print("      [SALTADO] Revisión posterior")
            
            elif choice == "L":
                # Listar sugerencias alternativas
                print("\n      [Alternativas]")
                alt_result = conn.execute(text("""
                    SELECT TOP 5
                        canonical_name,
                        position,
                        nationality,
                        similarity_score
                    FROM dim_player
                    WHERE lower(canonical_name) LIKE lower(:pattern)
                    ORDER BY similarity_score DESC
                """), {"pattern": f"%{source_name}%"})
                
                for i, (name, pos, nat, sim) in enumerate(alt_result, 1):
                    print(f"      {i}. {name} ({pos}, {nat}) - Sim: {sim}%")
                print("      (Ingresa opción o presiona Enter para continuar)")
            
            else:
                print("      [INVALIDO] Intenta nuevamente")


def stats_by_source():
    """Mostrar resumen por fuente."""
    print("\n[STATS BY SOURCE]")
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                source_system,
                COUNT(*) as total,
                SUM(CASE WHEN resolved THEN 1 ELSE 0 END) as resolved_count,
                ROUND(100.0 * SUM(CASE WHEN resolved THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_resolved,
                ROUND(AVG(similarity_score)::numeric, 1) as avg_similarity
            FROM player_review
            GROUP BY source_system
            ORDER BY total DESC
        """))
        
        for source, total, resolved, pct, avg_sim in result:
            print(f"  {source:15} - Total: {total:4} | Resueltos: {resolved:4} ({pct:5.1f}%) | Similitud: {avg_sim}%")


def main():
    parser = argparse.ArgumentParser(
        description="Resolver casos pendientes en player_review"
    )
    
    parser.add_argument("--interactive", action="store_true", help="Modo interactivo")
    parser.add_argument("--auto-accept", type=int, default=0, 
                       help="Aceptar automáticamente con similitud >= X%")
    parser.add_argument("--stats", action="store_true", help="Mostrar estadísticas")
    
    args = parser.parse_args()
    
    if not any([args.interactive, args.auto_accept, args.stats]):
        args.stats = True
    
    try:
        if args.stats:
            stats_by_source()
        
        if args.auto_accept > 0:
            auto_resolve_high_similarity(args.auto_accept)
        
        if args.interactive:
            interactive_resolve()
        
        print("\n[OK] Completado\n")
        
    except Exception as e:
        print(f"\n[ERROR] {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
