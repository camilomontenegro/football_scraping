#!/usr/bin/env python3
"""
review_players.py
=================
Script para revisar y resolver jugadores en player_review.

Uso:
    python review_players.py --unresolved       # Mostrar sin resolver
    python review_players.py --stats            # Estadísticas
    python review_players.py --export           # Exportar a CSV
"""

import argparse
import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from loaders.common import engine

def show_unresolved_stats():
    """Mostrar estadísticas de jugadores sin resolver."""
    print("\n" + "="*70)
    print("[STATS] Jugadores en player_review")
    print("="*70)
    
    with engine.connect() as conn:
        # Total sin resolver
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) as unresolved,
                SUM(CASE WHEN resolved = TRUE THEN 1 ELSE 0 END) as resolved
            FROM player_review
        """))
        row = result.fetchone()
        print(f"  Total: {row[0]}")
        print(f"  Sin resolver: {row[1]}")
        print(f"  Resueltos: {row[2]}")
        
        # Por fuente
        print("\n[Por fuente]")
        result = conn.execute(text("""
            SELECT 
                source_system,
                COUNT(*) as count,
                SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) as unresolved,
                AVG(similarity_score) as avg_similarity
            FROM player_review
            GROUP BY source_system
            ORDER BY count DESC
        """))
        
        for source, count, unres, avg_sim in result:
            print(f"  {source:20} - Total: {count:4} | Sin resolver: {unres:4} | Similitud promedio: {avg_sim:.1f}%")
        
        # Por rango de similitud
        print("\n[Por rango de similitud]")
        result = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN similarity_score >= 90 THEN '90-100% (Muy similar)'
                    WHEN similarity_score >= 75 THEN '75-89% (Similar)'
                    WHEN similarity_score >= 50 THEN '50-74% (Posible)'
                    ELSE '< 50% (Poco probable)'
                END as rango,
                COUNT(*) as count,
                SUM(CASE WHEN resolved = FALSE THEN 1 ELSE 0 END) as unresolved
            FROM player_review
            GROUP BY rango
            ORDER BY similarity_score DESC
        """))
        
        for rango, count, unres in result:
            print(f"  {rango:25} - Total: {count:4} | Sin resolver: {unres:4}")


def show_unresolved_players(limit=20):
    """Mostrar jugadores sin resolver."""
    print("\n" + "="*100)
    print("[UNRESOLVED] Primeros jugadores sin resolver")
    print("="*100)
    
    with engine.connect() as conn:
        result = conn.execute(text(f"""
            SELECT 
                id,
                source_name,
                source_system,
                source_id,
                suggested_canonical_id,
                similarity_score,
                resolved
            FROM player_review
            WHERE resolved = FALSE
            ORDER BY similarity_score DESC, created_at DESC
            LIMIT {limit}
        """))
        
        rows = result.fetchall()
        
        if not rows:
            print("  [OK] No hay jugadores sin resolver")
            return
        
        print(f"\n  Mostrando {len(rows)} de {len(rows)} registros:\n")
        
        for id_, name, source, source_id, sugg_id, sim, resolved in rows:
            print(f"  [{id_:4}] {name:40} | {source:15} (ID: {source_id})")
            print(f"          Sugerencia: ID {sugg_id} | Similitud: {sim}%")
            
            # Mostrar detalles del sugerido
            if sugg_id:
                det = conn.execute(text("""
                    SELECT canonical_name FROM dim_player WHERE canonical_id = :id
                """), {"id": sugg_id}).fetchone()
                if det:
                    print(f"          Sugerido: {det[0]}")
            print()


def export_to_csv():
    """Exportar player_review a CSV."""
    print("\n[EXPORT] Exportando player_review a CSV...")
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.source_id,
                pr.suggested_canonical_id,
                dp.canonical_name as suggested_player_name,
                pr.similarity_score,
                pr.resolved,
                pr.created_at,
                pr.reviewed_at
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            ORDER BY pr.similarity_score DESC, pr.created_at DESC
        """), conn)
    
    output_file = Path(__file__).parent / "data" / "player_review_export.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"  [OK] Exportado a: {output_file}")
    print(f"  Total registros: {len(df)}")
    print(f"  Sin resolver: {df[~df['resolved']].shape[0]}")


def show_best_candidates():
    """Mostrar los mejores candidatos de cada fuente."""
    print("\n" + "="*100)
    print("[CANDIDATES] Mejores candidatos por similitud")
    print("="*100)
    
    with engine.connect() as conn:
        # Top 30 por similitud
        result = conn.execute(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.similarity_score,
                dp.canonical_name as suggested_player_name,
                pr.resolved
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            WHERE pr.similarity_score >= 75
            ORDER BY pr.similarity_score DESC
            LIMIT 30
        """))
        
        rows = result.fetchall()
        
        if not rows:
            print("  [INFO] No hay candidatos con similitud >= 75%")
            return
        
        print(f"\n  Total candidatos con similitud >= 75%: {len(rows)}\n")
        
        for id_, name, source, sim, sugg_name, resolved in rows:
            status = "[RESUELTO]" if resolved else "[PENDIENTE]"
            print(f"  {status} [{id_:4}] {name:40} -> {sugg_name:40}")
            print(f"           Fuente: {source:15} | Similitud: {sim}%\n")


def main():
    parser = argparse.ArgumentParser(
        description="Revisar y analizar jugadores en player_review",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python review_players.py --stats           # Ver estadísticas
  python review_players.py --unresolved      # Ver sin resolver
  python review_players.py --candidates      # Ver mejores candidatos
  python review_players.py --export          # Exportar a CSV
        """
    )
    
    parser.add_argument("--stats", action="store_true", help="Mostrar estadísticas")
    parser.add_argument("--unresolved", action="store_true", help="Mostrar sin resolver")
    parser.add_argument("--candidates", action="store_true", help="Mostrar mejores candidatos")
    parser.add_argument("--export", action="store_true", help="Exportar a CSV")
    parser.add_argument("--limit", type=int, default=20, help="Límite de registros a mostrar")
    
    args = parser.parse_args()
    
    # Si no se especifica nada, mostrar stats
    if not any([args.stats, args.unresolved, args.candidates, args.export]):
        args.stats = True
    
    try:
        if args.stats:
            show_unresolved_stats()
        
        if args.unresolved:
            show_unresolved_players(args.limit)
        
        if args.candidates:
            show_best_candidates()
        
        if args.export:
            export_to_csv()
        
        print("\n[OK] Operación completada\n")
        
    except Exception as e:
        print(f"\n[ERROR] {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
