#!/usr/bin/env python3
"""
query_players.py
================
Consultas útiles para analizar player_review.

Uso:
    python query_players.py --high-similarity   # >= 90% similitud
    python query_players.py --no-suggestion     # Sin sugerencia automática
    python query_players.py --by-source TM      # Por fuente específica
"""

import argparse
import sys
from pathlib import Path
from sqlalchemy import text
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from loaders.common import engine

def show_high_similarity():
    """Mostrar casos con alta similitud (>=90%)."""
    print("\n[HIGH SIMILARITY] Similitud >= 90%")
    print("="*120)
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.similarity_score,
                dp.canonical_name as suggested_player,
                pr.resolved
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            WHERE pr.similarity_score >= 90
            ORDER BY pr.similarity_score DESC
        """), conn)
    
    if df.empty:
        print("  [OK] No hay registros con similitud >= 90%")
        return
    
    print(f"\n  Total: {len(df)} registros\n")
    print(df.to_string(index=False))
    
    # Export
    output_file = Path(__file__).parent / "data" / "high_similarity.csv"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False)
    print(f"\n  Exportado a: {output_file}")


def show_no_suggestion():
    """Mostrar casos SIN sugerencia automática."""
    print("\n[NO SUGGESTION] Casos sin sugerencia")
    print("="*100)
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.source_id,
                pr.created_at
            FROM player_review pr
            WHERE pr.suggested_canonical_id IS NULL
            AND pr.resolved = FALSE
            ORDER BY pr.created_at DESC
        """), conn)
    
    if df.empty:
        print("  [OK] Todos tienen sugerencia")
        return
    
    print(f"\n  Total: {len(df)} registros sin sugerencia\n")
    print(df.head(20).to_string(index=False))
    
    if len(df) > 20:
        print(f"  ... y {len(df) - 20} más")


def show_by_source(source):
    """Mostrar por fuente específica."""
    print(f"\n[BY SOURCE] Fuente: {source}")
    print("="*120)
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.similarity_score,
                dp.canonical_name as suggested_player,
                pr.resolved
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            WHERE UPPER(pr.source_system) = UPPER(:source)
            ORDER BY pr.similarity_score DESC
        """), conn, params={"source": source})
    
    if df.empty:
        print(f"  [ERROR] No hay registros para fuente: {source}")
        return
    
    print(f"\n  Total: {len(df)} registros\n")
    
    # Agrupar por similitud
    print("  [ESTADÍSTICAS]")
    for threshold in [90, 75, 50]:
        count = len(df[df['similarity_score'] >= threshold])
        pct = 100.0 * count / len(df)
        print(f"    >= {threshold}%: {count:4} ({pct:5.1f}%)")
    
    print("\n  [PRIMEROS 20 REGISTROS]")
    print(df.head(20).to_string(index=False))


def show_similarity_distribution():
    """Mostrar distribución de similitud."""
    print("\n[DISTRIBUTION] Distribución de similitud")
    print("="*60)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN similarity_score >= 90 THEN '90-100%'
                    WHEN similarity_score >= 75 THEN '75-89%'
                    WHEN similarity_score >= 50 THEN '50-74%'
                    WHEN similarity_score >= 25 THEN '25-49%'
                    ELSE '0-24%'
                END as rango,
                COUNT(*) as count,
                ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM player_review WHERE resolved = FALSE)::float, 1) as pct
            FROM player_review
            WHERE resolved = FALSE
            GROUP BY rango
            ORDER BY similarity_score DESC
        """))
        
        print("\n  Rango          | Cantidad | Porcentaje")
        print("  " + "-"*50)
        for rango, count, pct in result:
            print(f"  {rango:14} | {count:8} | {pct:6.1f}%")


def search_player(pattern):
    """Buscar jugadores en player_review."""
    print(f"\n[SEARCH] Buscando: '{pattern}'")
    print("="*120)
    
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT 
                pr.id,
                pr.source_name,
                pr.source_system,
                pr.similarity_score,
                dp.canonical_name as suggested_player,
                pr.resolved
            FROM player_review pr
            LEFT JOIN dim_player dp ON pr.suggested_canonical_id = dp.canonical_id
            WHERE LOWER(pr.source_name) LIKE LOWER(:pattern)
            ORDER BY pr.similarity_score DESC
        """), conn, params={"pattern": f"%{pattern}%"})
    
    if df.empty:
        print(f"  [INFO] No hay resultados para: {pattern}")
        return
    
    print(f"\n  Total: {len(df)} resultados\n")
    print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        description="Consultas útiles para analizar player_review",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python query_players.py --high-similarity         # Similitud >= 90%
  python query_players.py --no-suggestion           # Sin sugerencia
  python query_players.py --by-source TM            # Por fuente Transfermarkt
  python query_players.py --distribution            # Distribución de similitud
  python query_players.py --search "Cristiano"      # Buscar jugador
        """
    )
    
    parser.add_argument("--high-similarity", action="store_true", help="Similitud >= 90%")
    parser.add_argument("--no-suggestion", action="store_true", help="Sin sugerencia automática")
    parser.add_argument("--by-source", type=str, help="Por fuente (TM, SofaScore, Understat, etc.)")
    parser.add_argument("--distribution", action="store_true", help="Distribución de similitud")
    parser.add_argument("--search", type=str, help="Buscar jugador por nombre")
    
    args = parser.parse_args()
    
    if not any([args.high_similarity, args.no_suggestion, args.by_source, args.distribution, args.search]):
        args.distribution = True
    
    try:
        if args.high_similarity:
            show_high_similarity()
        
        if args.no_suggestion:
            show_no_suggestion()
        
        if args.by_source:
            show_by_source(args.by_source)
        
        if args.distribution:
            show_similarity_distribution()
        
        if args.search:
            search_player(args.search)
        
    except Exception as e:
        print(f"\n[ERROR] {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
