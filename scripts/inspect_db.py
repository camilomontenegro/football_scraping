"""
inspect_db.py
==============
Pequeño script de diagnóstico que imprime qué hay actualmente en la BD,
para depurar el flujo "Actualizar datos" del wizard.

Uso:
    python -m scripts.inspect_db
    python -m scripts.inspect_db --competition Bundesliga --season 2025/2026
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text

# Permite ejecutar como módulo desde la raíz del proyecto
sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from scripts.pipeline_runner import _season_variants, get_competition  # noqa: E402


def _fmt(n) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except Exception:
        return str(n)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--competition", "-c", type=str, default=None)
    parser.add_argument("--season", "-s", type=str, default=None)
    args = parser.parse_args()

    print("=" * 70)
    print("  INSPECCIÓN DE LA BASE DE DATOS")
    print("=" * 70)

    with engine.connect() as conn:
        # Conteos globales
        print("\n[Tablas dim_/fact_]")
        for table in [
            "dim_team", "dim_player", "dim_match",
            "fact_shots", "fact_events", "fact_injuries",
        ]:
            try:
                n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                print(f"  {table:<15} {_fmt(n):>10}")
            except Exception as e:
                print(f"  {table:<15} ERROR: {e}")

        # Distribución (competition, season) en dim_match
        print("\n[dim_match — distribución por (competition, season)]")
        rows = conn.execute(text("""
            SELECT competition, season, COUNT(*) AS c, MAX(match_date) AS last_date
            FROM dim_match
            GROUP BY competition, season
            ORDER BY c DESC
            LIMIT 30
        """)).fetchall()
        if not rows:
            print("  (vacío) — dim_match no tiene filas")
        else:
            print(f"  {'competition':<25} {'season':<22} {'count':>8}  {'last_date'}")
            for comp, season, c, last in rows:
                print(f"  {str(comp)[:24]:<25} {str(season)[:21]:<22} {_fmt(c):>8}  {last or '-'}")

        # Distribución de IDs de fuente en dim_match
        print("\n[dim_match — partidos con cada id_<fuente> rellenado]")
        for col in ["id_sofascore", "id_understat", "id_statsbomb", "id_whoscored"]:
            n = conn.execute(text(
                f"SELECT COUNT(*) FROM dim_match WHERE {col} IS NOT NULL"
            )).scalar()
            print(f"  {col:<20} {_fmt(n):>10}")

        # Si pasaron --competition --season, simular get_last_match_date
        if args.competition and args.season:
            print(f"\n[Simulación de búsqueda incremental]")
            print(f"  Competición : {args.competition}")
            print(f"  Temporada   : {args.season}")
            comp_config = get_competition(args.competition)
            comp_db_name = (comp_config["name"] if comp_config else args.competition).lower()
            variants = _season_variants(args.competition, args.season)
            print(f"  Variantes que se prueban en SQL:")
            for v in variants:
                print(f"    · {v!r}")
            print(f"  competition LIKE  : %{comp_db_name}%")

            from sqlalchemy import bindparam
            sql = text("""
                SELECT MAX(match_date), COUNT(*) FROM dim_match
                WHERE LOWER(competition) LIKE :comp_like
                  AND season IN :variants
            """).bindparams(bindparam("variants", expanding=True))
            row = conn.execute(sql, {
                "comp_like": f"%{comp_db_name}%",
                "variants":  variants,
            }).fetchone()
            last_date, match_count = row[0], row[1] or 0
            print(f"\n  Resultado: last_date={last_date}  match_count={_fmt(match_count)}")

            # Caso A: hay partidos pero todos sin fecha
            if match_count > 0 and not last_date:
                print(
                    "\n  [!] La competición/temporada SÍ está en BD pero "
                    "los partidos no tienen `match_date` (NULL)."
                )
                print(
                    "      → la opción 'Actualizar' del wizard no podrá "
                    "calcular `from_date`."
                )
                print(
                    "      Solución rápida: lanza el backfill de fechas:\n"
                    "          python -m scripts.backfill_match_dates"
                )
            # Caso B: no hay partidos para esta combinación
            elif match_count == 0:
                # ¿La competición existe con otra temporada?
                rows_c = conn.execute(text("""
                    SELECT season, COUNT(*) FROM dim_match
                    WHERE LOWER(competition) LIKE :comp_like
                    GROUP BY season ORDER BY COUNT(*) DESC
                """), {"comp_like": f"%{comp_db_name}%"}).fetchall()
                if rows_c:
                    print("\n  La competición existe con otra(s) temporada(s):")
                    for s, n in rows_c:
                        print(f"    · {s!r}: {_fmt(n)} partidos")
                    print(
                        "\n  Solución: ejecuta `python -m scripts.normalize_db_seasons` "
                        "para unificar formatos a 'YYYY/YYYY'."
                    )
                else:
                    rows_t = conn.execute(text(
                        "SELECT DISTINCT competition FROM dim_match LIMIT 20"
                    )).fetchall()
                    print("\n  La competición NO está en BD. Competiciones que sí tiene:")
                    for (c,) in rows_t:
                        print(f"    · {c!r}")
            # Caso C: todo OK, hay partidos con fecha
            else:
                print(f"  → opción 'Actualizar' usaría from_date={last_date}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
