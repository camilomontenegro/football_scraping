"""
normalize_db_seasons.py
========================
Normaliza retroactivamente el campo `season` en BD al formato canónico
'YYYY/YYYY' usando utils.season_utils.normalize_season().

A diferencia del SQL plano (db/normalize_seasons.sql), este script
detecta automáticamente CUALQUIER formato presente y lo normaliza —
incluso temporadas futuras o variantes que no estén listadas a mano.

También borra los partidos residuales con data_source='understat',
siguiendo el criterio del equipo: Understat NO es fuente principal
de dim_match, solo enriquece IDs.

Uso:
    python -m scripts.normalize_db_seasons               # ejecuta los cambios
    python -m scripts.normalize_db_seasons --dry-run     # solo muestra qué haría
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402

from loaders.common import engine  # noqa: E402
from utils.season_utils import normalize_season  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="No hace cambios, solo lista lo que haría.")
    args = ap.parse_args()

    print("=" * 70)
    print("  NORMALIZACIÓN DE SEASONS")
    print("=" * 70)

    with engine.begin() as conn:

        # 1) Limpiar Understat residual de dim_match
        rows = conn.execute(text("""
            SELECT match_id FROM dim_match WHERE data_source = 'understat'
        """)).fetchall()
        ids = [r[0] for r in rows]
        if ids:
            print(f"\n[1/4] Partidos con data_source='understat': {len(ids)}")
            print(f"      → IDs: {ids}")
            if not args.dry_run:
                conn.execute(text(
                    "DELETE FROM fact_events WHERE match_id = ANY(:ids)"
                ), {"ids": ids})
                conn.execute(text(
                    "DELETE FROM fact_shots  WHERE match_id = ANY(:ids)"
                ), {"ids": ids})
                conn.execute(text(
                    "DELETE FROM dim_match WHERE match_id = ANY(:ids)"
                ), {"ids": ids})
                print("      → eliminados (events, shots, dim_match)")
        else:
            print("\n[1/4] Sin partidos residuales de Understat. OK.")

        # 2) Listar formatos de season actualmente presentes
        rows = conn.execute(text("""
            SELECT season, COUNT(*) AS n
            FROM dim_match
            GROUP BY season
            ORDER BY season
        """)).fetchall()
        print(f"\n[2/4] Formatos de season en dim_match ({len(rows)} distintos):")
        for s, n in rows:
            norm = normalize_season(s)
            mark = "" if norm == s else f"  →  {norm!r}"
            print(f"      {s!r:40s}  count={n:>5}{mark}")

        # 3) Normalizar dim_match.season
        updates = 0
        for s, n in rows:
            norm = normalize_season(s)
            if norm and norm != s:
                if args.dry_run:
                    print(f"      [dry] {s!r} → {norm!r} ({n} filas)")
                else:
                    res = conn.execute(text(
                        "UPDATE dim_match SET season = :n WHERE season = :s"
                    ), {"n": norm, "s": s})
                    updates += res.rowcount
        print(f"\n[3/4] dim_match.season normalizados: {updates}")

        # 4) Normalizar fact_injuries.season
        rows = conn.execute(text("""
            SELECT season, COUNT(*) AS n
            FROM fact_injuries
            GROUP BY season ORDER BY season
        """)).fetchall()
        print(f"\n[4/4] Formatos en fact_injuries ({len(rows)} distintos):")
        inj_updates = 0
        for s, n in rows:
            norm = normalize_season(s)
            if norm and norm != s:
                if args.dry_run:
                    print(f"      [dry] {s!r} → {norm!r} ({n} filas)")
                else:
                    res = conn.execute(text(
                        "UPDATE fact_injuries SET season = :n WHERE season = :s"
                    ), {"n": norm, "s": s})
                    inj_updates += res.rowcount
        print(f"      fact_injuries.season normalizados: {inj_updates}")

        # Estado final
        if not args.dry_run:
            print("\n[OK] Estado final dim_match.season:")
            rows = conn.execute(text("""
                SELECT season, COUNT(*) FROM dim_match
                GROUP BY season ORDER BY season
            """)).fetchall()
            for s, n in rows:
                print(f"      {s!r:20s}  count={n}")

    print("\n" + "=" * 70)
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
