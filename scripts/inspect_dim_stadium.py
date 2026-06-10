"""
scripts/inspect_dim_stadium.py
==============================
Audita el estado actual de la tabla dim_stadium en la BD conectada
via .env (football_db). Reporta:

  - Conexion: host, port, DB.
  - Total de filas, filas con is_current=TRUE.
  - Fill-rate por columna (% de filas con valor no-null no-vacio).
  - Muestra de 5 filas con enriquecimiento Wikidata (lat/lon/image_url).
  - Lista de columnas sugeridas para DROP (< 1% fill-rate).

Solo SELECT. No modifica nada.

Uso:
    python -m scripts.inspect_dim_stadium
    python -m scripts.inspect_dim_stadium --csv inspect_report.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _connect():
    env_path = _PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path, encoding="utf-8")
    host = os.getenv("DB_HOST", "localhost").strip()
    port = int(os.getenv("DB_PORT", "5432").strip())
    db   = os.getenv("DB_NAME", "football_db").strip()
    user = os.getenv("DB_USER", "postgres").strip()
    pwd  = os.getenv("DB_PASSWORD", "").strip()
    print(f"[CONN] {user}@{host}:{port}/{db}")
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pwd)


def _table_exists(conn, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{name}",))
        return cur.fetchone()[0] is not None


def _columns(conn, table: str):
    sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (table,))
        return cur.fetchall()


def _row_count(conn, table: str, where: str = "") -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table} {where}")
        return cur.fetchone()[0]


def _fill_rate(conn, table: str, col: str, total: int) -> int:
    """Cuenta filas con `col` distinto de NULL y de cadena vacia."""
    if total == 0:
        return 0
    with conn.cursor() as cur:
        try:
            # Para texto consideramos '' como vacio
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE "{col}" IS NOT NULL
                  AND (CASE WHEN pg_typeof("{col}")::text LIKE '%char%' OR pg_typeof("{col}")::text='text'
                            THEN trim(("{col}")::text) <> '' ELSE TRUE END)
                """
            )
            return cur.fetchone()[0]
        except psycopg2.Error:
            conn.rollback()
            # Fallback simple: solo NULL
            cur.execute(f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NOT NULL')
            return cur.fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="Volcado del fill-rate a CSV.")
    args = ap.parse_args()

    with _connect() as conn:
        if not _table_exists(conn, "dim_stadium"):
            print("ERROR: dim_stadium no existe. Lanza el bootstrap.")
            return 2

        total = _row_count(conn, "dim_stadium")
        current = _row_count(conn, "dim_stadium", "WHERE is_current = TRUE")
        teams = _row_count(conn, "dim_stadium", "WHERE id_transfermarkt_team IS NOT NULL")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT id_transfermarkt_team) FROM dim_stadium")
            distinct_teams = cur.fetchone()[0]

        print()
        print("=" * 70)
        print(f"  dim_stadium — total filas: {total}")
        print(f"                is_current=TRUE: {current}")
        print(f"                equipos distintos: {distinct_teams}")
        print("=" * 70)

        cols = _columns(conn, "dim_stadium")
        print(f"\n{'Columna':30s} | {'Tipo':25s} | Fill% | n")
        print("-" * 78)

        rows_for_csv = []
        drop_candidates = []
        for col, dtype in cols:
            n = _fill_rate(conn, "dim_stadium", col, total)
            pct = 100.0 * n / total if total else 0.0
            print(f"{col:30s} | {dtype:25s} | {pct:5.1f} | {n}")
            rows_for_csv.append({"column": col, "type": dtype, "fill_pct": round(pct, 2), "n_filled": n})
            if pct < 1.0 and col not in ("stadium_id", "data_hash"):
                drop_candidates.append(col)

        print()
        print("=" * 70)
        print("Sample de 5 filas con wikidata_qid no nulo (si las hay):")
        print("=" * 70)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stadium_id, team_slug, stadium_name,
                       wikidata_qid, latitude, longitude,
                       LEFT(COALESCE(image_url,''), 60) AS image_preview,
                       LEFT(COALESCE(wikipedia_url,''), 60) AS wiki_preview
                FROM dim_stadium
                WHERE wikidata_qid IS NOT NULL
                ORDER BY stadium_id
                LIMIT 5
            """)
            cols_s = [d[0] for d in cur.description]
            rows = cur.fetchall()
            if not rows:
                print("  (sin filas con wikidata_qid)")
            else:
                for r in rows:
                    print("  " + " | ".join(f"{c}={v}" for c, v in zip(cols_s, r)))

        print()
        print("=" * 70)
        print("Sugerencia DROP COLUMN (fill < 1%):")
        print("=" * 70)
        if drop_candidates:
            print("  ALTER TABLE dim_stadium")
            for i, c in enumerate(drop_candidates):
                sep = "," if i < len(drop_candidates) - 1 else ";"
                print(f"    DROP COLUMN {c}{sep}")
        else:
            print("  (ninguna columna por debajo del 1%)")

        if args.csv:
            with open(args.csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["column", "type", "fill_pct", "n_filled"])
                w.writeheader()
                w.writerows(rows_for_csv)
            print(f"\nVolcado CSV: {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
