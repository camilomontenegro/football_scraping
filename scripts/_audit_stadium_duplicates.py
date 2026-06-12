"""Auditoría de duplicados y datos incorrectos en dim_stadium. Solo lectura."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", encoding="utf-8")


def connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost").strip(),
        port=int(os.getenv("DB_PORT", "5432").strip()),
        dbname=os.getenv("DB_NAME", "football_db").strip(),
        user=os.getenv("DB_USER", "postgres").strip(),
        password=os.getenv("DB_PASSWORD", "").strip(),
    )


def run(cur, emit, sql, title, limit_print=50):
    emit(f"\n=== {title} ===")
    cur.execute(sql)
    rows = cur.fetchall()
    if not rows:
        emit("  (ninguno)")
        return rows
    for i, r in enumerate(rows):
        if i >= limit_print:
            emit(f"  ... y {len(rows) - limit_print} más")
            break
        emit(f"  {dict(r)}")
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, help="Guardar informe completo en archivo UTF-8.")
    args = ap.parse_args()
    out_lines: list[str] = []

    def emit(line: str = "") -> None:
        sys.stdout.write(line + "\n")
        out_lines.append(line)

    with connect() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS n FROM dim_stadium")
            total = cur.fetchone()["n"]
            cur.execute("SELECT COUNT(DISTINCT id_transfermarkt_team) AS n FROM dim_stadium")
            teams = cur.fetchone()["n"]
            cur.execute("""
                SELECT COALESCE(SUM(cnt - 1), 0) AS n FROM (
                  SELECT COUNT(*) cnt FROM dim_stadium
                  GROUP BY id_transfermarkt_team, data_hash HAVING COUNT(*) > 1
                ) s
            """)
            dup_hash = cur.fetchone()["n"]
            cur.execute("""
                SELECT COUNT(*) AS n FROM (
                  SELECT id_transfermarkt_team FROM dim_stadium
                  GROUP BY id_transfermarkt_team HAVING COUNT(*) > 1
                ) s
            """)
            multi = cur.fetchone()["n"]
            cur.execute(
                "SELECT COUNT(*) AS n FROM dim_stadium WHERE canonical_team_id IS NULL"
            )
            no_fk = cur.fetchone()["n"]
            cur.execute("""
                SELECT COUNT(*) AS n FROM dim_stadium a
                JOIN dim_stadium b ON a.id_transfermarkt_team = b.id_transfermarkt_team
                  AND a.stadium_id < b.stadium_id
                  AND a.valid_from_season <= b.valid_to_season
                  AND b.valid_from_season <= a.valid_to_season
            """)
            overlap = cur.fetchone()["n"]

            emit("=== RESUMEN dim_stadium ===")
            emit(f"  Total filas: {total}")
            emit(f"  Equipos distintos (tm_id): {teams}")
            emit(f"  Equipos con >1 fila SCD2: {multi}")
            emit(f"  Filas redundantes (mismo equipo+hash): {dup_hash}")
            emit(f"  Pares de rangos solapados: {overlap}")
            emit(f"  Sin canonical_team_id: {no_fk}")

            run(cur, emit, """
                SELECT id_transfermarkt_team, team_slug, data_hash, COUNT(*) AS n,
                       array_agg(stadium_id ORDER BY stadium_id) AS ids,
                       array_agg(DISTINCT stadium_name) AS names
                FROM dim_stadium
                GROUP BY 1, 2, 3 HAVING COUNT(*) > 1
                ORDER BY n DESC LIMIT 25
            """, "DUPLICADOS EXACTOS (mismo equipo + data_hash)")

            run(cur, emit, """
                SELECT id_transfermarkt_team, team_slug, COUNT(*) AS n,
                       array_agg(stadium_id ORDER BY stadium_id) AS ids,
                       array_agg(DISTINCT stadium_name) AS names,
                       array_agg(valid_from_season || '..' || valid_to_season
                                 ORDER BY valid_from_season) AS ranges
                FROM dim_stadium
                GROUP BY 1, 2 HAVING COUNT(*) > 1
                ORDER BY n DESC LIMIT 35
            """, "EQUIPOS CON MÚLTIPLES FILAS SCD2")

            run(cur, emit, """
                SELECT stadium_id, team_slug, stadium_name, id_transfermarkt_team
                FROM dim_stadium WHERE canonical_team_id IS NULL
                ORDER BY team_slug
            """, "SIN FK A dim_team (canonical_team_id NULL)")

            run(cur, emit, """
                SELECT stadium_id, team_slug, stadium_name, capacity, wikidata_qid
                FROM dim_stadium
                WHERE (
                  stadium_name ILIKE '%Balompi%'
                  OR stadium_name ~* '^(FC |Real |CA |AC |SC |CD |UD |RCD )'
                  OR (stadium_name ILIKE '%United%' AND stadium_name NOT ILIKE '%Stadium%')
                  OR (stadium_name ILIKE '%City%' AND stadium_name NOT ILIKE '%City Ground%'
                      AND stadium_name NOT ILIKE '%Etihad%' AND stadium_name NOT ILIKE '%stadium%')
                )
                AND stadium_name NOT ILIKE '%estadio%'
                AND stadium_name NOT ILIKE '%stadium%'
                AND stadium_name NOT ILIKE '%arena%'
                AND stadium_name NOT ILIKE '%park%'
                AND stadium_name NOT ILIKE '%field%'
                AND stadium_name NOT ILIKE '%stadion%'
                ORDER BY team_slug
            """, "NOMBRES INCORRECTOS (parecen nombre de equipo, no de estadio)", 80)

            run(cur, emit, """
                SELECT a.stadium_id AS id1, b.stadium_id AS id2, a.team_slug,
                       a.stadium_name AS n1, b.stadium_name AS n2,
                       a.valid_from_season AS f1, a.valid_to_season AS t1,
                       b.valid_from_season AS f2, b.valid_to_season AS t2,
                       a.data_hash = b.data_hash AS same_hash
                FROM dim_stadium a
                JOIN dim_stadium b ON a.id_transfermarkt_team = b.id_transfermarkt_team
                  AND a.stadium_id < b.stadium_id
                  AND a.valid_from_season <= b.valid_to_season
                  AND b.valid_from_season <= a.valid_to_season
                ORDER BY a.team_slug LIMIT 40
            """, "RANGOS TEMPORALES SOLAPADOS")

            run(cur, emit, """
                SELECT LOWER(TRIM(stadium_name)) AS stadium_name_norm,
                       COUNT(DISTINCT id_transfermarkt_team) AS teams,
                       array_agg(DISTINCT team_slug ORDER BY team_slug) AS slugs
                FROM dim_stadium WHERE stadium_name IS NOT NULL AND TRIM(stadium_name) <> ''
                GROUP BY 1 HAVING COUNT(DISTINCT id_transfermarkt_team) > 1
                ORDER BY teams DESC LIMIT 25
            """, "MISMO NOMBRE EN DISTINTOS EQUIPOS (puede ser legítimo)")

            run(cur, emit, """
                SELECT ds.stadium_id, ds.team_slug, ds.stadium_name,
                       ds.latitude, ds.longitude, ds.wikidata_qid
                FROM dim_stadium ds
                WHERE ds.latitude IS NOT NULL AND ds.longitude IS NOT NULL
                  AND (ds.latitude < -90 OR ds.latitude > 90
                       OR ds.longitude < -180 OR ds.longitude > 180
                       OR (ds.latitude = 0 AND ds.longitude = 0))
            """, "COORDENADAS INVÁLIDAS O (0,0)")

            run(cur, emit, """
                SELECT ds.stadium_id, ds.team_slug, ds.stadium_name, m.cnt AS matches_linked
                FROM dim_stadium ds
                LEFT JOIN (
                  SELECT stadium_id, COUNT(*) cnt FROM dim_match
                  WHERE stadium_id IS NOT NULL GROUP BY stadium_id
                ) m ON m.stadium_id = ds.stadium_id
                WHERE COALESCE(m.cnt, 0) = 0
                ORDER BY ds.team_slug
                LIMIT 40
            """, "ESTADIOS SIN PARTIDOS VINCULADOS (muestra)")

            cur.execute("""
                SELECT COUNT(*) AS n FROM dim_stadium ds
                LEFT JOIN (
                  SELECT stadium_id FROM dim_match WHERE stadium_id IS NOT NULL GROUP BY 1
                ) m ON m.stadium_id = ds.stadium_id
                WHERE m.stadium_id IS NULL
            """)
            orphan = cur.fetchone()["n"]
            emit(f"\n=== TOTAL estadios sin partidos vinculados: {orphan} ===")

            cur.execute("""
                SELECT COUNT(*) AS n FROM dim_stadium
                WHERE (
                  stadium_name ILIKE '%Balompi%'
                  OR stadium_name ~* '^(FC |Real |CA |AC |SC |CD |UD |RCD )'
                  OR (stadium_name ILIKE '%United%' AND stadium_name NOT ILIKE '%Stadium%')
                  OR (stadium_name ILIKE '%City%' AND stadium_name NOT ILIKE '%City Ground%'
                      AND stadium_name NOT ILIKE '%Etihad%' AND stadium_name NOT ILIKE '%stadium%')
                )
                AND stadium_name NOT ILIKE '%estadio%'
                AND stadium_name NOT ILIKE '%stadium%'
                AND stadium_name NOT ILIKE '%arena%'
                AND stadium_name NOT ILIKE '%park%'
                AND stadium_name NOT ILIKE '%field%'
                AND stadium_name NOT ILIKE '%stadion%'
            """)
            bad_names = cur.fetchone()["n"]
            emit(f"\n=== TOTAL nombres sospechosos (equipo en vez de estadio): {bad_names} ===")

            cur.execute("""
                SELECT COUNT(*) AS n FROM (
                  SELECT id_transfermarkt_team, valid_from_season
                  FROM dim_stadium
                  GROUP BY 1, 2 HAVING COUNT(*) > 1
                ) s
            """)
            dup_season = cur.fetchone()["n"]
            emit(f"=== Grupos (equipo, temporada) con >1 fila: {dup_season} ===")

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
        emit(f"\nInforme guardado: {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
