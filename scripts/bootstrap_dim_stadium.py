"""
scripts/bootstrap_dim_stadium.py
================================
Recrea dim_stadium desde cero con el modelo SCD2 y la carga con los CSV ya
generados por el scraper de Transfermarkt.

Util cuando se ha hecho DROP TABLE dim_stadium y se quiere volver a montar
todo sin tener que recordar el orden de comandos.

Pasos:
  1. Ejecuta el SQL de creacion (db/add_dim_stadium.sql) -- idempotente.
  2. Llama al loader SCD2 que recorre data/clean/<comp>/<season>/transfermarkt/stadiums.csv
  3. (Opcional) Compacta las filas adyacentes con mismo data_hash.

Uso:
    python -m scripts.bootstrap_dim_stadium
    python -m scripts.bootstrap_dim_stadium --dry-run        # solo informa
    python -m scripts.bootstrap_dim_stadium --no-compact     # salta compactacion
    python -m scripts.bootstrap_dim_stadium --competition la-liga
    python -m scripts.bootstrap_dim_stadium --season 2025_2026

Requiere:
  - .env con credenciales de la BD.
  - Que existan los CSV en data/clean (lanzar antes el scraper si no estan).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Imports diferidos: defined as a getter para que --help no requiera psycopg2.
def _get_engine():
    from loaders.common import engine
    return engine

log = logging.getLogger(__name__)


SQL_FILE = PROJECT_ROOT / "db" / "add_dim_stadium.sql"
MIGRATION_V3_FILE = PROJECT_ROOT / "db" / "migrate_dim_stadium_v3.sql"
MIGRATION_WIKIDATA_FILE = PROJECT_ROOT / "db" / "migrate_dim_stadium_wikidata.sql"
NAME_HISTORY_FILE = PROJECT_ROOT / "db" / "create_dim_stadium_name_history.sql"


def step_create_table(dry_run: bool) -> None:
    """Ejecuta db/add_dim_stadium.sql (idempotente). Crea tabla + indices."""
    if not SQL_FILE.exists():
        raise SystemExit(f"[!] No se encuentra {SQL_FILE}")

    print(f"\n[1/3] Creando dim_stadium desde {SQL_FILE.name}...")
    sql = SQL_FILE.read_text(encoding="utf-8")

    if dry_run:
        print("    (dry-run) SQL a ejecutar:")
        for line in sql.splitlines()[:8]:
            print("    | " + line)
        print("    | ...")
        return

    # Conexion AUTOCOMMIT: el SQL trae varios CREATE que conviene aislar.
    with _get_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(sql))
    print("    [OK] Tabla dim_stadium e indices creados.")


def step_verify_table() -> bool:
    """Comprueba que la tabla existe y tiene las columnas SCD2."""
    print("\n[verify] Comprobando estructura de dim_stadium...")
    with _get_engine().connect() as conn:
        cols = conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dim_stadium'
            ORDER BY ordinal_position
        """)).fetchall()

    if not cols:
        print("    [!] La tabla dim_stadium no existe.")
        return False

    col_names = {c[0] for c in cols}
    required = {
        "valid_from_season", "valid_to_season", "data_hash",
        "id_transfermarkt_team", "stadium_name", "is_current",
        "latitude", "longitude", "wikidata_qid",
    }
    missing = required - col_names
    if missing:
        print(f"    [!] Faltan columnas SCD2: {missing}")
        return False

    print(f"    [OK] {len(cols)} columnas, schema SCD2 confirmado.")
    return True


def step_migrate_v3(dry_run: bool) -> None:
    """Ejecuta migraciones v3 si la tabla ya existia con un esquema anterior."""
    print("\n[migrate] Asegurando columnas v3 e historial de nombres...")
    files = [MIGRATION_V3_FILE, MIGRATION_WIKIDATA_FILE, NAME_HISTORY_FILE]
    for path in files:
        if not path.exists():
            raise SystemExit(f"[!] No se encuentra {path}")
        if dry_run:
            print(f"    (dry-run) Ejecutaria {path.name}")
            continue
        sql = path.read_text(encoding="utf-8")
        with _get_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(sql))
        print(f"    [OK] {path.name}")


def step_load_csvs(competition: str | None, season: str | None,
                   dry_run: bool) -> int:
    """Carga los CSV en dim_stadium via el loader SCD2."""
    print("\n[2/3] Cargando CSVs en dim_stadium (modelo SCD2)...")
    from loaders.stadium_loader import load_stadiums

    if dry_run:
        print("    (dry-run) Se llamaria a loaders.stadium_loader.load_stadiums()")
        print(f"    filtros: competition={competition}, season={season}")
        return 0

    with _get_engine().begin() as conn:
        from scripts.compact_dim_stadium import backfill_hashes, merge_adjacent
        print("    Recalculando data_hash y fusionando filas previas...")
        backfill_hashes(conn, force=True)
        merge_adjacent(conn)
        total = load_stadiums(
            conn,
            competition=competition,
            season=season,
        )
    print(f"    [OK] Filas afectadas (inserted + extended): {total}")
    return total


def step_compact(dry_run: bool, skip: bool) -> None:
    """Backfill de hash + fusion de filas adyacentes con mismo estado."""
    if skip:
        print("\n[3/3] Compactacion saltada (--no-compact).")
        return

    print("\n[3/3] Compactando dim_stadium (backfill hash + merge adyacentes)...")

    if dry_run:
        # Ejecuta compact en modo dry-run
        from scripts.compact_dim_stadium import (
            backfill_hashes, merge_adjacent, merge_overlapping_duplicates,
        )
        with _get_engine().begin() as conn:
            n_hash = backfill_hashes(conn, dry_run=True, force=True)
            n_overlap = merge_overlapping_duplicates(conn, dry_run=True)
            n_merge = merge_adjacent(conn, dry_run=True)
        print(f"    (dry-run) data_hash a calcular:     {n_hash}")
        print(f"    (dry-run) solapamientos a fusionar: {n_overlap}")
        print(f"    (dry-run) adyacentes a fusionar:    {n_merge}")
        return

    from scripts.compact_dim_stadium import (
        backfill_hashes, merge_adjacent, merge_overlapping_duplicates,
    )
    with _get_engine().begin() as conn:
        n_hash = backfill_hashes(conn, dry_run=False, force=True)
        n_overlap = merge_overlapping_duplicates(conn, dry_run=False)
        n_merge = merge_adjacent(conn, dry_run=False)
    print(f"    data_hash calculado:     {n_hash}")
    print(f"    solapamientos fusionados: {n_overlap}")
    print(f"    adyacentes fusionados:    {n_merge}")


def step_report() -> None:
    """Imprime un resumen del estado final de la tabla."""
    print("\n[report] Estado final de dim_stadium:")
    with _get_engine().connect() as conn:
        n_rows = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()
        n_teams = conn.execute(text(
            "SELECT COUNT(DISTINCT id_transfermarkt_team) FROM dim_stadium"
        )).scalar()
        n_multi_version_teams = conn.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT id_transfermarkt_team
                FROM dim_stadium
                GROUP BY id_transfermarkt_team
                HAVING COUNT(*) >= 2
            ) x
        """)).scalar()
        n_current = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium WHERE is_current = TRUE
        """)).scalar()
        n_states_per_team = conn.execute(text("""
            SELECT id_transfermarkt_team, team_slug, COUNT(*) AS n_estados
            FROM dim_stadium
            GROUP BY id_transfermarkt_team, team_slug
            HAVING COUNT(*) > 1
            ORDER BY n_estados DESC, team_slug
            LIMIT 10
        """)).fetchall()

    print(f"    Filas totales:    {n_rows}")
    print(f"    Equipos unicos:   {n_teams}")
    print(f"    Filas vigentes:   {n_current}")
    print(f"    Equipos >=2 SCD2: {n_multi_version_teams}")
    if n_states_per_team:
        print(f"\n    Equipos con multiples estados (top 10):")
        for r in n_states_per_team:
            print(f"      {r.team_slug:<35} {r.n_estados} estados")
    else:
        print("    Todos los equipos tienen un unico estado (ningun cambio detectado).")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s -- %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Bootstrap completo de dim_stadium (SCD2).",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="No escribe nada, solo informa.")
    parser.add_argument("--no-compact", action="store_true",
                        help="Salta el paso de compactacion final.")
    parser.add_argument("--competition",
                        help="Filtrar a una sola competicion (ej: la-liga).")
    parser.add_argument("--season",
                        help="Filtrar a una sola temporada (ej: 2025_2026).")
    args = parser.parse_args()

    print("=" * 70)
    print("  BOOTSTRAP DE dim_stadium (modelo SCD2)")
    print("=" * 70)
    if args.dry_run:
        print("  Modo DRY-RUN: no se modifica nada.")

    # 1) Crear tabla
    step_create_table(args.dry_run)
    step_migrate_v3(args.dry_run)

    # Verificar
    if not args.dry_run:
        if not step_verify_table():
            raise SystemExit(1)

    # 2) Cargar CSV
    step_load_csvs(args.competition, args.season, args.dry_run)

    # 3) Compactar
    step_compact(args.dry_run, args.no_compact)

    # Reporte
    if not args.dry_run:
        step_report()

    print("\n" + "=" * 70)
    print("  Bootstrap COMPLETADO" + (" (dry-run)" if args.dry_run else ""))
    print("=" * 70)


if __name__ == "__main__":
    main()
