"""
db/migrate_data.py
==================
Exporta e importa DATOS entre bases de datos de compañeros.

Transfiere:
  • dim_stadium    — tabla completa
  • dim_match      — columnas de enriquecimiento (attendance + weather)

Las FKs internas (canonical_team_id, match_id) son SERIAL y difieren entre
BDs, así que se resuelven al importar mediante IDs de fuente estables:
  • dim_stadium  → id_transfermarkt_team (mismo en ambas BDs)
  • dim_match    → id_sofascore / id_understat / id_whoscored

Uso:
  # 1. En TU máquina: exportar a CSVs
  python db/migrate_data.py export

  # 2. Copia data/exports/ a la máquina de tu compañero

  # 3. En la máquina del COMPAÑERO: importar
  python db/migrate_data.py import

  # Opciones
  python db/migrate_data.py export --out-dir ruta/
  python db/migrate_data.py import --in-dir ruta/
  python db/migrate_data.py import --dry-run
  python db/migrate_data.py import --in-dir ruta/ --merge   # actualiza filas existentes (image_url, QID, clima)
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "football_db")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    print("ERROR: DB_PASSWORD no definido en .env")
    raise SystemExit(1)

URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DEFAULT_DIR = PROJECT_ROOT / "data" / "exports"


# ═══════════════════════════════════════════════════════════════════
# EXPORT
# ═══════════════════════════════════════════════════════════════════

# Columnas de dim_stadium que nos interesa transferir (sin PKs locales).
# Se exportan/importan solo las que existan en la BD origen/destino.
STADIUM_EXPORT_COLS = [
    "id_transfermarkt_team", "team_slug",
    "valid_from_season", "valid_to_season",
    "stadium_name", "capacity", "capacity_intl", "seats_total",
    "built_year", "owner", "operator",
    "address", "city", "country", "surface",
    "architect", "naming_rights", "previous_names_raw",
    "pitch_length_m", "pitch_width_m", "has_pitch_heating",
    "tm_url",
    "wikidata_qid", "latitude", "longitude",
    "altitude_m", "timezone", "roof_type",
    "wikipedia_url", "image_url",
    "data_hash", "data_source",
]

# Campos de dim_stadium que se fusionan en filas ya existentes (--merge).
STADIUM_MERGE_COLS = [
    "wikidata_qid", "latitude", "longitude",
    "altitude_m", "timezone", "wikipedia_url", "image_url",
    "architect", "capacity", "seats_total", "built_year",
    "city", "country", "surface", "owner", "operator", "address",
]

# Columnas de enriquecimiento de dim_match + IDs de fuente para matching.
MATCH_ENRICH_COLS = [
    "id_sofascore", "id_understat", "id_whoscored",
    "attendance",
    "temperature_c", "humidity_pct", "precipitation_mm",
    "wind_speed_kmh", "weather_code",
]


def _table_columns(conn, table: str) -> set[str]:
    rows = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :table
    """), {"table": table})
    return {r[0] for r in rows}


def _resolve_stadium_cols(conn, label: str = "export") -> list[str]:
    """Columnas de dim_stadium presentes en esta BD, en orden estable."""
    available = _table_columns(conn, "dim_stadium")
    cols = [c for c in STADIUM_EXPORT_COLS if c in available]
    missing = [c for c in STADIUM_EXPORT_COLS if c not in available]
    if missing:
        print(f"  dim_stadium ({label}): omitiendo columnas ausentes en BD: "
              f"{', '.join(missing)}")
    if not cols:
        raise RuntimeError("dim_stadium no tiene columnas exportables en común")
    return cols


def _resolve_match_cols(conn) -> list[str]:
    available = _table_columns(conn, "dim_match")
    cols = [c for c in MATCH_ENRICH_COLS if c in available]
    missing = [c for c in MATCH_ENRICH_COLS if c not in available]
    if missing:
        print(f"  dim_match: omitiendo columnas ausentes en BD: {', '.join(missing)}")
    return cols


_INT_STADIUM_COLS = {
    "id_transfermarkt_team", "capacity", "capacity_intl", "seats_total",
    "built_year", "pitch_length_m", "pitch_width_m", "altitude_m",
}
_FLOAT_STADIUM_COLS = {"latitude", "longitude"}
_BOOL_STADIUM_COLS = {"has_pitch_heating"}


def _safe_int(v) -> int | None:
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_bool(v) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).lower() in ("true", "1", "t", "yes")


def _coerce_stadium_value(col: str, value):
    if col in _INT_STADIUM_COLS:
        return _safe_int(value)
    if col in _FLOAT_STADIUM_COLS:
        return _safe_float(value)
    if col in _BOOL_STADIUM_COLS:
        return _safe_bool(value)
    return value


def _write_csv(rows: list[dict], columns: list[str], path: Path) -> int:
    """Escribe filas a CSV. Devuelve nº de filas escritas."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c) for c in columns})
    return len(rows)


def do_export(out_dir: Path):
    engine = create_engine(URL)
    print("=" * 55)
    print(f"  EXPORT desde {DB_NAME}@{DB_HOST}")
    print(f"  Destino: {out_dir}")
    print("=" * 55)

    with engine.connect() as conn:
        stadium_cols = _resolve_stadium_cols(conn, label="export")
        # ── dim_stadium ──
        r = conn.execute(text(
            f"SELECT {', '.join(stadium_cols)} FROM dim_stadium "
            "ORDER BY id_transfermarkt_team, valid_from_season"
        ))
        stadium_rows = [dict(row._mapping) for row in r]
        stadium_path = out_dir / "dim_stadium.csv"
        n = _write_csv(stadium_rows, stadium_cols, stadium_path)
        print(f"\n  dim_stadium: {n} filas -> {stadium_path}")

        # ── dim_match (solo filas con datos de enriquecimiento) ──
        match_cols = _resolve_match_cols(conn)
        if not match_cols:
            print("  dim_match enrichment: omitido (sin columnas compatibles)")
        else:
            where = (
                "attendance IS NOT NULL OR "
                "temperature_c IS NOT NULL"
            )
            r = conn.execute(text(
                f"SELECT {', '.join(match_cols)} FROM dim_match WHERE {where} "
                "ORDER BY match_date"
            ))
            match_rows = [dict(row._mapping) for row in r]
            match_path = out_dir / "dim_match_enrichment.csv"
            n = _write_csv(match_rows, match_cols, match_path)
            print(f"  dim_match enrichment: {n} filas -> {match_path}")

    print(f"\n  Copia la carpeta {out_dir} a la máquina de tu compañero")
    print("  y ejecuta:  python db/migrate_data.py import\n")


# ═══════════════════════════════════════════════════════════════════
# IMPORT
# ═══════════════════════════════════════════════════════════════════

def _read_csv(path: Path) -> list[dict]:
    """Lee CSV a lista de dicts. Convierte '' a None."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({k: (v if v != "" else None) for k, v in row.items()})
        return rows


def _legacy_row_score(row: dict) -> int:
    score = 0
    if (row.get("image_url") or "").strip():
        score += 100
    if (row.get("wikidata_qid") or "").strip():
        score += 20
    if row.get("latitude") not in (None, ""):
        score += 10
    if (row.get("wikipedia_url") or "").strip():
        score += 5
    return score


def _best_legacy_stadium_per_team(rows: list[dict]) -> dict[int, dict]:
    """Una fila representativa por id_transfermarkt_team (la más completa)."""
    by_team: dict[int, dict] = {}
    for row in rows:
        tm_id = _safe_int(row.get("id_transfermarkt_team"))
        if not tm_id or tm_id < 0:
            continue
        prev = by_team.get(tm_id)
        if not prev or _legacy_row_score(row) > _legacy_row_score(prev):
            by_team[tm_id] = row
    return by_team


def do_import(in_dir: Path, dry_run: bool = False, merge: bool = False):
    engine = create_engine(URL)
    print("=" * 55)
    print(f"  IMPORT a {DB_NAME}@{DB_HOST}")
    print(f"  Fuente: {in_dir}")
    if dry_run:
        print("  *** MODO DRY-RUN: no se aplican cambios ***")
    if merge:
        print("  *** MERGE: rellena NULL en filas existentes (no inserta duplicados) ***")
    print("=" * 55)

    stadium_file = in_dir / "dim_stadium.csv"
    match_file   = in_dir / "dim_match_enrichment.csv"

    if not stadium_file.exists() and not match_file.exists():
        print(f"\n  ERROR: No se encontraron archivos en {in_dir}")
        print("  Esperados: dim_stadium.csv, dim_match_enrichment.csv")
        raise SystemExit(1)

    with engine.begin() as conn:
        # ── dim_stadium ──
        if stadium_file.exists():
            print(f"\n[1/2] Importando dim_stadium...")
            if merge:
                _merge_stadium_metadata(conn, stadium_file, dry_run)
            else:
                _import_stadiums(conn, stadium_file, dry_run)
        else:
            print(f"\n[1/2] dim_stadium.csv no encontrado, omitido")

        # ── dim_match enrichment ──
        if match_file.exists():
            print(f"\n[2/2] Importando enriquecimiento dim_match...")
            _import_match_enrichment(conn, match_file, dry_run)
        else:
            print(f"\n[2/2] dim_match_enrichment.csv no encontrado, omitido")

        if dry_run:
            conn.execute(text("SELECT 1"))
            print("\n  Dry-run completado. No se ha modificado nada.")
            conn.rollback()
            return

    print("\n  Importación completada.\n")


def _import_stadiums(conn, path: Path, dry_run: bool):
    """Importa dim_stadium resolviendo canonical_team_id por id_transfermarkt."""
    rows = _read_csv(path)
    if not rows:
        print("  dim_stadium: CSV vacío")
        return

    csv_cols = list(rows[0].keys())
    dest_cols = _resolve_stadium_cols(conn, label="import")
    data_cols = [c for c in dest_cols if c in csv_cols]
    if not data_cols:
        print("  dim_stadium: CSV sin columnas compatibles con la BD destino")
        return

    inserted = skipped = no_team = 0

    for row in rows:
        tm_team_id = _safe_int(row.get("id_transfermarkt_team"))
        if not tm_team_id:
            skipped += 1
            continue

        # Resolver canonical_team_id en la BD destino
        r = conn.execute(text(
            "SELECT canonical_id FROM dim_team WHERE id_transfermarkt = :tid"
        ), {"tid": tm_team_id})
        team_row = r.fetchone()
        canonical_team_id = team_row[0] if team_row else None

        if not canonical_team_id:
            no_team += 1
            continue

        # Comprobar si ya existe (por id_transfermarkt_team + valid_from_season)
        r = conn.execute(text(
            "SELECT 1 FROM dim_stadium "
            "WHERE id_transfermarkt_team = :tid AND valid_from_season = :vfs"
        ), {"tid": tm_team_id, "vfs": row["valid_from_season"]})
        if r.fetchone():
            skipped += 1
            continue

        if dry_run:
            inserted += 1
            continue

        insert_cols = ["canonical_team_id"] + data_cols
        params = {"canonical_team_id": canonical_team_id}
        for col in data_cols:
            params[col] = _coerce_stadium_value(col, row.get(col))

        col_sql = ", ".join(insert_cols)
        val_sql = ", ".join(f":{c}" for c in insert_cols)
        conn.execute(
            text(f"INSERT INTO dim_stadium ({col_sql}) VALUES ({val_sql})"),
            params,
        )
        inserted += 1

    verb = "se insertarían" if dry_run else "insertadas"
    print(f"  dim_stadium: {inserted} {verb} | {skipped} ya existían | {no_team} sin equipo en dim_team")


def _merge_stadium_metadata(conn, path: Path, dry_run: bool) -> None:
    """Rellena image_url, wikidata_qid, coords, etc. en filas existentes por id_transfermarkt_team."""
    rows = _read_csv(path)
    if not rows:
        print("  dim_stadium merge: CSV vacío")
        return

    dest_cols = [c for c in STADIUM_MERGE_COLS if c in _table_columns(conn, "dim_stadium")]
    if not dest_cols:
        print("  dim_stadium merge: sin columnas compatibles en BD")
        return

    by_team = _best_legacy_stadium_per_team(rows)
    updated_rows = skipped = no_team = 0

    _str_cols = set(dest_cols) - _INT_STADIUM_COLS - _FLOAT_STADIUM_COLS - _BOOL_STADIUM_COLS
    set_parts = []
    for col in dest_cols:
        if col in _str_cols:
            set_parts.append(
                f"{col} = COALESCE(NULLIF(TRIM({col}::text), ''), :{col})",
            )
        elif col in _BOOL_STADIUM_COLS:
            set_parts.append(f"{col} = COALESCE({col}, :{col})")
        else:
            set_parts.append(f"{col} = COALESCE({col}, :{col})")
    set_clause = ", ".join(set_parts)

    for tm_id, row in by_team.items():
        r = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE id_transfermarkt = :tid"),
            {"tid": tm_id},
        )
        if not r.fetchone():
            no_team += 1
            continue

        r = conn.execute(
            text(
                "SELECT COUNT(*) FROM dim_stadium WHERE id_transfermarkt_team = :tid",
            ),
            {"tid": tm_id},
        )
        if r.scalar() == 0:
            skipped += 1
            continue

        if dry_run:
            updated_rows += conn.execute(
                text(
                    "SELECT COUNT(*) FROM dim_stadium WHERE id_transfermarkt_team = :tid",
                ),
                {"tid": tm_id},
            ).scalar()
            continue

        params: dict = {"tid": tm_id}
        for col in dest_cols:
            params[col] = _coerce_stadium_value(col, row.get(col))

        result = conn.execute(
            text(f"""
                UPDATE dim_stadium
                SET {set_clause},
                    updated_at = NOW()
                WHERE id_transfermarkt_team = :tid
            """),
            params,
        )
        updated_rows += result.rowcount

        # Propagar a filas sintéticas del mismo equipo (id TM negativo)
        canon = conn.execute(
            text("SELECT canonical_id FROM dim_team WHERE id_transfermarkt = :tid"),
            {"tid": tm_id},
        ).scalar()
        if canon:
            params_sib = {**params, "cid": canon}
            conn.execute(
                text(f"""
                    UPDATE dim_stadium
                    SET {set_clause},
                        updated_at = NOW()
                    WHERE canonical_team_id = :cid
                      AND id_transfermarkt_team < 0
                """),
                params_sib,
            )

    verb = "filas se actualizarían" if dry_run else "filas actualizadas"
    print(
        f"  dim_stadium merge: {updated_rows} {verb} | "
        f"{len(by_team)} equipos en CSV | {skipped} sin fila en BD | {no_team} sin dim_team",
    )


def _import_match_enrichment(conn, path: Path, dry_run: bool):
    """Actualiza columnas de enriquecimiento en dim_match.

    Matching por id_sofascore > id_understat > id_whoscored (primer ID no nulo).
    Solo escribe en columnas que estén a NULL (nunca sobreescribe datos existentes).
    """
    rows = _read_csv(path)
    updated = skipped = no_match = 0

    for row in rows:
        ss_id = _safe_int(row.get("id_sofascore"))
        us_id = _safe_int(row.get("id_understat"))
        ws_id = _safe_int(row.get("id_whoscored"))

        if not ss_id and not us_id and not ws_id:
            skipped += 1
            continue

        # Buscar el match en la BD destino por el primer ID disponible
        match_id = None
        for col, val in [("id_sofascore", ss_id), ("id_understat", us_id), ("id_whoscored", ws_id)]:
            if val is None:
                continue
            r = conn.execute(text(
                f"SELECT match_id FROM dim_match WHERE {col} = :v"
            ), {"v": val})
            found = r.fetchone()
            if found:
                match_id = found[0]
                break

        if not match_id:
            no_match += 1
            continue

        if dry_run:
            updated += 1
            continue

        # UPDATE solo columnas que estén a NULL (no machacar datos existentes)
        conn.execute(text("""
            UPDATE dim_match SET
                attendance       = COALESCE(attendance,       :attendance),
                temperature_c    = COALESCE(temperature_c,    :temperature_c),
                humidity_pct     = COALESCE(humidity_pct,     :humidity_pct),
                precipitation_mm = COALESCE(precipitation_mm, :precipitation_mm),
                wind_speed_kmh   = COALESCE(wind_speed_kmh,   :wind_speed_kmh),
                weather_code     = COALESCE(weather_code,     :weather_code)
            WHERE match_id = :mid
        """), {
            "mid":              match_id,
            "attendance":       _safe_int(row.get("attendance")),
            "temperature_c":    _safe_float(row.get("temperature_c")),
            "humidity_pct":     _safe_int(row.get("humidity_pct")),
            "precipitation_mm": _safe_float(row.get("precipitation_mm")),
            "wind_speed_kmh":   _safe_float(row.get("wind_speed_kmh")),
            "weather_code":     _safe_int(row.get("weather_code")),
        })
        updated += 1

    verb = "se actualizarían" if dry_run else "actualizados"
    print(f"  dim_match: {updated} {verb} | {skipped} sin IDs | {no_match} sin match en destino")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Exporta/importa datos de dim_stadium y enriquecimiento de dim_match entre BDs.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    exp = sub.add_parser("export", help="Exporta datos de tu BD a CSVs")
    exp.add_argument("--out-dir", type=Path, default=DEFAULT_DIR,
                     help=f"Carpeta destino (default: {DEFAULT_DIR})")

    imp = sub.add_parser("import", help="Importa CSVs a la BD")
    imp.add_argument("--in-dir", type=Path, default=DEFAULT_DIR,
                     help=f"Carpeta con los CSVs (default: {DEFAULT_DIR})")
    imp.add_argument("--dry-run", action="store_true",
                     help="Muestra qué haría sin tocar la BD")
    imp.add_argument("--merge", action="store_true",
                     help="Fusiona metadata en dim_stadium existente (image_url, QID, coords)")

    args = parser.parse_args()

    if args.command == "export":
        do_export(args.out_dir)
    elif args.command == "import":
        do_import(args.in_dir, dry_run=args.dry_run, merge=args.merge)


if __name__ == "__main__":
    main()
