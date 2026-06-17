"""
loaders/market_value_transfers_loader.py
=========================================
<<<<<<< HEAD
Carga fact_transfers y fact_market_value desde CSVs con canonical_id resuelto.

CSVs esperados (producidos por scrapers/transfer_value_scraper.py):
    data/clean/transfers/transfers.csv
    data/clean/market_value/market_value.csv

Uso:
    python -m loaders.market_value_transfers_loader
    python -m loaders.market_value_transfers_loader --dry-run
    python -m loaders.market_value_transfers_loader --only transfers
    python -m loaders.market_value_transfers_loader --only market_value
"""
from __future__ import annotations

=======
Carga fact_transfers y fact_market_value desde los CSVs limpios producidos
por scrapers/transfer_value_scraper.py.
 
Los CSVs ya contienen los canonical_id resueltos desde el scraper:
    - canonical_id:           canonical_id del jugador en dim_player
    - from_team_canonical_id: canonical_id del equipo origen en dim_team
    - to_team_canonical_id:   canonical_id del equipo destino en dim_team
    - club_canonical_id:      canonical_id del club en dim_team (market value)
 
El team_map se carga como fallback por si algún canonical_id no viene
resuelto en el CSV — en ese caso se resuelve via id_transfermarkt del equipo.
 
Los nombres de equipo se normalizan con normalize_team_name() antes de
insertar para que coincidan con los nombres canónicos de SofaScore en dim_team.
Si el equipo no está en canonical_teams se guarda el nombre de Transfermarkt.
 
Uso:
    # Carga completa — inserta en fact_transfers y fact_market_value
    python -m loaders.market_value_transfers_loader

    # Simula la carga sin escribir nada en la BD — útil para verificar
    # cuántos registros se procesarían y detectar problemas antes de cargar
    python -m loaders.market_value_transfers_loader --dry-run

    # Carga solo fact_transfers (omite fact_market_value)
    python -m loaders.market_value_transfers_loader --only transfers

    # Carga solo fact_market_value (omite fact_transfers)
    python -m loaders.market_value_transfers_loader --only market_value

    # Combinar flags — dry-run solo para transfers
    python -m loaders.market_value_transfers_loader --dry-run --only transfers
"""
 
from __future__ import annotations
 
>>>>>>> origin/main
import argparse
import logging
import sys
from pathlib import Path
from typing import Optional
<<<<<<< HEAD

import pandas as pd
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine
from utils.canonical_teams import normalize_team_name
from utils.data_paths import DATA_ROOT

log = logging.getLogger(__name__)

TRANSFERS_CSV = DATA_ROOT / "clean" / "transfers" / "transfers.csv"
MV_CSV = DATA_ROOT / "clean" / "market_value" / "market_value.csv"
BATCH_SIZE = 5000

=======
 
import pandas as pd
from sqlalchemy import text
 
sys.path.append(str(Path(__file__).resolve().parent.parent))
 
from loaders.common import engine
from utils.data_paths import DATA_ROOT
from utils.canonical_teams import normalize_team_name
 
log = logging.getLogger(__name__)
 
 
# ── Rutas de los CSVs ─────────────────────────────────────────────────────────
 
TRANSFERS_CSV = DATA_ROOT / "clean" / "transfers" / "transfers.csv"
MV_CSV        = DATA_ROOT / "clean" / "market_value" / "market_value.csv"
 
 
# ── Consulta para cargar el team_map de fallback ──────────────────────────────
 
# Se usa cuando el canonical_id del equipo no viene resuelto en el CSV.
# Mapea id_transfermarkt → canonical_id de dim_team.
>>>>>>> origin/main
TEAM_MAP_SQL = text("""
    SELECT id_transfermarkt, canonical_id
    FROM dim_team
    WHERE id_transfermarkt IS NOT NULL
""")
<<<<<<< HEAD

INSERT_TRANSFER_SQL = text("""
    INSERT INTO fact_transfers (
        player_id, season, transfer_date,
        from_team_id, from_team_name,
        to_team_id, to_team_name,
        fee_raw, fee_euros, fee_currency,
        transfer_type, is_loan,
        id_tm_from_team, id_tm_to_team
    ) VALUES (
        :player_id, :season, :transfer_date,
        :from_team_id, :from_team_name,
        :to_team_id, :to_team_name,
        :fee_raw, :fee_euros, :fee_currency,
=======
 
 
# ── Queries de inserción ──────────────────────────────────────────────────────
 
INSERT_TRANSFER_SQL = text("""
    INSERT INTO fact_transfers (
        player_id,    season,         transfer_date,
        from_team_id, from_team_name,
        to_team_id,   to_team_name,
        fee_euros,      fee_currency,
        transfer_type, is_loan,
        id_tm_from_team, id_tm_to_team
    ) VALUES (
        :player_id,    :season,         :transfer_date,
        :from_team_id, :from_team_name,
        :to_team_id,   :to_team_name,
        :fee_euros,      :fee_currency,
>>>>>>> origin/main
        :transfer_type, :is_loan,
        :id_tm_from_team, :id_tm_to_team
    )
    ON CONFLICT (
<<<<<<< HEAD
        player_id, season, transfer_date,
        COALESCE(id_tm_from_team, -1), COALESCE(id_tm_to_team, -1)
    )
    DO NOTHING
""")

INSERT_MV_SQL = text("""
    INSERT INTO fact_market_value (
        player_id, value_date, market_value,
        club_id, club_name, id_tm_club
    ) VALUES (
        :player_id, :value_date, :market_value,
        :club_id, :club_name, :id_tm_club
=======
        player_id,
        season,
        transfer_date,
        COALESCE(id_tm_from_team, -1),
        COALESCE(id_tm_to_team, -1)
    )
    DO NOTHING
""")
 
# ON CONFLICT usa los mismos campos que el índice único ux_transfers_unique:
#   (player_id, season, transfer_date, id_tm_from_team, id_tm_to_team)
# Si ya existe un registro con esa combinación → DO NOTHING (ignora el duplicado)
 
INSERT_MV_SQL = text("""
    INSERT INTO fact_market_value (
        player_id, value_date, market_value,
        club_id,   club_name,  id_tm_club
    ) VALUES (
        :player_id, :value_date, :market_value,
        :club_id,   :club_name,  :id_tm_club
>>>>>>> origin/main
    )
    ON CONFLICT (player_id, value_date)
    DO UPDATE SET
        market_value = EXCLUDED.market_value,
<<<<<<< HEAD
        club_id      = COALESCE(EXCLUDED.club_id, fact_market_value.club_id),
        club_name    = COALESCE(EXCLUDED.club_name, fact_market_value.club_name),
        id_tm_club   = COALESCE(EXCLUDED.id_tm_club, fact_market_value.id_tm_club)
""")


def _safe_int(value) -> Optional[int]:
=======
        club_id      = COALESCE(EXCLUDED.club_id,    fact_market_value.club_id),
        club_name    = COALESCE(EXCLUDED.club_name,  fact_market_value.club_name),
        id_tm_club   = COALESCE(EXCLUDED.id_tm_club, fact_market_value.id_tm_club)
""")
 
# ON CONFLICT usa el índice único ux_market_value_unique: (player_id, value_date)
# Si ya existe → DO UPDATE actualiza el valor de mercado y los datos del club
# COALESCE: solo actualiza club_id/club_name/id_tm_club si el nuevo valor no es NULL
#           así no sobreescribe datos existentes con NULLs
 
 
# ── Helpers de conversión segura ──────────────────────────────────────────────
# Necesarios porque pandas representa nulos como NaN (float) en lugar de None,
# lo que causa errores al insertar en PostgreSQL.
 
def _safe_int(value) -> Optional[int]:
    """
    Convierte un valor a int de forma segura.
    Devuelve None si el valor es NaN, None o no convertible.
 
    Ejemplos:
        _safe_int(288.0)  → 288    ← pandas lee enteros como float cuando hay NaN
        _safe_int(NaN)    → None
        _safe_int(None)   → None
    """
>>>>>>> origin/main
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None
<<<<<<< HEAD


def _safe_str(value) -> Optional[str]:
=======
 
 
def _safe_str(value) -> Optional[str]:
    """
    Convierte un valor a str de forma segura.
    Devuelve None si el valor es NaN, None o string vacío.
 
    Ejemplos:
        _safe_str("Real Madrid") → "Real Madrid"
        _safe_str(NaN)           → None   ← pandas representa nulos como NaN
        _safe_str("")            → None
    """
>>>>>>> origin/main
    if value is None or pd.isna(value):
        return None
    clean_string = str(value).strip()
    return clean_string if clean_string else None
<<<<<<< HEAD


def _safe_date(value) -> Optional[str]:
=======
 
 
def _safe_date(value) -> Optional[str]:
    """
    Convierte un valor a fecha en formato 'YYYY-MM-DD' de forma segura.
    Recorta a 10 caracteres para eliminar la parte de hora si la hubiera.
    Devuelve None si el valor es NaT (nulo de fecha en pandas) o None.
 
    Ejemplos:
        _safe_date("2021-08-31 00:00:00") → "2021-08-31"
        _safe_date("2021-08-31")          → "2021-08-31"
        _safe_date(NaT)                   → None
    """
>>>>>>> origin/main
    if value is None or pd.isna(value):
        return None
    date_string = str(value).strip()[:10]
    return date_string if date_string and date_string != "NaT" else None
<<<<<<< HEAD


def _safe_bool(value) -> bool:
=======
 
 
def _safe_bool(value) -> bool:
    """
    Convierte un valor a bool de forma segura.
    Necesario porque pandas puede leer True/False del CSV como strings.
 
    Ejemplos:
        _safe_bool(True)    → True
        _safe_bool("True")  → True   ← pandas puede leerlo como string
        _safe_bool("False") → False
        _safe_bool(NaN)     → False
    """
>>>>>>> origin/main
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")
<<<<<<< HEAD


def _load_team_map() -> dict[int, int]:
    team_map: dict[int, int] = {}
    with engine.connect() as conn:
        for row in conn.execute(TEAM_MAP_SQL).mappings():
            team_map[int(row["id_transfermarkt"])] = int(row["canonical_id"])
    return team_map


def _execute_batch(conn, sql, batch: list[dict]) -> None:
    if batch:
        conn.execute(sql, batch)


def load_transfers(
    dry_run: bool = False,
    csv_path: Path | None = None,
) -> int:
    path = csv_path or TRANSFERS_CSV
    if not path.exists():
        log.warning("CSV de transfers no encontrado: %s", path)
        return 0

    df = pd.read_csv(path)
    if df.empty:
        log.info("CSV de transfers vacío")
        return 0

    log.info("Cargando %d filas de transfers desde %s...", len(df), path)
    team_map = _load_team_map()

    inserted = 0
    skipped = 0
    batch: list[dict] = []

    def flush(conn) -> None:
        nonlocal inserted
        if dry_run or not batch:
            batch.clear()
            return
        _execute_batch(conn, INSERT_TRANSFER_SQL, batch)
        inserted += len(batch)
        batch.clear()

    if dry_run:
        for _, row in df.iterrows():
            if not _safe_int(row.get("canonical_id")):
                skipped += 1
                continue
            inserted += 1
        log.info("fact_transfers: %d procesables, %d saltados (dry-run)", inserted, skipped)
        return inserted

    with engine.begin() as conn:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            player_id = _safe_int(row.get("canonical_id"))
            if not player_id:
                skipped += 1
                continue

            transfer_date = _safe_date(row.get("transfer_date"))
            season = _safe_str(row.get("season"))
            if not season and not transfer_date:
                skipped += 1
                continue

            from_id_tm = _safe_int(row.get("from_team_id_tm"))
            to_id_tm = _safe_int(row.get("to_team_id_tm"))
            from_team_id = _safe_int(row.get("from_team_canonical_id")) or team_map.get(from_id_tm)
            to_team_id = _safe_int(row.get("to_team_canonical_id")) or team_map.get(to_id_tm)

            batch.append({
                "player_id": player_id,
                "season": season,
                "transfer_date": transfer_date,
                "from_team_id": from_team_id,
                "from_team_name": normalize_team_name(_safe_str(row.get("from_team_name")) or "") or None,
                "to_team_id": to_team_id,
                "to_team_name": normalize_team_name(_safe_str(row.get("to_team_name")) or "") or None,
                "fee_raw": _safe_str(row.get("fee_raw")),
                "fee_euros": _safe_int(row.get("fee_euros")),
                "fee_currency": _safe_str(row.get("fee_currency")) or "€",
                "transfer_type": _safe_str(row.get("transfer_type")) or "unknown",
                "is_loan": _safe_bool(row.get("is_loan")),
                "id_tm_from_team": from_id_tm,
                "id_tm_to_team": to_id_tm,
            })

            if len(batch) >= BATCH_SIZE:
                flush(conn)
            if i % 50000 == 0:
                log.info("  transfers… %d/%d", i, len(df))

        flush(conn)

    log.info("fact_transfers: %d insertados, %d saltados", inserted, skipped)
    return inserted


def load_market_value(
    dry_run: bool = False,
    csv_path: Path | None = None,
) -> int:
    path = csv_path or MV_CSV
    if not path.exists():
        log.warning("CSV de market value no encontrado: %s", path)
        return 0

    df = pd.read_csv(path)
    if df.empty:
        log.info("CSV de market value vacío")
        return 0

    log.info("Cargando %d filas de market value desde %s...", len(df), path)
    team_map = _load_team_map()

    inserted = 0
    skipped = 0
    batch: list[dict] = []

    def flush(conn) -> None:
        nonlocal inserted
        if dry_run or not batch:
            batch.clear()
            return
        _execute_batch(conn, INSERT_MV_SQL, batch)
        inserted += len(batch)
        batch.clear()

    if dry_run:
        for _, row in df.iterrows():
            if not _safe_int(row.get("canonical_id")):
                skipped += 1
                continue
            if not _safe_date(row.get("value_date")) or _safe_int(row.get("market_value")) is None:
                skipped += 1
                continue
            inserted += 1
        log.info("fact_market_value: %d procesables, %d saltados (dry-run)", inserted, skipped)
        return inserted

    with engine.begin() as conn:
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            player_id = _safe_int(row.get("canonical_id"))
            if not player_id:
                skipped += 1
                continue

            value_date = _safe_date(row.get("value_date"))
            market_value = _safe_int(row.get("market_value"))
            if not value_date or market_value is None:
                skipped += 1
                continue

            id_tm_club = _safe_int(row.get("id_tm_club"))
            club_id = _safe_int(row.get("club_canonical_id")) or team_map.get(id_tm_club)

            batch.append({
                "player_id": player_id,
                "value_date": value_date,
                "market_value": market_value,
                "club_id": club_id,
                "club_name": normalize_team_name(_safe_str(row.get("club_name")) or "") or None,
                "id_tm_club": id_tm_club,
            })

            if len(batch) >= BATCH_SIZE:
                flush(conn)
            if i % 100000 == 0:
                log.info("  market_value… %d/%d", i, len(df))

        flush(conn)

    log.info("fact_market_value: %d insertados/actualizados, %d saltados", inserted, skipped)
    return inserted


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Loader de transfers y market value (canonical_id en CSV).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", choices=("transfers", "market_value"))
    parser.add_argument("--transfers-csv", type=Path, default=TRANSFERS_CSV)
    parser.add_argument("--market-value-csv", type=Path, default=MV_CSV)
    args = parser.parse_args()

    total = 0
    if args.only != "market_value":
        total += load_transfers(dry_run=args.dry_run, csv_path=args.transfers_csv)
    if args.only != "transfers":
        total += load_market_value(dry_run=args.dry_run, csv_path=args.market_value_csv)

    print(f"\nDone. {total} registros {'procesados' if args.dry_run else 'cargados'}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
=======
 
 
# ── Carga del team_map de fallback ────────────────────────────────────────────
 
def _load_team_map() -> dict:
    """
    Carga el mapeo id_transfermarkt → canonical_id desde dim_team.
    Se usa como fallback cuando el canonical_id del equipo no viene
    resuelto en el CSV.
 
    Ejemplo de estructura devuelta:
        {
            418:  12,    # id_tm Real Madrid → canonical_id 12
            131:  45,    # id_tm FC Barcelona → canonical_id 45
            1050: 78,    # id_tm Villarreal → canonical_id 78
        }
 
    Devuelve:
        dict: {id_transfermarkt (int): canonical_id (int)}
    """
    team_map = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(TEAM_MAP_SQL).mappings()
            for row in rows:
                id_transfermarkt = int(row["id_transfermarkt"])
                canonical_id     = int(row["canonical_id"])
                team_map[id_transfermarkt] = canonical_id
    except Exception as error:
        log.error("Error cargando team_map: %s", error)
    return team_map
 
 
# ── Loaders ───────────────────────────────────────────────────────────────────
 
def load_transfers(dry_run: bool = False) -> int:
    """
    Carga fact_transfers desde el CSV limpio.
 
    El canonical_id del jugador viene directamente en la columna 'canonical_id'
    del CSV — no necesita resolución via id_transfermarkt.
 
    Los canonical_id de los equipos vienen en 'from_team_canonical_id' y
    'to_team_canonical_id'. Si no vienen resueltos se usa el team_map como fallback.
 
    Los nombres de equipo se normalizan con normalize_team_name() para que
    coincidan con los nombres canónicos de SofaScore guardados en dim_team.
 
    Parámetros:
        dry_run (bool): si True solo loguea sin insertar en la BD
 
    Devuelve:
        int: número de registros procesados
    """
    if not TRANSFERS_CSV.exists():
        log.warning("CSV de transfers no encontrado: %s", TRANSFERS_CSV)
        return 0
 
    df = pd.read_csv(TRANSFERS_CSV)
    if df.empty:
        log.info("CSV de transfers vacío")
        return 0
 
    log.info("Cargando %d filas de transfers...", len(df))
 
    # team_map como fallback por si algún canonical_id no viene en el CSV
    team_map = _load_team_map()
 
    inserted = 0
    skipped  = 0
 
    with engine.begin() as conn:
        for _, row in df.iterrows():
 
            # canonical_id del jugador — viene directo en el CSV
            player_id = _safe_int(row.get("canonical_id"))
            if not player_id:
                log.debug("Fila sin canonical_id — saltando")
                skipped += 1
                continue
 
            transfer_date = _safe_date(row.get("transfer_date"))
            season        = _safe_str(row.get("season"))
 
            if not season and not transfer_date:
                log.debug("Fila sin season ni transfer_date — saltando")
                skipped += 1
                continue
 
            # IDs de Transfermarkt de los equipos — para el índice único y trazabilidad
            from_id_tm = _safe_int(row.get("from_team_id_tm"))
            to_id_tm   = _safe_int(row.get("to_team_id_tm"))
 
            # canonical_id de los equipos — viene resuelto en el CSV
            # si no viene, se resuelve via team_map como fallback
            from_team_id = _safe_int(row.get("from_team_canonical_id")) or team_map.get(from_id_tm)
            to_team_id   = _safe_int(row.get("to_team_canonical_id"))   or team_map.get(to_id_tm)
 
            # normaliza los nombres al canónico de SofaScore
            # si el equipo no está en canonical_teams devuelve el nombre de TM tal cual
            from_team_name = normalize_team_name(_safe_str(row.get("from_team_name")) or "")
            to_team_name   = normalize_team_name(_safe_str(row.get("to_team_name"))   or "")
 
            params = {
                "player_id":       player_id,
                "season":          season,
                "transfer_date":   transfer_date,
                "from_team_id":    from_team_id,
                "from_team_name":  from_team_name or None,
                "to_team_id":      to_team_id,
                "to_team_name":    to_team_name or None,
                "fee_euros":       _safe_int(row.get("fee_euros")),
                "fee_currency":    _safe_str(row.get("fee_currency")) or "€",
                "transfer_type":   _safe_str(row.get("transfer_type")) or "unknown",
                "is_loan":         _safe_bool(row.get("is_loan")),
                "id_tm_from_team": from_id_tm,
                "id_tm_to_team":   to_id_tm,
            }
 
            if dry_run:
                log.info("  [dry-run] %s → %s (fee_euros=%s, type=%s)",
                        params["from_team_name"], params["to_team_name"],
                        params["fee_euros"], params["transfer_type"])
            else:
                conn.execute(INSERT_TRANSFER_SQL, params)
            inserted += 1
 
    log.info("fact_transfers: %d insertados, %d saltados", inserted, skipped)
    return inserted
 
 
def load_market_value(dry_run: bool = False) -> int:
    """
    Carga fact_market_value desde el CSV limpio.
 
    El canonical_id del jugador viene directamente en la columna 'canonical_id'.
    El club_id viene en 'club_canonical_id'. Si no viene resuelto se usa
    el team_map como fallback via id_tm_club.
    El club_name se normaliza con normalize_team_name().
 
    Parámetros:
        dry_run (bool): si True solo loguea sin insertar en la BD
 
    Devuelve:
        int: número de registros procesados
    """
    if not MV_CSV.exists():
        log.warning("CSV de market value no encontrado: %s", MV_CSV)
        return 0
 
    df = pd.read_csv(MV_CSV)
    if df.empty:
        log.info("CSV de market value vacío")
        return 0
 
    log.info("Cargando %d filas de market value...", len(df))
 
    # team_map como fallback por si algún club_canonical_id no viene en el CSV
    team_map = _load_team_map()
 
    inserted = 0
    skipped  = 0
 
    with engine.begin() as conn:
        for _, row in df.iterrows():
 
            # canonical_id del jugador — viene directo en el CSV
            player_id = _safe_int(row.get("canonical_id"))
            if not player_id:
                log.debug("Fila sin canonical_id — saltando")
                skipped += 1
                continue
 
            value_date   = _safe_date(row.get("value_date"))
            market_value = _safe_int(row.get("market_value"))
 
            if not value_date or market_value is None:
                log.debug("Fila sin value_date o market_value — saltando")
                skipped += 1
                continue
 
            # id_transfermarkt del club — para trazabilidad y fallback de FK
            id_tm_club = _safe_int(row.get("id_tm_club"))
 
            # canonical_id del club — viene resuelto en el CSV
            # si no viene, se resuelve via team_map como fallback
            club_id = _safe_int(row.get("club_canonical_id")) or team_map.get(id_tm_club)
 
            # normaliza el nombre del club al canónico de SofaScore
            club_name = normalize_team_name(_safe_str(row.get("club_name")) or "")
 
            params = {
                "player_id":    player_id,
                "value_date":   value_date,
                "market_value": market_value,
                "club_id":      club_id,
                "club_name":    club_name or None,
                "id_tm_club":   id_tm_club,
            }
 
            if dry_run:
                log.info("  [dry-run] player=%d date=%s value=%d club=%s",
                         player_id, value_date, market_value, club_name)
            else:
                conn.execute(INSERT_MV_SQL, params)
            inserted += 1
 
    log.info("fact_market_value: %d insertados/actualizados, %d saltados", inserted, skipped)
    return inserted
 
 
# ── Punto de entrada ──────────────────────────────────────────────────────────
 
def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
 
    parser = argparse.ArgumentParser(description="Loader de transfers y market value.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula la carga sin escribir en la BD")
    parser.add_argument("--only", choices=("transfers", "market_value"),
                        help="Cargar solo una tabla")
    args = parser.parse_args()
 
    total = 0
 
    if args.only != "market_value":
        total += load_transfers(dry_run=args.dry_run)
 
    if args.only != "transfers":
        total += load_market_value(dry_run=args.dry_run)
 
    print(f"\nDone. {total} registros {'procesados' if args.dry_run else 'cargados'}.")
    return 0
 
 
if __name__ == "__main__":
    raise SystemExit(main())
>>>>>>> origin/main
