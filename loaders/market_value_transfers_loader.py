"""
loaders/market_value_transfers_loader.py
=========================================
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

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

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

TEAM_MAP_SQL = text("""
    SELECT id_transfermarkt, canonical_id
    FROM dim_team
    WHERE id_transfermarkt IS NOT NULL
""")

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
        :transfer_type, :is_loan,
        :id_tm_from_team, :id_tm_to_team
    )
    ON CONFLICT (
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
    )
    ON CONFLICT (player_id, value_date)
    DO UPDATE SET
        market_value = EXCLUDED.market_value,
        club_id      = COALESCE(EXCLUDED.club_id, fact_market_value.club_id),
        club_name    = COALESCE(EXCLUDED.club_name, fact_market_value.club_name),
        id_tm_club   = COALESCE(EXCLUDED.id_tm_club, fact_market_value.id_tm_club)
""")


def _safe_int(value) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def _safe_str(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    clean_string = str(value).strip()
    return clean_string if clean_string else None


def _safe_date(value) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    date_string = str(value).strip()[:10]
    return date_string if date_string and date_string != "NaT" else None


def _safe_bool(value) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


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
