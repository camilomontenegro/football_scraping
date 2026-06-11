"""
loaders/transfers_loader.py
============================
Carga fact_transfers y fact_market_value desde los CSV limpios producidos
por scrapers/transfer_value_scraper.py o scrapers/transfermarkt_transfers_scraper.py.

Resolución de FKs:
    - player_id:   dim_player.id_transfermarkt
    - from_team_id / to_team_id / club_id:  dim_team.id_transfermarkt

Uso:
    python -m loaders.transfers_loader
    python -m loaders.transfers_loader --dry-run
    python -m loaders.transfers_loader --only transfers
    python -m loaders.transfers_loader --only market_value
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent.parent))

from loaders.common import engine
from utils.data_paths import DATA_ROOT

log = logging.getLogger(__name__)

CLEAN_TRANSFERS_CSV = DATA_ROOT / "clean" / "transfers" / "transfers.csv"
CLEAN_MV_CSV = DATA_ROOT / "clean" / "market_value" / "market_value.csv"

# ── FK RESOLUTION QUERIES ────────────────────────────────────────────────────

TEAM_MAP_SQL = text("""
    SELECT id_transfermarkt, canonical_id
    FROM dim_team
    WHERE id_transfermarkt IS NOT NULL
""")

PLAYER_MAP_SQL = text("""
    SELECT id_transfermarkt, canonical_id
    FROM dim_player
    WHERE id_transfermarkt IS NOT NULL
""")

# ── UPSERT QUERIES ──────────────────────────────────────────────────────────

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
        :fee_raw, :fee_euros, '€',
        :transfer_type, :is_loan,
        :id_tm_from_team, :id_tm_to_team
    )
    ON CONFLICT (player_id, season, transfer_date,
                 COALESCE(id_tm_from_team, -1), COALESCE(id_tm_to_team, -1))
    DO NOTHING
""")

INSERT_MV_SQL = text("""
    INSERT INTO fact_market_value (
        player_id, value_date, market_value, market_value_raw,
        club_id, club_name, id_tm_club
    ) VALUES (
        :player_id, :value_date, :market_value, :market_value_raw,
        :club_id, :club_name, :id_tm_club
    )
    ON CONFLICT (player_id, value_date)
    DO UPDATE SET
        market_value     = EXCLUDED.market_value,
        market_value_raw = EXCLUDED.market_value_raw,
        club_id          = COALESCE(EXCLUDED.club_id, fact_market_value.club_id),
        club_name        = COALESCE(EXCLUDED.club_name, fact_market_value.club_name),
        id_tm_club       = COALESCE(EXCLUDED.id_tm_club, fact_market_value.id_tm_club)
""")


# ── HELPERS ──────────────────────────────────────────────────────────────────


def _safe_int(val) -> Optional[int]:
    if val is None or pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _safe_str(val) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


def _safe_date(val) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()[:10]
    return s if s and s != "NaT" else None


def _safe_bool(val) -> bool:
    if val is None or pd.isna(val):
        return False
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


# ── LOADERS ──────────────────────────────────────────────────────────────────


def load_transfers(dry_run: bool = False) -> int:
    """Carga fact_transfers desde el CSV limpio."""
    if not CLEAN_TRANSFERS_CSV.exists():
        log.warning("CSV de transfers no encontrado: %s", CLEAN_TRANSFERS_CSV)
        return 0

    df = pd.read_csv(CLEAN_TRANSFERS_CSV)
    if df.empty:
        log.info("CSV de transfers vacío")
        return 0

    # Build FK maps
    with engine.connect() as conn:
        team_map = {
            int(r["id_transfermarkt"]): int(r["canonical_id"])
            for r in conn.execute(TEAM_MAP_SQL).mappings()
        }
        player_map = {
            int(r["id_transfermarkt"]): int(r["canonical_id"])
            for r in conn.execute(PLAYER_MAP_SQL).mappings()
        }

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            # Resolve player FK
            pid_tm = _safe_int(row.get("player_id_tm")) or _safe_int(row.get("id_transfermarkt"))
            player_id = _safe_int(row.get("player_id")) or _safe_int(row.get("canonical_id"))
            if not player_id and pid_tm:
                player_id = player_map.get(pid_tm)
            if not player_id:
                skipped += 1
                continue

            # Resolve team FKs
            from_tm = _safe_int(row.get("id_tm_from_team")) or _safe_int(row.get("from_team_id_tm"))
            to_tm = _safe_int(row.get("id_tm_to_team")) or _safe_int(row.get("to_team_id_tm"))
            from_team_id = team_map.get(from_tm) if from_tm else None
            to_team_id = team_map.get(to_tm) if to_tm else None

            transfer_date = _safe_date(row.get("transfer_date"))
            season = _safe_str(row.get("season"))

            if not season and not transfer_date:
                skipped += 1
                continue

            params = {
                "player_id": player_id,
                "season": season,
                "transfer_date": transfer_date,
                "from_team_id": from_team_id,
                "from_team_name": _safe_str(row.get("from_team_name")),
                "to_team_id": to_team_id,
                "to_team_name": _safe_str(row.get("to_team_name")),
                "fee_raw": _safe_str(row.get("fee_raw")),
                "fee_euros": _safe_int(row.get("fee_euros")),
                "transfer_type": _safe_str(row.get("transfer_type")) or "unknown",
                "is_loan": _safe_bool(row.get("is_loan")),
                "id_tm_from_team": from_tm,
                "id_tm_to_team": to_tm,
            }

            if dry_run:
                log.info("  [dry-run] transfer: %s → %s (%s)",
                         params["from_team_name"], params["to_team_name"], params["fee_raw"])
            else:
                conn.execute(INSERT_TRANSFER_SQL, params)
            inserted += 1

    log.info("fact_transfers: %d insertados, %d saltados", inserted, skipped)
    return inserted


def load_market_value(dry_run: bool = False) -> int:
    """Carga fact_market_value desde el CSV limpio."""
    if not CLEAN_MV_CSV.exists():
        log.warning("CSV de market value no encontrado: %s", CLEAN_MV_CSV)
        return 0

    df = pd.read_csv(CLEAN_MV_CSV)
    if df.empty:
        log.info("CSV de market value vacío")
        return 0

    with engine.connect() as conn:
        team_map = {
            int(r["id_transfermarkt"]): int(r["canonical_id"])
            for r in conn.execute(TEAM_MAP_SQL).mappings()
        }
        player_map = {
            int(r["id_transfermarkt"]): int(r["canonical_id"])
            for r in conn.execute(PLAYER_MAP_SQL).mappings()
        }

    inserted = 0
    skipped = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            pid_tm = _safe_int(row.get("player_id_tm")) or _safe_int(row.get("id_transfermarkt"))
            player_id = _safe_int(row.get("player_id")) or _safe_int(row.get("canonical_id"))
            if not player_id and pid_tm:
                player_id = player_map.get(pid_tm)
            if not player_id:
                skipped += 1
                continue

            value_date = _safe_date(row.get("value_date"))
            market_value = _safe_int(row.get("market_value"))
            if not value_date or market_value is None:
                skipped += 1
                continue

            club_tm = _safe_int(row.get("id_tm_club"))
            club_id = team_map.get(club_tm) if club_tm else None

            params = {
                "player_id": player_id,
                "value_date": value_date,
                "market_value": market_value,
                "market_value_raw": _safe_str(row.get("market_value_raw")),
                "club_id": club_id,
                "club_name": _safe_str(row.get("club_name")),
                "id_tm_club": club_tm,
            }

            if dry_run:
                log.info("  [dry-run] mv: %s = %s", value_date, market_value)
            else:
                conn.execute(INSERT_MV_SQL, params)
            inserted += 1

    log.info("fact_market_value: %d insertados/actualizados, %d saltados", inserted, skipped)
    return inserted


# ── MAIN ─────────────────────────────────────────────────────────────────────


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

    parser = argparse.ArgumentParser(description="Loader de transfers y market value.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only", choices=("transfers", "market_value"),
                        help="Cargar solo una tabla.")
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
