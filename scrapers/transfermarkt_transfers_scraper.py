"""
scrapers/transfermarkt_transfers_scraper.py
=============================================
Scraper de fichajes e historico de valor de mercado desde Transfermarkt CEAPI.

Usa los endpoints JSON internos de TM (sin renderizado JS):
  - Transfers: https://www.transfermarkt.co.uk/ceapi/transferHistory/list/{id}
  - Market Value: https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/{id}

Extrae datos para todos los jugadores en dim_player con id_transfermarkt != NULL.

Estructura de datos generados:
    data/raw/transfers/<player_id>.json
    data/raw/market_value/<player_id>.json
    data/clean/transfers/transfers.csv
    data/clean/market_value/market_value.csv

Uso:
    python -m scrapers.transfermarkt_transfers_scraper
    python -m scrapers.transfermarkt_transfers_scraper --limit 50
    python -m scrapers.transfermarkt_transfers_scraper --dry-run
    python -m scrapers.transfermarkt_transfers_scraper --skip-transfers
    python -m scrapers.transfermarkt_transfers_scraper --skip-market-value
    python -m scrapers.transfermarkt_transfers_scraper --force
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from sqlalchemy import text

from loaders.common import engine
from utils.data_paths import (
    DATA_ROOT,
    load_cache as _load_cache,
    save_cache as _save_cache,
)

log = logging.getLogger(__name__)

# -- CONSTANTS ----------------------------------------------------------------

DELAY_MIN = 1.0
DELAY_MAX = 3.0
MAX_RETRIES = 3
COMMIT_BATCH = 100

CACHE_NAME = "transfermarkt_transfers"

RAW_TRANSFERS_DIR = DATA_ROOT / "raw" / "transfers"
RAW_MV_DIR = DATA_ROOT / "raw" / "market_value"
CLEAN_TRANSFERS_DIR = DATA_ROOT / "clean" / "transfers"
CLEAN_MV_DIR = DATA_ROOT / "clean" / "market_value"

TRANSFERS_API = "https://www.transfermarkt.co.uk/ceapi/transferHistory/list/"
MARKET_VALUE_API = "https://www.transfermarkt.com/ceapi/marketValueDevelopment/graph/"

HEADERS = {
    "User-Agent": "football-scraping-wizard/1.0 (transfer+mv enrichment)",
    "Content-Type": "application/json",
}

# -- HELPERS ------------------------------------------------------------------


def _delay():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


def _request(url):
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("Intento %d/%d fallido para %s: %s", i + 1, MAX_RETRIES, url, e)
            time.sleep(2 * (i + 1))
    return None


def _parse_date(text_val):
    if not text_val or str(text_val).strip() in ("-", "", "?"):
        return None
    s = str(text_val).replace(".", "/").replace("-", "/").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_fee(raw):
    if not raw:
        return None, "unknown"
    clean = raw.strip().lower()
    if any(k in clean for k in ("cesion", "prestamo", "loan", "leihe")):
        return None, "loan"
    if any(k in clean for k in ("libre", "free", "ablösefrei")):
        return 0, "free"
    if any(k in clean for k in ("retirada", "retired", "karriereende")):
        return None, "retirement"
    if any(k in clean for k in ("fin de ces", "end of loan", "leihende")):
        return None, "end_of_loan"
    if clean in ("-", "?", "sin determinar", "sin coste", ""):
        return None, "unknown"
    m = re.search(r"([\d,.]+)\s*(mill|mio|mil|k)", clean)
    if m:
        num_str = m.group(1).replace(".", "").replace(",", ".")
        try:
            num = float(num_str)
        except ValueError:
            return None, "transfer"
        unit = m.group(2).lower()
        if unit in ("mill", "mio"):
            return int(num * 1_000_000), "transfer"
        if unit in ("mil", "k"):
            return int(num * 1_000), "transfer"
    m = re.search(r"([\d,.]+)", clean)
    if m:
        num_str = m.group(1).replace(".", "").replace(",", ".")
        try:
            return int(float(num_str)), "transfer"
        except ValueError:
            pass
    return None, "transfer"


def _parse_market_value(raw):
    if not raw:
        return None
    clean = raw.strip().lower().replace("€", "").strip()
    m = re.search(r"([\d,.]+)\s*(mill|mio|mil|k|bn)", clean)
    if not m:
        return None
    num_str = m.group(1).replace(".", "").replace(",", ".")
    try:
        num = float(num_str)
    except ValueError:
        return None
    unit = m.group(2).lower()
    if unit in ("mill", "mio"):
        return int(num * 1_000_000)
    if unit in ("mil", "k"):
        return int(num * 1_000)
    if unit == "bn":
        return int(num * 1_000_000_000)
    return None


def _json_default(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _save_raw(directory, player_id, data):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{player_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=_json_default)
    return path


# -- FETCH: TRANSFERS (CEAPI) ------------------------------------------------


def fetch_player_transfers(player_slug, player_id):
    """Descarga historial de fichajes via CEAPI JSON."""
    url = f"{TRANSFERS_API}{player_id}"
    r = _request(url)
    if not r:
        return []
    try:
        data = r.json()
    except Exception as e:
        log.warning("Failed to parse transfers JSON for %s: %s", player_id, e)
        return []

    items = data if isinstance(data, list) else data.get("transfers", data.get("transferHistory", []))
    if not isinstance(items, list):
        log.debug("Unexpected transfers response for %s: %s", player_id, type(data))
        return []

    transfers = []
    for t in items:
        if not isinstance(t, dict):
            continue
        season = t.get("season") or t.get("seasonID")
        date_str = t.get("dateUnformatted") or t.get("date") or t.get("transferDate")
        transfer_date = _parse_date(str(date_str)) if date_str else None

        from_obj = t.get("from") or t.get("oldClub") or {}
        if isinstance(from_obj, dict):
            from_name = from_obj.get("clubName") or from_obj.get("name")
            from_id = from_obj.get("id") or from_obj.get("clubID")
        else:
            from_name, from_id = (str(from_obj) if from_obj else None), None

        to_obj = t.get("to") or t.get("newClub") or {}
        if isinstance(to_obj, dict):
            to_name = to_obj.get("clubName") or to_obj.get("name")
            to_id = to_obj.get("id") or to_obj.get("clubID")
        else:
            to_name, to_id = (str(to_obj) if to_obj else None), None

        fee_raw = t.get("fee") or t.get("transferFee") or t.get("feeValue")
        fee_euros, transfer_type = _parse_fee(str(fee_raw) if fee_raw else "")
        is_loan = bool(t.get("loan")) or transfer_type in ("loan", "end_of_loan")

        try:
            from_id = int(from_id) if from_id else None
        except (ValueError, TypeError):
            from_id = None
        try:
            to_id = int(to_id) if to_id else None
        except (ValueError, TypeError):
            to_id = None

        if not from_name and not to_name:
            continue

        transfers.append({
            "season": str(season) if season else None,
            "transfer_date": transfer_date,
            "from_team_name": from_name,
            "from_team_id_tm": from_id,
            "to_team_name": to_name,
            "to_team_id_tm": to_id,
            "fee_raw": str(fee_raw) if fee_raw else None,
            "fee_euros": fee_euros,
            "transfer_type": transfer_type,
            "is_loan": is_loan,
        })
    return transfers


# -- FETCH: MARKET VALUE (CEAPI) ----------------------------------------------


def fetch_market_value_history(player_slug, player_id):
    """Descarga historico de valor de mercado via CEAPI JSON."""
    url = f"{MARKET_VALUE_API}{player_id}"
    r = _request(url)
    if not r:
        return []
    try:
        data = r.json()
    except Exception as e:
        log.warning("Failed to parse MV JSON for %s: %s", player_id, e)
        return []

    if isinstance(data, list):
        points = data
    elif isinstance(data, dict):
        points = (data.get("list") or data.get("data")
                  or data.get("marketValueDevelopment")
                  or data.get("market_value_development") or [])
    else:
        return []

    if not isinstance(points, list):
        return []

    values = []
    for point in points:
        if isinstance(point, dict):
            ts = point.get("x") or point.get("age") or point.get("timestamp")
            val = (point.get("y") or point.get("mw") or point.get("value")
                   or point.get("marketValue") or point.get("market_value"))
            club = (point.get("verein") or point.get("clubName")
                    or point.get("club") or point.get("club_name"))
            club_id = (point.get("verein_id") or point.get("clubId")
                       or point.get("club_id") or point.get("clubID"))
            datum = point.get("datum_mw") or point.get("date") or point.get("dateweek")

            value_date = None
            if ts and isinstance(ts, (int, float)) and ts > 1_000_000:
                value_date = datetime.fromtimestamp(ts / 1000).date()
            elif datum:
                value_date = _parse_date(str(datum))
            if value_date is None:
                continue

            if val is None:
                continue
            if isinstance(val, (int, float)):
                market_value = int(val)
            else:
                market_value = _parse_market_value(str(val))
            if market_value is None:
                continue

            try:
                club_id = int(club_id) if club_id else None
            except (ValueError, TypeError):
                club_id = None

            display = point.get("mw_display") or point.get("marketValueFormatted")
            values.append({
                "value_date": value_date,
                "market_value": market_value,
                "market_value_raw": display or f"{market_value:,} EUR",
                "club_name": club,
                "id_tm_club": club_id,
            })

        elif isinstance(point, list) and len(point) >= 2:
            ts, val = point[0], point[1]
            if isinstance(ts, (int, float)) and ts > 1_000_000:
                value_date = datetime.fromtimestamp(ts / 1000).date()
                values.append({
                    "value_date": value_date,
                    "market_value": int(val),
                    "market_value_raw": f"{int(val):,} EUR",
                    "club_name": None,
                    "id_tm_club": None,
                })
    return values


# -- ORCHESTRATOR -------------------------------------------------------------


def get_players_from_db():
    sql = text("""
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE id_transfermarkt IS NOT NULL
        ORDER BY canonical_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().fetchall()
    return [dict(r) for r in rows]


def scrape_all(dry_run=False, limit=None, skip_transfers=False,
               skip_market_value=False, force=False):
    players = get_players_from_db()
    if limit:
        players = players[:limit]

    cache = _load_cache(CACHE_NAME) if not force else {}
    stats = {"players": len(players), "transfers_scraped": 0, "mv_scraped": 0,
             "transfers_total": 0, "mv_total": 0, "skipped": 0, "errors": 0}

    log.info("Jugadores con id_transfermarkt: %d (limit=%s)", len(players), limit)

    for idx, player in enumerate(players, 1):
        pid = str(player["id_transfermarkt"])
        cid = player["canonical_id"]
        name = player["canonical_name"]

        cached = cache.get(pid, {})
        transfers_done = cached.get("transfers_scraped") and not force
        mv_done = cached.get("mv_scraped") and not force

        if (skip_transfers or transfers_done) and (skip_market_value or mv_done):
            stats["skipped"] += 1
            continue

        log.info("[%d/%d] %s (tm_id=%s, db_id=%d)", idx, len(players), name, pid, cid)

        if not skip_transfers and not transfers_done:
            if dry_run:
                log.info("  [dry-run] transfers para %s", name)
            else:
                try:
                    transfers = fetch_player_transfers(pid, pid)
                    _save_raw(RAW_TRANSFERS_DIR, pid, {
                        "player_id_tm": pid,
                        "canonical_id": cid,
                        "player_name": name,
                        "transfers": transfers,
                    })
                    stats["transfers_scraped"] += 1
                    stats["transfers_total"] += len(transfers)
                    log.info("  %d fichajes encontrados", len(transfers))
                except Exception as e:
                    log.error("  Error scraping transfers: %s", e)
                    stats["errors"] += 1
                _delay()

        if not skip_market_value and not mv_done:
            if dry_run:
                log.info("  [dry-run] market value para %s", name)
            else:
                try:
                    mv_history = fetch_market_value_history(pid, pid)
                    _save_raw(RAW_MV_DIR, pid, {
                        "player_id_tm": pid,
                        "canonical_id": cid,
                        "player_name": name,
                        "market_values": mv_history,
                    })
                    stats["mv_scraped"] += 1
                    stats["mv_total"] += len(mv_history)
                    log.info("  %d puntos de valor de mercado", len(mv_history))
                except Exception as e:
                    log.error("  Error scraping market value: %s", e)
                    stats["errors"] += 1
                _delay()

        if not dry_run:
            cache[pid] = {
                "name": name,
                "canonical_id": cid,
                "transfers_scraped": not skip_transfers or transfers_done,
                "mv_scraped": not skip_market_value or mv_done,
                "last_scraped": datetime.now().isoformat(),
            }
            if idx % COMMIT_BATCH == 0:
                _save_cache(CACHE_NAME, cache)

    if not dry_run:
        _save_cache(CACHE_NAME, cache)
    return stats


# -- TRANSFORM ----------------------------------------------------------------


def build_transfers_csv():
    if not RAW_TRANSFERS_DIR.exists():
        return None
    rows = []
    for path in sorted(RAW_TRANSFERS_DIR.glob("*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        cid = data.get("canonical_id")
        pid = data.get("player_id_tm")
        for t in data.get("transfers", []):
            fee_euros, transfer_type = _parse_fee(t.get("fee_raw", ""))
            if t.get("fee_euros") is not None:
                fee_euros = t["fee_euros"]
            if t.get("transfer_type"):
                transfer_type = t["transfer_type"]
            rows.append({
                "player_id": cid, "player_id_tm": pid,
                "season": t.get("season"), "transfer_date": t.get("transfer_date"),
                "from_team_name": t.get("from_team_name"),
                "to_team_name": t.get("to_team_name"),
                "fee_raw": t.get("fee_raw"), "fee_euros": fee_euros,
                "transfer_type": transfer_type,
                "is_loan": t.get("is_loan", False),
                "id_tm_from_team": t.get("from_team_id_tm"),
                "id_tm_to_team": t.get("to_team_id_tm"),
            })
    if not rows:
        return None
    df = pd.DataFrame(rows)
    CLEAN_TRANSFERS_DIR.mkdir(parents=True, exist_ok=True)
    path = CLEAN_TRANSFERS_DIR / "transfers.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Transfers CSV: %d filas -> %s", len(df), path)
    return df


def build_market_value_csv():
    if not RAW_MV_DIR.exists():
        return None
    rows = []
    for path in sorted(RAW_MV_DIR.glob("*.json")):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        cid = data.get("canonical_id")
        pid = data.get("player_id_tm")
        for mv in data.get("market_values", []):
            rows.append({
                "player_id": cid, "player_id_tm": pid,
                "value_date": mv.get("value_date"),
                "market_value": mv.get("market_value"),
                "market_value_raw": mv.get("market_value_raw"),
                "club_name": mv.get("club_name"),
                "id_tm_club": mv.get("id_tm_club"),
            })
    if not rows:
        return None
    df = pd.DataFrame(rows)
    CLEAN_MV_DIR.mkdir(parents=True, exist_ok=True)
    path = CLEAN_MV_DIR / "market_value.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Market Value CSV: %d filas -> %s", len(df), path)
    return df


# -- MAIN ---------------------------------------------------------------------


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")

    parser = argparse.ArgumentParser(
        description="Scraper de fichajes y valor de mercado (Transfermarkt CEAPI).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip-transfers", action="store_true")
    parser.add_argument("--skip-market-value", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--transform-only", action="store_true",
                        help="Solo genera CSVs desde raw JSONs existentes.")
    args = parser.parse_args()

    if args.transform_only:
        build_transfers_csv()
        build_market_value_csv()
        return 0

    stats = scrape_all(
        dry_run=args.dry_run, limit=args.limit,
        skip_transfers=args.skip_transfers,
        skip_market_value=args.skip_market_value,
        force=args.force,
    )

    print(f"\n{'=' * 50}")
    print(f"Jugadores procesados: {stats['players']}")
    print(f"Transfers scrapeados: {stats['transfers_scraped']} ({stats['transfers_total']} registros)")
    print(f"Market Value scrapeados: {stats['mv_scraped']} ({stats['mv_total']} puntos)")
    print(f"Saltados (cache): {stats['skipped']}")
    print(f"Errores: {stats['errors']}")

    if not args.dry_run:
        print("\nGenerando CSVs limpios...")
        build_transfers_csv()
        build_market_value_csv()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
