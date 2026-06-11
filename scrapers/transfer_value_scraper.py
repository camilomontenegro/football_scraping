"""
scrapers/transfer_value_scraper.py
===================================
Descarga el historial de valor de mercado y fichajes de cada jugador desde
la API interna de Transfermarkt y genera CSVs con todos los datos.

Guarda los JSON en disco para uso futuro y mantiene una caché para evitar
reprocesar jugadores ya descargados.

Estructura de archivos generados:
    data/raw/players/market_value/{id_transfermarkt}.json
    data/raw/players/transfers/{id_transfermarkt}.json
    data/raw/players/market_value.csv
    data/raw/players/transfers.csv
    data/clean/market_value/market_value.csv      ← listo para el loader
    data/clean/transfers/transfers.csv
    data/.cache/transfer_value_scraper_last_scraped.json

Uso:
    python -m scrapers.transfer_value_scraper
    python -m scrapers.transfer_value_scraper --limit 50
    python -m scrapers.transfer_value_scraper --id 583
    python -m scrapers.transfer_value_scraper --force
    python -m scrapers.transfer_value_scraper --skip-transfers
    python -m scrapers.transfer_value_scraper --skip-mv
    python -m scrapers.transfer_value_scraper --transform-only
    python -m scrapers.transfer_value_scraper --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import text

from loaders.common import engine
from utils.data_paths import DATA_ROOT, LOGS_ROOT, load_cache, save_cache
from utils.season_utils import normalize_season

log = logging.getLogger(__name__)

DELAY_MIN = 2.0
DELAY_MAX = 4.0
MAX_RETRIES = 3
COMMIT_BATCH = 50
CACHE_NAME = "transfer_value_scraper"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
}

TM_MV_URL = "https://www.transfermarkt.es/ceapi/marketValueDevelopment/graph/{id}"
TM_TRANSFERS_URL = "https://www.transfermarkt.es/ceapi/transferHistory/list/{id}"

RAW_PLAYERS_DIR = DATA_ROOT / "raw" / "players"
RAW_MV_DIR = RAW_PLAYERS_DIR / "market_value"
RAW_TRANSFERS_DIR = RAW_PLAYERS_DIR / "transfers"
MV_CSV_PATH = RAW_PLAYERS_DIR / "market_value.csv"
TRANSFERS_CSV_PATH = RAW_PLAYERS_DIR / "transfers.csv"

CLEAN_MV_DIR = DATA_ROOT / "clean" / "market_value"
CLEAN_TRANSFERS_DIR = DATA_ROOT / "clean" / "transfers"
CLEAN_MV_CSV = CLEAN_MV_DIR / "market_value.csv"
CLEAN_TRANSFERS_CSV = CLEAN_TRANSFERS_DIR / "transfers.csv"


def _setup_logging() -> None:
    log_path = LOGS_ROOT / "transfer_value_scraper.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s"))
    logging.getLogger().addHandler(file_handler)


def _load_cache() -> dict:
    return load_cache(CACHE_NAME)


def _save_cache(cache: dict) -> None:
    save_cache(CACHE_NAME, cache)


def parse_date(date_str: str) -> Optional[date]:
    if not date_str or str(date_str).strip() in ("-", ""):
        return None
    date_str = str(date_str).strip().replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def _parse_fee(raw: str | None) -> tuple[Optional[int], str, bool]:
    """Devuelve (fee_euros, transfer_type, is_loan)."""
    if not raw:
        return None, "unknown", False
    clean = str(raw).strip().lower()
    if any(k in clean for k in ("cesion", "cesión", "prestamo", "préstamo", "loan", "leihe")):
        return None, "loan", True
    if any(k in clean for k in ("libre", "free", "ablösefrei")):
        return 0, "free", False
    if any(k in clean for k in ("retirada", "retired", "karriereende")):
        return None, "retirement", False
    if any(k in clean for k in ("fin de ces", "end of loan", "leihende")):
        return None, "end_of_loan", True
    if clean in ("-", "?", "sin determinar", "sin coste", ""):
        return None, "unknown", False
    m = re.search(r"([\d,.]+)\s*(mill|mio|mil|k)", clean)
    if m:
        num_str = m.group(1).replace(".", "").replace(",", ".")
        try:
            num = float(num_str)
        except ValueError:
            return None, "transfer", False
        unit = m.group(2).lower()
        if unit in ("mill", "mio"):
            return int(num * 1_000_000), "transfer", False
        if unit in ("mil", "k"):
            return int(num * 1_000), "transfer", False
    m = re.search(r"([\d,.]+)", clean)
    if m:
        num_str = m.group(1).replace(".", "").replace(",", ".")
        try:
            return int(float(num_str)), "transfer", False
        except ValueError:
            pass
    return None, "transfer", False


def request_with_retry(url: str, retries: int = MAX_RETRIES) -> requests.Response | None:
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            log.warning("[HTTP %s] intento %d/%d — %s", code, attempt + 1, retries, url)
        except requests.exceptions.ConnectionError:
            log.warning("[CONNECTION ERROR] intento %d/%d — %s", attempt + 1, retries, url)
        except requests.exceptions.Timeout:
            log.warning("[TIMEOUT] intento %d/%d — %s", attempt + 1, retries, url)
        except Exception as e:
            log.warning("[ERROR] intento %d/%d — %s: %s", attempt + 1, retries, type(e).__name__, e)
        time.sleep(2 ** (attempt + 1))
    log.error("[FALLIDO] Se agotaron los %d reintentos para %s", retries, url)
    return None


def _append_to_csv(records: list[dict], path: Path) -> None:
    if not records:
        return
    df = pd.DataFrame(records)
    write_header = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, mode="a", index=False, header=write_header, encoding="utf-8-sig")
    log.info("%d registros guardados en %s", len(records), path.name)


def _extract_team_id_from_href(href: str) -> int | None:
    if not href:
        return None
    m = re.search(r"/verein/(\d+)", href)
    return int(m.group(1)) if m else None


def get_players(limit: int | None) -> list[tuple]:
    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE id_transfermarkt IS NOT NULL
        ORDER BY canonical_id DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    try:
        with engine.connect() as conn:
            return conn.execute(text(query)).fetchall()
    except Exception as e:
        log.error("Error al obtener jugadores de la BD: %s", e)
        return []


def get_single_player(player_id: int) -> list[tuple]:
    query = """
        SELECT canonical_id, canonical_name, id_transfermarkt
        FROM dim_player
        WHERE canonical_id = :id AND id_transfermarkt IS NOT NULL
    """
    try:
        with engine.connect() as conn:
            return conn.execute(text(query), {"id": player_id}).fetchall()
    except Exception as e:
        log.error("Error al obtener jugador con canonical_id %d: %s", player_id, e)
        return []


def get_team_map() -> dict:
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_transfermarkt, canonical_id, canonical_name
                FROM dim_team
                WHERE id_transfermarkt IS NOT NULL
            """)).fetchall()
        return {
            row[0]: {"canonical_id": row[1], "canonical_name": row[2]}
            for row in rows
        }
    except Exception as e:
        log.error("Error al obtener mapa de equipos: %s", e)
        return {}


def fetch_and_save_mv_json(id_transfermarkt: int) -> dict | None:
    url = TM_MV_URL.format(id=id_transfermarkt)
    response = request_with_retry(url)
    if not response:
        return None
    json_path = RAW_MV_DIR / f"{id_transfermarkt}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(response.text, encoding="utf-8")
    return response.json()


def extract_mv_to_csv(
    canonical_id: int,
    canonical_name: str,
    id_transfermarkt: int,
    data: dict,
) -> None:
    records = []
    for entry in data.get("list", []):
        mv = entry.get("y")
        if mv is None:
            continue
        records.append({
            "canonical_id": canonical_id,
            "canonical_name": canonical_name,
            "id_transfermarkt": id_transfermarkt,
            "market_value": int(mv),
            "market_value_raw": entry.get("mw"),
            "value_date": parse_date(entry.get("datum_mw")),
            "club_name": entry.get("verein"),
            "age": entry.get("age"),
        })
    _append_to_csv(records, MV_CSV_PATH)


def fetch_and_save_transfers_json(id_transfermarkt: int) -> dict | None:
    url = TM_TRANSFERS_URL.format(id=id_transfermarkt)
    response = request_with_retry(url)
    if not response:
        return None
    json_path = RAW_TRANSFERS_DIR / f"{id_transfermarkt}.json"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(response.text, encoding="utf-8")
    return response.json()


def extract_transfers_to_csv(
    canonical_id: int,
    canonical_name: str,
    id_transfermarkt: int,
    data: dict,
    team_map: dict,
) -> None:
    transfers = data.get("transfers", [])
    if not transfers:
        log.debug("Sin fichajes para jugador tm_id=%d", id_transfermarkt)
        return

    records = []
    for t in transfers:
        if not isinstance(t, dict):
            continue

        season = normalize_season(t.get("season"))
        transfer_date = parse_date(t.get("dateUnformatted") or t.get("date"))

        from_obj = t.get("from") or {}
        from_name = from_obj.get("clubName")
        from_href = from_obj.get("href", "")
        from_id_tm = _extract_team_id_from_href(from_href)
        from_canonical_id = team_map.get(from_id_tm, {}).get("canonical_id") if from_id_tm else None
        from_canonical_name = team_map.get(from_id_tm, {}).get("canonical_name") if from_id_tm else None

        to_obj = t.get("to") or {}
        to_name = to_obj.get("clubName")
        to_href = to_obj.get("href", "")
        to_id_tm = _extract_team_id_from_href(to_href)
        to_canonical_id = team_map.get(to_id_tm, {}).get("canonical_id") if to_id_tm else None
        to_canonical_name = team_map.get(to_id_tm, {}).get("canonical_name") if to_id_tm else None

        fee_raw = t.get("fee")
        if not from_name and not to_name:
            continue

        records.append({
            "canonical_id": canonical_id,
            "canonical_name": canonical_name,
            "id_transfermarkt": id_transfermarkt,
            "season": season,
            "transfer_date": transfer_date,
            "from_team_name": from_name,
            "from_team_id_tm": from_id_tm,
            "from_team_canonical_id": from_canonical_id,
            "from_team_canonical_name": from_canonical_name,
            "to_team_name": to_name,
            "to_team_id_tm": to_id_tm,
            "to_team_canonical_id": to_canonical_id,
            "to_team_canonical_name": to_canonical_name,
            "fee_raw": fee_raw,
        })

    _append_to_csv(records, TRANSFERS_CSV_PATH)


def _players_map_from_db() -> dict[int, tuple[int | None, str | None]]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id_transfermarkt, canonical_id, canonical_name
                FROM dim_player
                WHERE id_transfermarkt IS NOT NULL
            """)).fetchall()
        return {row[0]: (row[1], row[2]) for row in rows}
    except Exception as e:
        log.warning("No se pudo cargar el mapeo de jugadores: %s", e)
        return {}


def build_mv_csv_from_raw() -> None:
    players_map = _players_map_from_db()
    if not RAW_MV_DIR.exists():
        log.warning("No existe la carpeta %s — no hay JSON que procesar", RAW_MV_DIR)
        return

    if MV_CSV_PATH.exists():
        MV_CSV_PATH.unlink()
        log.info("CSV eliminado para regeneración: %s", MV_CSV_PATH)

    json_files = sorted(RAW_MV_DIR.glob("*.json"))
    log.info("Regenerando market value CSV desde %d archivos JSON...", len(json_files))

    for json_path in json_files:
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", json_path.name, e)
            continue

        id_transfermarkt = int(json_path.stem)
        canonical_id, canonical_name = players_map.get(id_transfermarkt, (None, None))
        extract_mv_to_csv(canonical_id, canonical_name, id_transfermarkt, data)

    log.info("Market value CSV regenerado: %s", MV_CSV_PATH)


def build_transfers_csv_from_raw() -> None:
    if not RAW_TRANSFERS_DIR.exists():
        log.warning("No existe la carpeta %s — no hay JSON que procesar", RAW_TRANSFERS_DIR)
        return

    if TRANSFERS_CSV_PATH.exists():
        TRANSFERS_CSV_PATH.unlink()
        log.info("CSV eliminado para regeneración: %s", TRANSFERS_CSV_PATH)

    players_map = _players_map_from_db()
    team_map = get_team_map()
    json_files = sorted(RAW_TRANSFERS_DIR.glob("*.json"))
    log.info("Regenerando transfers CSV desde %d archivos JSON...", len(json_files))

    for json_path in json_files:
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning("Error leyendo %s: %s", json_path.name, e)
            continue

        id_transfermarkt = int(json_path.stem)
        canonical_id, canonical_name = players_map.get(id_transfermarkt, (None, None))
        extract_transfers_to_csv(canonical_id, canonical_name, id_transfermarkt, data, team_map)

    log.info("Transfers CSV regenerado: %s", TRANSFERS_CSV_PATH)


def build_clean_csvs() -> None:
    """Genera CSVs en data/clean/ listos para loaders/transfers_loader.py."""
    if MV_CSV_PATH.exists():
        df = pd.read_csv(MV_CSV_PATH)
        if not df.empty:
            clean = pd.DataFrame({
                "player_id": df.get("canonical_id", df.get("player_id")),
                "player_id_tm": df.get("id_transfermarkt", df.get("player_id_tm")),
                "value_date": df["value_date"],
                "market_value": df["market_value"],
                "market_value_raw": df.get("market_value_raw"),
                "club_name": df.get("club_name"),
                "id_tm_club": None,
            })
            CLEAN_MV_DIR.mkdir(parents=True, exist_ok=True)
            clean.to_csv(CLEAN_MV_CSV, index=False, encoding="utf-8-sig")
            log.info("Clean MV CSV: %d filas → %s", len(clean), CLEAN_MV_CSV)

    if TRANSFERS_CSV_PATH.exists():
        df = pd.read_csv(TRANSFERS_CSV_PATH)
        if not df.empty:
            rows = []
            for _, row in df.iterrows():
                fee_euros, transfer_type, is_loan = _parse_fee(row.get("fee_raw"))
                rows.append({
                    "player_id": row.get("canonical_id") or row.get("player_id"),
                    "player_id_tm": row.get("id_transfermarkt") or row.get("player_id_tm"),
                    "season": row.get("season"),
                    "transfer_date": row.get("transfer_date"),
                    "from_team_name": row.get("from_team_name"),
                    "to_team_name": row.get("to_team_name"),
                    "fee_raw": row.get("fee_raw"),
                    "fee_euros": fee_euros,
                    "transfer_type": transfer_type,
                    "is_loan": is_loan,
                    "id_tm_from_team": row.get("from_team_id_tm") or row.get("id_tm_from_team"),
                    "id_tm_to_team": row.get("to_team_id_tm") or row.get("id_tm_to_team"),
                })
            clean = pd.DataFrame(rows)
            CLEAN_TRANSFERS_DIR.mkdir(parents=True, exist_ok=True)
            clean.to_csv(CLEAN_TRANSFERS_CSV, index=False, encoding="utf-8-sig")
            log.info("Clean transfers CSV: %d filas → %s", len(clean), CLEAN_TRANSFERS_CSV)


def scrape_players(
    players: list[tuple],
    cache: dict,
    force: bool = False,
    dry_run: bool = False,
    skip_mv: bool = False,
    skip_transfers: bool = False,
) -> dict:
    total = len(players)
    stats = {
        "total": total,
        "mv_ok": 0,
        "mv_failed": 0,
        "mv_skipped": 0,
        "transfers_ok": 0,
        "transfers_failed": 0,
        "transfers_skipped": 0,
    }
    processed_since_flush = 0
    team_map = get_team_map() if not skip_transfers else {}

    log.info(
        "Jugadores a procesar: %d (force=%s, dry_run=%s, skip_mv=%s, skip_transfers=%s)",
        total, force, dry_run, skip_mv, skip_transfers,
    )

    try:
        for i, (canonical_id, canonical_name, id_transfermarkt) in enumerate(players, 1):
            cache_key = str(id_transfermarkt)
            cached = cache.get(cache_key, {})
            mv_done = cached.get("mv_scraped") and not force
            transfers_done = cached.get("transfers_scraped") and not force

            log.info("[%d/%d] %s (tm_id=%d)", i, total, canonical_name, id_transfermarkt)

            if not skip_mv:
                if mv_done:
                    stats["mv_skipped"] += 1
                elif dry_run:
                    log.info("  [dry-run] MV: %s", TM_MV_URL.format(id=id_transfermarkt))
                else:
                    data = fetch_and_save_mv_json(id_transfermarkt)
                    if data:
                        extract_mv_to_csv(canonical_id, canonical_name, id_transfermarkt, data)
                        stats["mv_ok"] += 1
                        cached["mv_scraped"] = True
                    else:
                        stats["mv_failed"] += 1
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            if not skip_transfers:
                if transfers_done:
                    stats["transfers_skipped"] += 1
                elif dry_run:
                    log.info("  [dry-run] transfers: %s", TM_TRANSFERS_URL.format(id=id_transfermarkt))
                else:
                    data = fetch_and_save_transfers_json(id_transfermarkt)
                    if data:
                        extract_transfers_to_csv(
                            canonical_id, canonical_name, id_transfermarkt, data, team_map,
                        )
                        stats["transfers_ok"] += 1
                        cached["transfers_scraped"] = True
                    else:
                        stats["transfers_failed"] += 1
                    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            if not dry_run:
                cached["canonical_name"] = canonical_name
                cached["last_scraped"] = datetime.now().isoformat()
                cache[cache_key] = cached
                processed_since_flush += 1
                if processed_since_flush >= COMMIT_BATCH:
                    _save_cache(cache)
                    processed_since_flush = 0

    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario — guardando caché...")
        if not dry_run:
            _save_cache(cache)

    if not dry_run and processed_since_flush > 0:
        _save_cache(cache)

    return stats


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s - %(message)s",
    )
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="Scraper de historial de valor de mercado y fichajes (Transfermarkt)",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--id", type=int, default=None, dest="player_id")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-mv", action="store_true")
    parser.add_argument("--skip-transfers", action="store_true")
    parser.add_argument("--transform-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.transform_only:
        log.info("Modo transform-only — regenerando CSVs desde JSON existentes")
        if not args.skip_mv:
            build_mv_csv_from_raw()
        if not args.skip_transfers:
            build_transfers_csv_from_raw()
        build_clean_csvs()
        return 0

    cache = {} if args.force else _load_cache()
    if args.force:
        log.info("--force activado — se ignorará la caché")

    players = get_single_player(args.player_id) if args.player_id else get_players(args.limit)
    if not players:
        log.error("No se encontraron jugadores — verifica la conexión a la BD")
        return 1

    stats = scrape_players(
        players, cache,
        force=args.force,
        dry_run=args.dry_run,
        skip_mv=args.skip_mv,
        skip_transfers=args.skip_transfers,
    )

    if not args.dry_run:
        if not args.skip_mv:
            build_mv_csv_from_raw()
        if not args.skip_transfers:
            build_transfers_csv_from_raw()
        build_clean_csvs()

    print(f"\n{'=' * 50}")
    print(f"  Total jugadores:           {stats['total']}")
    print(f"  Market Value descargados:  {stats['mv_ok']}")
    print(f"  Market Value fallidos:     {stats['mv_failed']}")
    print(f"  Market Value en caché:     {stats['mv_skipped']}")
    print(f"  Transfers descargados:     {stats['transfers_ok']}")
    print(f"  Transfers fallidos:        {stats['transfers_failed']}")
    print(f"  Transfers en caché:        {stats['transfers_skipped']}")
    print(f"  CSV market value:          {MV_CSV_PATH}")
    print(f"  CSV transfers:             {TRANSFERS_CSV_PATH}")
    print(f"  Clean MV:                  {CLEAN_MV_CSV}")
    print(f"  Clean transfers:           {CLEAN_TRANSFERS_CSV}")
    print(f"{'=' * 50}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
