"""
scrapers/backfill_attendance.py
================================
Rellena dim_match.attendance para partidos que ya tienen id_sofascore
usando el endpoint de detalle de SofaScore via Selenium (igual que sofascore_generico).

Actualiza dim_match directamente. Guarda cada 50 partidos y al interrumpir con Ctrl+C.

Uso:
    python -m scrapers.backfill_attendance --limit 5 --dry-run
    python -m scrapers.backfill_attendance --limit 50
    python -m scrapers.backfill_attendance
    python -m scrapers.backfill_attendance --force
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import text

import pandas as pd

from loaders.common import engine
from utils.chrome_driver import create_chrome_driver
from utils.data_paths import DATA_ROOT, load_cache, save_cache

log = logging.getLogger(__name__)

# -- CONFIG -------------------------------------------------------------------

SOFASCORE_EVENT_API = "https://api.sofascore.com/api/v1/event/"
DELAY_MIN = 2.0
DELAY_MAX = 4.0
COMMIT_BATCH = 50
PROGRESS_EVERY = 25   # resumen periódico aunque no haya attendance
DETAIL_FIRST = 5      # detalle partido a partido solo al inicio
CACHE_NAME = "backfill_attendance"

RAW_DIR = DATA_ROOT / "raw" / "attendance"
CLEAN_DIR = DATA_ROOT / "clean" / "attendance"


# -- SELENIUM HELPERS (mismo patron que sofascore_generico) --------------------


def _create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-images")
    options.add_argument("--disable-extensions")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.7727.117 Safari/537.36"
    )
    options.page_load_strategy = "eager"
    driver = create_chrome_driver(options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def _get_json(driver, url, timeout=5):
    driver.get(url)
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: len(d.find_element("tag name", "body").text.strip()) > 0
        )
    except Exception:
        pass
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    try:
        return json.loads(driver.find_element("tag name", "body").text)
    except (json.JSONDecodeError, Exception):
        return {}


# -- CORE LOGIC ---------------------------------------------------------------


def _extract_attendance(event):
    att = event.get("attendance")
    if att is None:
        venue = event.get("venue") or {}
        att = venue.get("attendance")
    if att is not None:
        try:
            val = int(att)
            return val if val > 0 else None
        except (ValueError, TypeError):
            pass
    return None


def get_matches_to_fill(force=False):
    where = "m.id_sofascore IS NOT NULL"
    if not force:
        where += " AND m.attendance IS NULL"

    sql = text(f"""
        SELECT m.match_id, m.id_sofascore, m.match_date,
               ht.canonical_name AS home_team,
               at2.canonical_name AS away_team
        FROM dim_match m
        LEFT JOIN dim_team ht ON m.home_team_id = ht.canonical_id
        LEFT JOIN dim_team at2 ON m.away_team_id = at2.canonical_id
        WHERE {where}
        ORDER BY m.match_date DESC, m.match_id
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().fetchall()
    return [dict(r) for r in rows]


def _save_raw_event(ss_id, event_data):
    """Guarda el JSON crudo del evento en data/raw/attendance/{ss_id}.json."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    path = RAW_DIR / f"{ss_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(event_data, f, ensure_ascii=False, indent=2, default=str)


def _flush_clean_csv(all_rows):
    """Regenera data/clean/attendance/attendance.csv con todos los resultados acumulados."""
    if not all_rows:
        return
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    path = CLEAN_DIR / "attendance.csv"
    df = pd.DataFrame(all_rows)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log.info("Clean CSV: %d filas -> %s", len(df), path)


def _flush_batch(updates):
    with engine.begin() as conn:
        for u in updates:
            conn.execute(text("""
                UPDATE dim_match SET attendance = :attendance
                WHERE match_id = :match_id
            """), u)


def _format_eta(elapsed: float, done: int, total: int) -> str:
    if done <= 0 or elapsed <= 0:
        return "?"
    remaining = (elapsed / done) * (total - done)
    mins, secs = divmod(int(remaining), 60)
    if mins >= 60:
        hrs, mins = divmod(mins, 60)
        return f"{hrs}h {mins}m"
    return f"{mins}m {secs}s"


def _log_progress(idx, total, updated, no_data, errors, elapsed, detail=None):
    pct = 100.0 * idx / total if total else 0
    eta = _format_eta(elapsed, idx, total)
    line = (
        f"  [{idx}/{total}] {pct:5.1f}%  "
        f"ok={updated}  sin_dato={no_data}  err={errors}  "
        f"ETA ~{eta}"
    )
    if detail:
        line += f"  |  {detail}"
    print(line, flush=True)


def backfill(dry_run=False, limit=None, force=False):
    matches = get_matches_to_fill(force=force)

    # Cargar cache: ss_id -> "ok" (tiene dato) | "no_data" (consultado, sin dato)
    cache = load_cache(CACHE_NAME) if not force else {}
    cached_count = 0

    # Filtrar partidos ya consultados sin resultado
    filtered = []
    for m in matches:
        ss_key = str(m["id_sofascore"])
        if ss_key in cache:
            cached_count += 1
            continue
        filtered.append(m)

    if limit:
        filtered = filtered[:limit]

    total = len(filtered)
    print(f"\n{'='*60}")
    print(f"  Backfill attendance via SofaScore (Selenium)")
    print(f"  Pendientes DB: {len(matches)}  |  Ya en cache: {cached_count}")
    print(f"  A procesar: {total}  |  Modo: {'DRY-RUN' if dry_run else 'LIVE'}")
    print(f"{'='*60}\n")

    if total == 0:
        print("  Nada que procesar.")
        return

    # Cargar filas previas del clean CSV para acumular
    clean_path = CLEAN_DIR / "attendance.csv"
    if clean_path.exists():
        try:
            existing_df = pd.read_csv(clean_path)
            clean_rows = existing_df.to_dict("records")
        except Exception:
            clean_rows = []
    else:
        clean_rows = []
    existing_ss_ids = {str(r.get("id_sofascore")) for r in clean_rows}

    driver = _create_driver(headless=True)
    updated = 0
    no_data = 0
    errors = 0
    batch = []
    cache_dirty = False
    t0 = time.monotonic()

    try:
        for idx, match in enumerate(filtered, 1):
            ss_id = match["id_sofascore"]
            ss_key = str(ss_id)
            home = match.get("home_team") or "?"
            away = match.get("away_team") or "?"
            date = match.get("match_date") or "?"

            url = f"{SOFASCORE_EVENT_API}{ss_id}"
            data = _get_json(driver, url)
            event = data.get("event", data)
            elapsed = time.monotonic() - t0
            detail = None

            if not event or not isinstance(event, dict):
                errors += 1
                cache[ss_key] = "error"
                cache_dirty = True
                detail = f"ERROR {home} vs {away} ({date})"
            else:
                # Guardar raw JSON del evento
                if not dry_run:
                    _save_raw_event(ss_id, event)

                attendance = _extract_attendance(event)

                if attendance is None:
                    no_data += 1
                    cache[ss_key] = "no_data"
                    cache_dirty = True
                    if idx <= DETAIL_FIRST:
                        detail = f"{home} vs {away} ({date}) — sin dato"
                else:
                    updated += 1
                    cache[ss_key] = "ok"
                    cache_dirty = True
                    detail = f"{home} vs {away} ({date}) — {attendance:,}"

                    # Acumular fila para el clean CSV
                    if ss_key not in existing_ss_ids:
                        clean_rows.append({
                            "match_id": match["match_id"],
                            "id_sofascore": ss_id,
                            "match_date": date,
                            "home_team": home,
                            "away_team": away,
                            "attendance": attendance,
                        })
                        existing_ss_ids.add(ss_key)

                    if not dry_run:
                        batch.append({"match_id": match["match_id"], "attendance": attendance})

            show_progress = (
                idx <= DETAIL_FIRST
                or idx % PROGRESS_EVERY == 0
                or idx == total
                or detail and "—" in detail and "sin dato" not in detail  # siempre si hay attendance
            )
            if show_progress:
                _log_progress(idx, total, updated, no_data, errors, elapsed, detail)

            # Flush DB batch + cache + clean CSV cada COMMIT_BATCH
            if not dry_run and len(batch) >= COMMIT_BATCH:
                _flush_batch(batch)
                batch.clear()
                _flush_clean_csv(clean_rows)
                print(f"  >> Commit BD+cache: {updated} attendance guardados en total", flush=True)
            if cache_dirty and idx % COMMIT_BATCH == 0:
                save_cache(CACHE_NAME, cache)
                cache_dirty = False

    except KeyboardInterrupt:
        print("\n  Interrumpido — guardando progreso...", flush=True)
    finally:
        if not dry_run and batch:
            _flush_batch(batch)
        if not dry_run and clean_rows:
            _flush_clean_csv(clean_rows)
        if cache_dirty:
            save_cache(CACHE_NAME, cache)
        try:
            driver.quit()
        except Exception:
            pass

    print(f"\n{'='*60}")
    print(f"  Actualizados: {updated}  |  Sin dato: {no_data}  |  Errores: {errors}")
    print(f"  Cache total: {len(cache)} entradas")
    print(f"  Raw JSONs: {RAW_DIR}")
    print(f"  Clean CSV: {clean_path} ({len(clean_rows)} filas)")
    print(f"{'='*60}")


# -- CLI ----------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s - %(message)s")
    parser = argparse.ArgumentParser(
        description="Backfill dim_match.attendance desde SofaScore (Selenium).")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch aunque ya tenga attendance.")
    args = parser.parse_args()
    backfill(dry_run=args.dry_run, limit=args.limit, force=args.force)


if __name__ == "__main__":
    main()
