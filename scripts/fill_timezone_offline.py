"""
scripts/fill_timezone_offline.py
================================
Rellena dim_stadium.timezone usando lat/lon ya cargados (Wikidata).

Usa la lib `timezonefinder` (offline, sin red). Cada lat/lon resuelve
a un timezone IANA (e.g. "Europe/Madrid"). Polite, idempotente.

Instala una vez:
    pip install timezonefinder

Uso:
    python -m scripts.fill_timezone_offline                # dry-run
    python -m scripts.fill_timezone_offline --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

log = logging.getLogger("tz.offline")


def _connect():
    load_dotenv(dotenv_path=_PROJECT_ROOT / ".env", encoding="utf-8")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost").strip(),
        port=int(os.getenv("DB_PORT", "5432").strip()),
        dbname=os.getenv("DB_NAME", "football_db").strip(),
        user=os.getenv("DB_USER", "postgres").strip(),
        password=os.getenv("DB_PASSWORD", "").strip(),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    try:
        from timezonefinder import TimezoneFinder
    except ImportError:
        log.error("Falta `timezonefinder`. Instala con:  pip install timezonefinder")
        return 1
    tf = TimezoneFinder()

    n_ok = n_fail = 0
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stadium_id, latitude, longitude
                FROM dim_stadium
                WHERE timezone IS NULL
                  AND latitude IS NOT NULL AND longitude IS NOT NULL
            """)
            rows = cur.fetchall()
        log.info("candidatos: %d", len(rows))

        for sid, lat, lon in rows:
            tz = tf.timezone_at(lat=float(lat), lng=float(lon))
            if not tz:
                n_fail += 1
                continue
            n_ok += 1
            if args.apply:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE dim_stadium SET timezone=%s, updated_at=NOW() WHERE stadium_id=%s",
                        (tz, sid),
                    )
        if args.apply:
            conn.commit()

    log.info("DONE  resolved=%d failed=%d  (%s)",
             n_ok, n_fail, "APPLIED" if args.apply else "DRY-RUN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
