"""
scripts/run_fill_match_context.py
===================================
Scrapea WhoScored para partidos con huecos de contexto, carga a BD y
rellena asistencia desde SofaScore.

Pipeline (en serie, un scraper a la vez):
  1. scrape_whoscored_by_ids  (--from-db --only-gaps --skip-existing)
  2. whoscored_stats_extractor
  3. backfill_match_context
  4. backfill_attendance (SofaScore)

Uso:
    python -m scripts.run_fill_match_context
    python -m scripts.run_fill_match_context --skip-attendance
    python -m scripts.run_fill_match_context --competition "Champions League"
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from loaders.common import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PY = ROOT / ".venv" / "Scripts" / "python.exe"
if not PY.exists():
    PY = Path(sys.executable)


def _scrape_jobs(competition: str | None = None) -> list[tuple[str, str, int]]:
    """(competition, season_db, gap_count) ordenados por prioridad."""
    filt = ""
    params: dict = {}
    if competition:
        filt = "AND dc.canonical_name = :competition"
        params["competition"] = competition

    sql = f"""
        SELECT dc.canonical_name, m.season, COUNT(*) AS n
        FROM dim_match m
        JOIN dim_competition dc ON dc.canonical_id = m.competition_id
        WHERE m.id_whoscored IS NOT NULL
          AND (m.referee_id IS NULL OR m.manager_home IS NULL OR m.manager_away IS NULL
               OR m.attendance IS NULL OR m.attendance = 0)
          {filt}
        GROUP BY dc.canonical_name, m.season
        ORDER BY
            COUNT(*) FILTER (WHERE m.referee_id IS NULL) DESC,
            COUNT(*) FILTER (WHERE m.manager_home IS NULL OR m.manager_away IS NULL) DESC,
            n DESC
    """
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def _ensure_scraper_slot(max_wait: int = 3600) -> None:
    """Espera a que no haya otro scraper activo (p. ej. huérfano de un run anterior)."""
    import os
    import time

    from utils.scraper_lock import LOCK_PATH, _pid_alive, _read_lock

    waited = 0
    while LOCK_PATH.exists():
        pid, cmd = _read_lock()
        if pid is None or pid == os.getpid() or not _pid_alive(pid):
            try:
                LOCK_PATH.unlink()
            except OSError:
                pass
            break
        if waited == 0:
            log.warning("Scraper activo (PID %s: %s), esperando...", pid, cmd)
        time.sleep(10)
        waited += 10
        if waited >= max_wait:
            raise RuntimeError(f"Scraper PID {pid} sigue activo tras {max_wait}s")


def _run(cmd: list[str]) -> int:
    log.info(">> %s", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape + backfill contexto de partido")
    ap.add_argument("--competition", "-c", default=None)
    ap.add_argument("--skip-scrape", action="store_true")
    ap.add_argument("--skip-attendance", action="store_true")
    args = ap.parse_args()

    jobs = _scrape_jobs(args.competition)
    if not jobs:
        log.info("No hay trabajos de scrape pendientes.")
    else:
        log.info("Trabajos WhoScored: %d comp/temporada, %d partidos con huecos",
                 len(jobs), sum(n for _, _, n in jobs))

    _run([str(PY), "-m", "scripts.audit_match_context"])

    if not args.skip_scrape:
        for comp, season, n in jobs:
            log.info("=== SCRAPE %s %s (%d huecos) ===", comp, season, n)
            _ensure_scraper_slot()
            rc = _run([
                str(PY), "-m", "scripts.scrape_whoscored_by_ids",
                "-c", comp,
                "-s", season,
                "--from-db",
                "--only-gaps",
                "--skip-existing",
            ])
            if rc != 0:
                log.error("Scrape falló para %s %s (rc=%d), continuando...", comp, season, rc)

        log.info("=== EXTRACTOR WhoScored ===")
        rc = _run([str(PY), "-m", "scrapers.whoscored_stats_extractor"])
        if rc != 0:
            log.warning("Extractor rc=%d", rc)

        log.info("=== LOADER WhoScored stats ===")
        rc = _run([str(PY), "-m", "loaders.whoscored_stats_loader"])
        if rc != 0:
            log.warning("Loader rc=%d", rc)

    log.info("=== BACKFILL match context ===")
    rc = _run([str(PY), "-m", "scripts.backfill_match_context"])
    if rc != 0:
        log.error("Backfill falló, rc=%d", rc)
        sys.exit(rc)

    if not args.skip_attendance:
        log.info("=== BACKFILL attendance (SofaScore) ===")
        rc = _run([str(PY), "-m", "scrapers.backfill_attendance"])
        if rc != 0:
            log.warning("Attendance backfill rc=%d", rc)

        _run([str(PY), "-m", "scripts.backfill_match_context", "--skip-json", "--skip-extract"])

    log.info("=== AUDITORIA FINAL ===")
    _run([str(PY), "-m", "scripts.audit_match_context"])


if __name__ == "__main__":
    main()
