"""Scrape match_centre.json for Serie A 24/25 + UCL 23/24-24/25, then backfill."""
from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = ROOT / ".venv" / "Scripts" / "python.exe"
if not PY.exists():
    PY = Path(sys.executable)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

JOBS = [
    ("Serie A", "2024/25"),
    ("Champions League", "2023/24"),
    ("Champions League", "2024/25"),
]


def run_scrape(competition: str, season: str) -> int:
    cmd = [
        str(PY), "-m", "scripts.scrape_whoscored_by_ids",
        "-c", competition,
        "-s", season,
        "--from-db",
        "--skip-existing",
    ]
    log.info(">> %s", " ".join(cmd))
    return subprocess.call(cmd, cwd=str(ROOT))


def main() -> None:
    for comp, season in JOBS:
        log.info("=== %s %s ===", comp, season)
        rc = run_scrape(comp, season)
        if rc != 0:
            log.error("Scrape falló (%s %s), rc=%d", comp, season, rc)
            sys.exit(rc)

    log.info("=== Backfill match context ===")
    rc = subprocess.call(
        [str(PY), "-m", "scripts.backfill_match_context"],
        cwd=str(ROOT),
    )
    if rc != 0:
        log.error("Backfill falló, rc=%d", rc)
        sys.exit(rc)

    subprocess.call([str(PY), "-m", "scripts.audit_match_context"], cwd=str(ROOT))


if __name__ == "__main__":
    main()
