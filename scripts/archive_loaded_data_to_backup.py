"""
Archiva en el backup del Escritorio los datos ya cargados en PostgreSQL
y los saca del proyecto (mueve, no copia).

Criterio de "cargado": temporadas de WORKING_COMPETITION_NAMES con
eventos en fact_events (vía dim_competition + dim_match).

Uso:
    python -m scripts.archive_loaded_data_to_backup --dry-run
    python -m scripts.archive_loaded_data_to_backup
    python -m scripts.archive_loaded_data_to_backup --backup "D:/mi_backup"
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loaders.common import engine
from utils.data_paths import CLEAN_ROOT, DATA_ROOT, RAW_ROOT, normalize_season, slugify_competition
from wizard.competitions import WORKING_COMPETITION_NAMES

log = logging.getLogger(__name__)

DEFAULT_BACKUP = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup")

EXTRA_PATHS = [
    RAW_ROOT / "players",
    RAW_ROOT / "market_value",
    RAW_ROOT / "transfers",
    CLEAN_ROOT / "market_value",
    CLEAN_ROOT / "transfers",
    DATA_ROOT / ".import_staging",
]

REPORTS_GLOBS = ("whoscored_*.csv", "fact_player_match_stats_muestra_*.csv")


def _loaded_comp_seasons() -> list[tuple[str, str, str]]:
    """Devuelve (canonical_name, slug, season_label) con eventos en BD."""
    names = set(WORKING_COMPETITION_NAMES)
    sql = text("""
        SELECT dc.canonical_name, m.season, COUNT(e.event_id) AS n
        FROM fact_events e
        JOIN dim_match m ON m.match_id = e.match_id
        JOIN dim_competition dc ON dc.canonical_id = m.competition_id
        WHERE dc.canonical_name = ANY(:names)
        GROUP BY dc.canonical_name, m.season
        HAVING COUNT(e.event_id) > 0
        ORDER BY dc.canonical_name, m.season
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"names": list(names)}).fetchall()

    out: list[tuple[str, str, str]] = []
    for comp, season, _ in rows:
        season_label = (normalize_season(season) or str(season)).replace("/", "_")
        out.append((comp, slugify_competition(comp), season_label))
    return out


def _merge_move(src: Path, dst: Path, dry_run: bool) -> bool:
    """Mueve src → dst fusionando si dst ya existe."""
    if not src.exists():
        return False
    if dry_run:
        log.info("[dry-run] mover %s → %s", src, dst)
        return True

    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists():
        shutil.move(str(src), str(dst))
        return True

    if src.is_file():
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
        return True

    for child in list(src.iterdir()):
        _merge_move(child, dst / child.name, dry_run=False)
    try:
        src.rmdir()
    except OSError:
        pass
    return True


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024
    return f"{n:.1f}TB"


def archive_loaded(
    backup_root: Path,
    dry_run: bool = False,
    git_untrack: bool = True,
) -> dict:
    backup_data = backup_root / "data"
    backup_reports = backup_root / "reports"
    manifest: dict = {
        "archived_at": datetime.now(timezone.utc).isoformat(),
        "project": str(PROJECT_ROOT),
        "backup": str(backup_root),
        "comp_seasons": [],
        "extra_paths": [],
        "reports": [],
        "git_untracked": [],
    }

    pairs = _loaded_comp_seasons()
    log.info("Temporadas cargadas en BD: %d", len(pairs))

    moved_bytes = 0
    for comp, slug, season in pairs:
        for kind, root in (("raw", RAW_ROOT), ("clean", CLEAN_ROOT)):
            src = root / slug / season
            if not src.exists():
                continue
            dst = backup_data / kind / slug / season
            size = _dir_size(src)
            if _merge_move(src, dst, dry_run):
                moved_bytes += size
                manifest["comp_seasons"].append({
                    "competition": comp,
                    "slug": slug,
                    "season": season,
                    "kind": kind,
                    "bytes": size,
                })
                log.info("  %s %s/%s (%s)", kind, slug, season, _fmt_size(size))

    for extra in EXTRA_PATHS:
        if not extra.exists():
            continue
        rel = extra.relative_to(DATA_ROOT)
        dst = backup_data / rel
        size = _dir_size(extra)
        if _merge_move(extra, dst, dry_run):
            moved_bytes += size
            manifest["extra_paths"].append({"path": str(rel), "bytes": size})
            log.info("  extra %s (%s)", rel, _fmt_size(size))

    reports_dir = PROJECT_ROOT / "reports"
    if reports_dir.is_dir():
        for pattern in REPORTS_GLOBS:
            for src in reports_dir.glob(pattern):
                dst = backup_reports / src.name
                size = src.stat().st_size if src.is_file() else 0
                if _merge_move(src, dst, dry_run):
                    moved_bytes += size
                    manifest["reports"].append(src.name)
                    log.info("  report %s", src.name)

    manifest["total_bytes"] = moved_bytes
    manifest_path = backup_root / f"manifest_archived_{datetime.now():%Y%m%d_%H%M%S}.json"
    if not dry_run:
        backup_root.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Manifiesto: %s", manifest_path)

    if git_untrack and not dry_run:
        tracked = subprocess.run(
            ["git", "ls-files", "data/", "reports/"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.splitlines()

        to_remove = []
        for rel in tracked:
            p = Path(rel)
            if not p.exists():
                to_remove.append(rel)
            elif p.is_relative_to(DATA_ROOT / "raw") or p.is_relative_to(DATA_ROOT / "clean"):
                # archivo suelto aún en data/ raw/clean
                if p.name not in ("README.md", "stadium_overrides.json"):
                    to_remove.append(rel)

        for pattern in REPORTS_GLOBS:
            for rel in tracked:
                if Path(rel).match(pattern):
                    to_remove.append(rel)

        to_remove = sorted(set(to_remove))
        if to_remove:
            # git rm --cached en lotes
            batch = 200
            for i in range(0, len(to_remove), batch):
                chunk = to_remove[i : i + batch]
                subprocess.run(
                    ["git", "rm", "-r", "--cached", "--ignore-unmatch", *chunk],
                    cwd=PROJECT_ROOT,
                    check=False,
                )
            manifest["git_untracked"] = to_remove
            log.info("Quitados del índice git: %d archivos", len(to_remove))

    log.info("Total archivado: %s", _fmt_size(moved_bytes))
    return manifest


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Archiva datos cargados en BD al backup del Escritorio")
    parser.add_argument("--backup", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-git", action="store_true", help="No ejecutar git rm --cached")
    args = parser.parse_args()

    archive_loaded(args.backup, dry_run=args.dry_run, git_untrack=not args.no_git)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
