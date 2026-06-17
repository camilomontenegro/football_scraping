"""Restaura raw/clean del backup al proyecto para comp/season concretos."""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.data_paths import slugify_competition

DEFAULT_BACKUP = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup/data")


def restore_pairs(
    pairs: list[tuple[str, str]],
    *,
    backup_root: Path,
    kinds: tuple[str, ...] = ("raw", "clean"),
    dry_run: bool = False,
) -> int:
    n = 0
    for comp_name, season in pairs:
        slug = slugify_competition(comp_name)
        for kind in kinds:
            src = backup_root / kind / slug / season
            if not src.is_dir():
                print(f"  [skip] no backup: {kind}/{slug}/{season}")
                continue
            dst = PROJECT_ROOT / "data" / kind / slug / season
            if dry_run:
                print(f"  [dry-run] {src} -> {dst}")
                n += 1
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            files = sum(1 for f in dst.rglob("*") if f.is_file())
            print(f"  OK {kind}/{slug}/{season} ({files} archivos)")
            n += 1
    return n


def main() -> int:
    parser = argparse.ArgumentParser(description="Restaura datos del backup al proyecto")
    parser.add_argument("--backup", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--raw-only", action="store_true")
    args = parser.parse_args()

    # Temporadas con huecos de tiros o necesarias para rebuild
    pairs = [
        ("La Liga", "2021_2022"),
        ("Premier League", "2022_2023"),
        ("La Liga", "2025_2026"),
        ("Premier League", "2025_2026"),
        ("Bundesliga", "2025_2026"),
        ("Ligue 1", "2025_2026"),
        ("Serie A", "2025_2026"),
    ]
    kinds = ("raw",) if args.raw_only else ("raw", "clean")
    print(f"Restaurando {len(pairs)} comp/temporada desde {args.backup} ...")
    n = restore_pairs(pairs, backup_root=args.backup, kinds=kinds, dry_run=args.dry_run)
    print(f"Hecho: {n} carpetas restauradas")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
