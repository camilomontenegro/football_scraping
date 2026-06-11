"""Importa bundle raw Bundesliga (zip compañeros) al layout data/raw/bundesliga/."""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_BACKUP_RAW = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup/data/raw")
COMP_SLUG = "bundesliga"
PREFIX = f"{COMP_SLUG}/{COMP_SLUG}/"


def _dest_for_zip_member(name: str) -> Path | None:
    """bundesliga/bundesliga/<season>/<source>/... → bundesliga/<season>/<source>/..."""
    if not name.startswith(PREFIX) or name.endswith("/"):
        return None
    rel = name[len(PREFIX) :]
    return Path(COMP_SLUG) / rel


def import_zip(
    zip_path: Path,
    dest_root: Path,
    *,
    dry_run: bool = False,
) -> int:
    dest_root = dest_root.resolve()
    written = 0
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            rel_dest = _dest_for_zip_member(info.filename)
            if rel_dest is None:
                continue
            out = dest_root / rel_dest
            if dry_run:
                written += 1
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(info) as src, out.open("wb") as dst:
                shutil.copyfileobj(src, dst)
            written += 1
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Importa zip raw Bundesliga al backup/proyecto")
    parser.add_argument(
        "--zip",
        type=Path,
        default=Path(r"C:/Users/ivanm/Desktop/bundesliga-20260605T092845Z-3-001.zip"),
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=DEFAULT_BACKUP_RAW,
        help="Raíz data/raw (backup por defecto)",
    )
    parser.add_argument("--also-project", action="store_true", help="Copia también al data/raw del repo")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.zip.exists():
        print(f"[!] Zip no encontrado: {args.zip}")
        return 1

    n = import_zip(args.zip, args.dest, dry_run=args.dry_run)
    print(f"{'[DRY-RUN] ' if args.dry_run else ''}Extraidos {n} archivos -> {args.dest}")

    if args.also_project and not args.dry_run:
        proj_raw = PROJECT_ROOT / "data" / "raw"
        src = args.dest / COMP_SLUG
        dst = proj_raw / COMP_SLUG
        if src.is_dir():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
            print(f"Copiado a proyecto: {dst}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
