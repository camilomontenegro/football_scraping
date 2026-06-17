"""
scripts/import_desktop_archives.py
===================================
Importa en un solo paso los dos archivos del escritorio:
  - understat_25_26.zip
  - 2025-2026-*.zip (Transfermarkt + SofaScore)

Uso:
  python scripts/import_desktop_archives.py
  python scripts/import_desktop_archives.py --understat-only
  python scripts/import_desktop_archives.py --dry-run
"""
from __future__ import annotations

import argparse
import shutil
import sys
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.import_understat_external import EXTERNAL_LEAGUES, import_league  # noqa: E402
from scripts.import_teammate_bundle import (  # noqa: E402
    import_sofascore,
    import_transfermarkt,
    _bundle_root,
)
import zipfile
import shutil as _shutil


def _extract_understat(zip_path: Path, dest_root: Path) -> Path:
    """Extrae understat/<liga>/... bajo dest_root/understat/."""
    dest = dest_root / "understat"
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for entry in zf.namelist():
            if not entry.endswith(".csv"):
                continue
            # understat/la_liga/understat_matches_la_liga.csv
            parts = Path(entry).parts
            if len(parts) < 3 or parts[0] != "understat":
                continue
            target = dest / Path(*parts[1:])
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(entry) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)
    return dest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--understat-zip",
        type=Path,
        default=Path.home() / "Desktop" / "understat_25_26.zip",
    )
    parser.add_argument(
        "--bundle-zip",
        type=Path,
        default=None,
        help="Ruta al zip 2025-2026 (por defecto: primer 2025-2026*.zip en Desktop)",
    )
    parser.add_argument("--understat-only", action="store_true")
    parser.add_argument("--bundle-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-merge", action="store_true")
    args = parser.parse_args()

    merge = not args.no_merge

    if not args.bundle_only:
        if not args.understat_zip.is_file():
            print(f"[!] No encontrado: {args.understat_zip}")
            sys.exit(1)
        print("=== Understat ===")
        staging = PROJECT_ROOT / "understat"
        if not args.dry_run:
            _extract_understat(args.understat_zip, PROJECT_ROOT)
        source = staging if staging.is_dir() else PROJECT_ROOT / "understat"
        results = [
            import_league(s, dry_run=args.dry_run, merge=merge, source_root=source)
            for s in EXTERNAL_LEAGUES
        ]
        print(f"Understat: {sum(results)}/{len(results)} ligas\n")

    if not args.understat_only:
        bundle = args.bundle_zip
        if bundle is None:
            desktop = Path.home() / "Desktop"
            candidates = sorted(desktop.glob("2025-2026*.zip"))
            bundle = candidates[0] if candidates else None
        if bundle is None or not bundle.is_file():
            print("[!] No se encontró zip 2025-2026 en Desktop")
            sys.exit(1)
        print("=== Bundle TM + SofaScore ===")
        staging = PROJECT_ROOT / "data" / ".import_staging" / "teammate_bundle"
        if staging.exists():
            _shutil.rmtree(staging)
        staging.mkdir(parents=True)
        print(f"Extrayendo {bundle.name} ...")
        with zipfile.ZipFile(bundle, "r") as zf:
            zf.extractall(staging)
        root = _bundle_root(staging)
        merge_flag = merge
        n_tm = import_transfermarkt(root, merge=merge_flag, dry_run=args.dry_run)
        n_ss = import_sofascore(root, merge=merge_flag, dry_run=args.dry_run)
        if not args.dry_run:
            _shutil.rmtree(staging, ignore_errors=True)
        print(f"Bundle: {n_tm} TM + {n_ss} SofaScore\n")


if __name__ == "__main__":
    main()
