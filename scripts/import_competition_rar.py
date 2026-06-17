"""
scripts/import_competition_rar.py
=================================
Importa un archivo .rar con datos raw (layout data/raw/<slug>/<season>/...)
al proyecto y opcionalmente genera clean + carga en PostgreSQL.

Uso:
    python scripts/import_competition_rar.py --rar ~/Desktop/primeira_liga.rar
    python scripts/import_competition_rar.py --rar ~/Desktop/primeira_liga.rar --process
    python scripts/import_competition_rar.py --extracted data/.import_staging/primeira_liga/primeira_liga --slug primeira_liga
"""
from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.data_paths import RAW_ROOT, slugify_competition
from wizard.competitions import COMPETITIONS

log = logging.getLogger(__name__)

UNRAR_CANDIDATES = [
    PROJECT_ROOT / "tools" / "UnRAR.exe",
    Path(r"C:\Program Files\WinRAR\UnRAR.exe"),
    Path(r"C:\Program Files (x86)\WinRAR\UnRAR.exe"),
]


def _competition_from_slug(slug: str) -> str | None:
    for name in COMPETITIONS:
        if slugify_competition(name) == slug:
            return name
    return None


def _find_unrar() -> Path | None:
    for path in UNRAR_CANDIDATES:
        if path.is_file():
            return path
    return None


def extract_rar(rar_path: Path, dest: Path) -> Path:
    """Extrae el .rar y devuelve la carpeta raíz con temporadas."""
    dest.mkdir(parents=True, exist_ok=True)
    unrar = _find_unrar()
    if unrar is None:
        raise RuntimeError(
            "No se encontró UnRAR.exe. Colócalo en tools/UnRAR.exe "
            "(p. ej. desde https://www.rarlab.com/rar_add.htm) o instala WinRAR."
        )
    log.info("Extrayendo %s → %s", rar_path, dest)
    subprocess.run(
        [str(unrar), "x", "-y", str(rar_path), str(dest) + "\\"],
        check=True,
    )
    return _detect_root(dest)


def _detect_root(extracted: Path) -> Path:
    """Acepta raíz con o sin carpeta intermedia (p. ej. primeira_liga/2020_2021/)."""
    if not extracted.is_dir():
        raise FileNotFoundError(extracted)
    children = [p for p in extracted.iterdir() if p.is_dir()]
    if len(children) == 1 and not any(c.name[0].isdigit() for c in children):
        inner = children[0]
        if any(d.is_dir() and "_" in d.name for d in inner.iterdir()):
            return inner
    return extracted


def copy_raw_seasons(source_root: Path, slug: str, *, dry_run: bool = False) -> list[str]:
    """Copia temporadas de source_root a data/raw/<slug>/."""
    seasons: list[str] = []
    for season_dir in sorted(source_root.iterdir()):
        if not season_dir.is_dir():
            continue
        if not (season_dir / "whoscored").is_dir() and not (season_dir / "sofascore").is_dir():
            continue
        dst = RAW_ROOT / slug / season_dir.name
        seasons.append(season_dir.name)
        if dry_run:
            log.info("[dry-run] %s → %s", season_dir, dst)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(season_dir, dst)
        n = sum(1 for _ in (dst / "whoscored").rglob("match_centre.json")) if (dst / "whoscored").is_dir() else 0
        log.info("OK raw/%s/%s (%d match_centre)", slug, season_dir.name, n)
    return seasons


def process_season(competition: str, season_folder: str) -> None:
    """raw → clean (WhoScored) + enrichment + carga BD."""
    from scripts.rebuild_clean_from_raw import rebuild_whoscored
    from scrapers.whoscored_stats_extractor import run as extract_ws_stats
    from loaders.whoscored_stats_loader import load_all as load_ws_stats
    from wizard.pipeline_runner import run_load

    season_ws = season_folder.replace("_", "/")
    log.info("=== Procesando %s %s ===", competition, season_folder)

    if not rebuild_whoscored(competition, season_folder):
        log.warning("rebuild_whoscored falló para %s %s", competition, season_folder)

    extract_ws_stats(competition=slugify_competition(competition), season=season_folder)
    load_ws_stats(competition=slugify_competition(competition), season=season_folder)
    run_load(competition=competition, season=season_ws)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Importa competición desde .rar")
    parser.add_argument("--rar", type=Path, help="Ruta al archivo .rar")
    parser.add_argument("--extracted", type=Path, help="Carpeta ya extraída (sin --rar)")
    parser.add_argument("--slug", type=str, help="Slug carpeta (p. ej. primeira_liga). Auto-detecta si hay una sola.")
    parser.add_argument("--process", action="store_true", help="Generar clean y cargar en BD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.rar and not args.extracted:
        parser.error("Indica --rar o --extracted")

    staging = PROJECT_ROOT / "data" / ".import_staging" / "competition_rar"
    try:
        if args.extracted:
            root = _detect_root(args.extracted.resolve())
        else:
            if staging.exists():
                shutil.rmtree(staging)
            staging.mkdir(parents=True)
            root = extract_rar(args.rar.resolve(), staging)

        slug = args.slug or root.name
        competition = _competition_from_slug(slug)
        if not competition:
            parser.error(f"Slug desconocido: {slug}. Pásalo en wizard/competitions.py primero.")

        log.info("Competición: %s (slug=%s)", competition, slug)
        seasons = copy_raw_seasons(root, slug, dry_run=args.dry_run)
        if not seasons:
            log.error("No se encontraron temporadas con whoscored/sofascore en %s", root)
            return 1
        log.info("Temporadas importadas: %s", ", ".join(seasons))

        if args.process and not args.dry_run:
            for season in seasons:
                process_season(competition, season)

        return 0
    finally:
        if args.rar and staging.exists() and not args.extracted:
            shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
