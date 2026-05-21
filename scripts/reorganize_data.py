#!/usr/bin/env python3
"""
scripts/reorganize_data.py
==========================
Migra `data/raw/` de la estructura antigua a la nueva convención del repo:

    ANTES:  data/raw/<source>/<comp_slug>/season=<label>/...
            data/raw/<comp_slug>/<source>/season=<label>/...   (legacy)

    AHORA:  data/raw/<comp_slug>/<season>/<source>/<files>           ← crudos
            data/clean/<comp_slug>/<season>/<source>/<files>.csv     ← DB-ready

Reglas:
    1. `comp_slug` se normaliza a snake_case sin tildes vía slugify_competition().
    2. `season` se normaliza a 'YYYY_YYYY' vía normalize_season().
    3. Las carpetas `batch_id=...` se aplastan: los JSON se mueven al nivel
       del scraper y el batch queda como campo dentro del JSON (si ya lo trae)
       o se loguea en un `_batch_index.json` de cada (comp, season, source).
    4. Los CSV "DB-ready" (transfermarkt_stadiums.csv, players_clean.csv, etc.)
       se mueven a `data/clean/...`.
    5. Cachés globales (`*_last_scraped.json`) se mueven a `data/.cache/`.

Uso:
    # Plan de movimientos (no toca nada)
    python -m scripts.reorganize_data --dry-run

    # Migración real con backup automático en data/_old/
    python -m scripts.reorganize_data --apply

    # Migración sin backup (más rápida, irreversible)
    python -m scripts.reorganize_data --apply --no-backup

    # Sólo un source concreto
    python -m scripts.reorganize_data --apply --source transfermarkt
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

# Permite ejecutar como script suelto
sys.path.append(str(Path(__file__).resolve().parent.parent))

from utils.data_paths import (
    PROJECT_ROOT, DATA_ROOT, RAW_ROOT, CLEAN_ROOT,
    slugify_competition, normalize_season,
)

log = logging.getLogger("reorganize_data")

# Carpeta donde se hace backup de la estructura previa
BACKUP_DIR = DATA_ROOT / "_old"
# Cachés globales (caché de scrapers)
CACHE_DIR  = DATA_ROOT / ".cache"

# Sufijos que se consideran "limpios" (CSV DB-ready)
_CLEAN_SUFFIXES = (".csv",)
# Sufijos que se consideran "crudos"
_RAW_SUFFIXES = (".json", ".jsonl", ".html", ".parquet")

# Lista cerrada de fuentes válidas (para distinguir source-first de comp-first)
KNOWN_SOURCES = {
    "transfermarkt", "sofascore", "understat", "statsbomb", "whoscored",
}

# Nombres de archivos que son caché global (no datos de competición)
_CACHE_FILENAMES = {
    "last_scraped.json",
    "stadiums_last_scraped.json",
}


# ── Utilidades ───────────────────────────────────────────────────────────────

class Action:
    """Una acción atómica del plan de migración."""
    __slots__ = ("kind", "src", "dst", "note")

    def __init__(self, kind: str, src: Path, dst: Path, note: str = ""):
        self.kind = kind       # 'move' | 'mkdir' | 'cache' | 'skip'
        self.src = src
        self.dst = dst
        self.note = note

    def __repr__(self) -> str:
        rel_src = _rel(self.src)
        rel_dst = _rel(self.dst) if self.dst else "-"
        return f"[{self.kind:5}] {rel_src}  →  {rel_dst}" + (
            f"   ({self.note})" if self.note else ""
        )


def _rel(p: Optional[Path]) -> str:
    if p is None:
        return "-"
    try:
        return str(p.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(p)


def _split_season_folder(name: str) -> Optional[str]:
    """De 'season=2024_2025' / 'season=LaLiga 25' devuelve la temporada normalizada."""
    if not name.lower().startswith("season"):
        return normalize_season(name)
    return normalize_season(name)


def _is_clean_file(path: Path) -> bool:
    return path.suffix.lower() in _CLEAN_SUFFIXES


def _is_raw_file(path: Path) -> bool:
    return path.suffix.lower() in _RAW_SUFFIXES


# ── Detectores de layout ─────────────────────────────────────────────────────

def _detect_layout_for(root: Path) -> str:
    """
    Decide si `root` (carpeta hija directa de data/raw) representa una fuente
    o una competición.

    Retorna:
        'source'  si el nombre está en KNOWN_SOURCES
        'comp'    en otro caso
    """
    return "source" if root.name.lower() in KNOWN_SOURCES else "comp"


# ── Planificador ─────────────────────────────────────────────────────────────

def plan_migration(
    raw_root: Path = RAW_ROOT,
    only_source: Optional[str] = None,
) -> list[Action]:
    """
    Genera el plan de movimientos. NO toca el sistema de archivos.

    Args:
        raw_root:    carpeta a migrar (por defecto data/raw)
        only_source: si se da, filtra a esa fuente.

    Returns:
        Lista de `Action`. Se puede ejecutar luego con apply_plan().
    """
    actions: list[Action] = []
    if not raw_root.exists():
        log.warning("No existe %s — nada que migrar.", raw_root)
        return actions

    for first_level in sorted(raw_root.iterdir()):
        if not first_level.is_dir():
            # Archivos sueltos en data/raw/ → caché global o ignorar
            if first_level.name in _CACHE_FILENAMES:
                actions.append(Action(
                    "cache", first_level, CACHE_DIR / first_level.name,
                    "caché global",
                ))
            continue

        layout = _detect_layout_for(first_level)

        if layout == "source":
            source = first_level.name.lower()
            if only_source and source != only_source:
                continue
            actions.extend(_plan_source_first(first_level, source))
        else:
            # Layout legacy: <comp>/<source>/season=.../
            comp_raw = first_level.name
            actions.extend(_plan_comp_first(first_level, comp_raw, only_source))

    return actions


def _plan_source_first(source_dir: Path, source: str) -> list[Action]:
    """
    Plan para layout `data/raw/<source>/<comp>/season=<label>/...`.
    """
    actions: list[Action] = []

    # Archivos sueltos en data/raw/<source>/ → caché
    for f in source_dir.iterdir():
        if f.is_file():
            if f.name in _CACHE_FILENAMES or "last_scraped" in f.name:
                actions.append(Action(
                    "cache", f, CACHE_DIR / f"{source}_{f.name}",
                    "caché global",
                ))
            else:
                actions.append(Action("skip", f, None, "archivo suelto, revisar"))

    for comp_dir in sorted(source_dir.iterdir()):
        if not comp_dir.is_dir():
            continue
        comp_slug = slugify_competition(comp_dir.name)
        if not comp_slug:
            actions.append(Action("skip", comp_dir, None, "comp_slug vacío"))
            continue

        for season_dir in sorted(comp_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            season = _split_season_folder(season_dir.name)
            if not season:
                actions.append(Action(
                    "skip", season_dir, None,
                    f"season no reconocida: {season_dir.name}",
                ))
                continue
            actions.extend(_plan_season_contents(
                season_dir, comp_slug, season, source,
            ))

    return actions


def _plan_comp_first(comp_root: Path, comp_raw: str,
                     only_source: Optional[str]) -> list[Action]:
    """
    Plan para layout legacy `data/raw/<comp>/<source>/season=<label>/...`.
    """
    actions: list[Action] = []
    comp_slug = slugify_competition(comp_raw)
    if not comp_slug:
        actions.append(Action("skip", comp_root, None, "comp_slug vacío"))
        return actions

    for source_dir in sorted(comp_root.iterdir()):
        if not source_dir.is_dir():
            actions.append(Action("skip", source_dir, None, "no es carpeta"))
            continue
        source = source_dir.name.lower()
        if source not in KNOWN_SOURCES:
            actions.append(Action("skip", source_dir, None,
                                  f"source desconocida: {source}"))
            continue
        if only_source and source != only_source:
            continue

        for season_dir in sorted(source_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            season = _split_season_folder(season_dir.name)
            if not season:
                actions.append(Action(
                    "skip", season_dir, None,
                    f"season no reconocida: {season_dir.name}",
                ))
                continue
            actions.extend(_plan_season_contents(
                season_dir, comp_slug, season, source,
            ))
    return actions


def _plan_season_contents(season_dir: Path, comp_slug: str,
                          season: str, source: str) -> list[Action]:
    """
    Plan para el contenido dentro de una carpeta season=<label>.

    Aplasta carpetas batch_id=*, separa raw vs clean.

    Caso legacy: la temporada se guardó como 'season=LaLiga 25' con un subdir
    '26/' (porque el código antiguo escribía 'LaLiga 25/26' como ruta). Si
    detectamos ese patrón, también lo aplastamos.
    """
    actions: list[Action] = []
    raw_target   = RAW_ROOT   / comp_slug / season / source
    clean_target = CLEAN_ROOT / comp_slug / season / source

    # Detectar subdir-fragmento de temporada (2 dígitos como nivel huérfano)
    legacy_year_subdir = None
    children = [c for c in season_dir.iterdir() if c.is_dir()]
    if len(children) == 1 and re.fullmatch(r"\d{2}", children[0].name):
        legacy_year_subdir = children[0].name  # ej. '26'

    for item in sorted(season_dir.rglob("*")):
        if not item.is_file():
            continue
        rel_parts = []
        for part in item.relative_to(season_dir).parts:
            if part.startswith("batch_id="):
                continue  # aplastamos batches
            if legacy_year_subdir and part == legacy_year_subdir:
                continue  # aplastamos el fragmento '26' del split por '/'
            rel_parts.append(part)
        rel = Path(*rel_parts)

        if _is_clean_file(item):
            dst = clean_target / rel
            note = "CSV → clean/"
        elif _is_raw_file(item):
            dst = raw_target / rel
            note = "JSON → raw/"
        else:
            actions.append(Action("skip", item, None, "extensión desconocida"))
            continue

        actions.append(Action("move", item, dst, note))

    return actions


# ── Ejecución ────────────────────────────────────────────────────────────────

def apply_plan(actions: list[Action], backup: bool = True) -> dict:
    """
    Aplica el plan al sistema de archivos.

    Returns: dict con contadores: moved, cached, skipped, errors.
    """
    counters = defaultdict(int)

    if backup and RAW_ROOT.exists():
        log.info("Creando backup completo en %s …", _rel(BACKUP_DIR))
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        # Copia (no mueve) para poder revertir manualmente
        for child in RAW_ROOT.iterdir():
            target = BACKUP_DIR / child.name
            if target.exists():
                continue
            try:
                if child.is_dir():
                    shutil.copytree(child, target)
                else:
                    shutil.copy2(child, target)
            except Exception as e:
                log.error("Backup fallido para %s: %s", child, e)

    for act in actions:
        try:
            if act.kind == "move":
                act.dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(act.src), str(act.dst))
                counters["moved"] += 1
            elif act.kind == "cache":
                act.dst.parent.mkdir(parents=True, exist_ok=True)
                if act.src.is_file():
                    shutil.move(str(act.src), str(act.dst))
                    counters["cached"] += 1
            elif act.kind == "skip":
                counters["skipped"] += 1
            elif act.kind == "mkdir":
                act.dst.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            log.error("Fallo aplicando %s: %s", act, e)
            counters["errors"] += 1

    # Limpieza: borrar carpetas viejas vacías bajo data/raw
    _prune_empty_dirs(RAW_ROOT)
    return dict(counters)


def _prune_empty_dirs(root: Path) -> None:
    """Elimina recursivamente carpetas vacías bajo `root`."""
    if not root.exists():
        return
    # rglob de abajo a arriba
    for d in sorted(root.rglob("*"), key=lambda p: -len(p.parts)):
        if d.is_dir():
            try:
                d.rmdir()  # falla si tiene contenido
            except OSError:
                pass


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Reorganiza data/raw a la nueva convención del proyecto."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Muestra el plan sin modificar nada (default).")
    parser.add_argument("--apply", action="store_true",
                        help="Ejecuta el plan sobre el sistema de archivos.")
    parser.add_argument("--no-backup", action="store_true",
                        help="Salta el backup automático en data/_old/.")
    parser.add_argument("--source",
                        help="Migra sólo una fuente (transfermarkt, sofascore, ...).")
    parser.add_argument("--root", type=Path,
                        help="Raíz alternativa (por defecto data/raw).")
    args = parser.parse_args()

    raw_root = args.root or RAW_ROOT
    print(f"\n[INFO] Carpeta raíz: {_rel(raw_root)}")
    print(f"[INFO] Filtro fuente: {args.source or '(todas)'}")
    print()

    actions = plan_migration(raw_root=raw_root, only_source=args.source)

    # Resumen por tipo
    by_kind: dict[str, int] = defaultdict(int)
    for a in actions:
        by_kind[a.kind] += 1
    print("─" * 70)
    print(f"  PLAN — {len(actions)} acciones:")
    for k, n in sorted(by_kind.items()):
        print(f"     {k:6} : {n}")
    print("─" * 70)

    # Detallar (limitado para no inundar)
    SHOW = 40
    for a in actions[:SHOW]:
        print(" ", a)
    if len(actions) > SHOW:
        print(f"  … y {len(actions) - SHOW} más (usa --apply o filtra con --source)")
    print()

    if args.apply:
        print("[APPLY] Ejecutando plan…")
        res = apply_plan(actions, backup=not args.no_backup)
        print(f"[DONE] {res}")
    else:
        print("[DRY-RUN] Nada se ha modificado. "
              "Re-ejecuta con --apply para aplicar.")


if __name__ == "__main__":
    main()
