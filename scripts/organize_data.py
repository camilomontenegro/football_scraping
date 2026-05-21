"""
organize_data.py
=================
Reorganiza data/raw/ a la convencion canonica:

    data/raw/<source>/<comp_slug>/season=YYYY_YYYY/<archivo>

Uso:
    python -m scripts.organize_data            # dry-run
    python -m scripts.organize_data --apply    # mueve los archivos
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW = PROJECT_ROOT / "data" / "raw"

SOURCES = {"sofascore", "understat", "whoscored", "statsbomb", "transfermarkt"}

COMP_SLUGS = {
    "la liga": "la-liga", "laliga": "la-liga",
    "la_liga": "la-liga", "la-liga": "la-liga", "espana-laliga": "la-liga",
    "bundesliga": "bundesliga", "alemania-bundesliga": "bundesliga",
    "premier league": "premier-league", "premier-league": "premier-league",
    "premierleague": "premier-league", "inglaterra-premier-league": "premier-league",
    "serie a": "serie-a", "serie-a": "serie-a", "italia-serie-a": "serie-a",
    "ligue 1": "ligue-1", "ligue-1": "ligue-1", "francia-ligue-1": "ligue-1",
    "primeira liga": "primeira-liga", "primeira-liga": "primeira-liga",
    "eredivisie": "eredivisie", "championship": "championship",
    "segunda division": "segunda-division", "segunda-division": "segunda-division",
    "fifa world cup": "fifa-world-cup", "fifa-world-cup": "fifa-world-cup",
    "internacional-fifa-world-cup": "fifa-world-cup",
    "champions league": "champions-league", "champions-league": "champions-league",
    "europa league": "europa-league", "europa-league": "europa-league",
}


def _norm_comp_slug(raw: str) -> str:
    if not raw:
        return "unknown"
    k = raw.strip().lower().replace("_", " ").replace("-", " ")
    return COMP_SLUGS.get(k, k.replace(" ", "-"))


def _norm_season(raw: str) -> str | None:
    if not raw:
        return None
    s = raw.replace("/", "_").replace(" ", "_")
    m = re.search(r"(\d{2,4})_(\d{2,4})", s)
    if m:
        a, b = m.group(1), m.group(2)
        if len(a) == 2: a = "20" + a
        if len(b) == 2: b = "20" + b
        return f"{a}_{b}"
    m = re.search(r"\b(\d{4})\b", s)
    if m:
        a = int(m.group(1))
        return f"{a}_{a + 1}"
    return None


def _classify(path: Path):
    try:
        rel = path.relative_to(RAW)
    except ValueError:
        return None
    parts = list(rel.parts)
    if not parts:
        return None

    # Caso 1: data/raw/<source>/...
    if parts[0] in SOURCES:
        source = parts[0]
        if path.parent == RAW / source:
            stem = path.stem
            for kind in ("matches", "shots", "events", "teams", "players", "injuries"):
                prefix = f"{source}_{kind}_"
                if stem.startswith(prefix):
                    raw_slug = stem[len(prefix):]
                    return source, _norm_comp_slug(raw_slug), "unknown"
            return None
        if len(parts) >= 3 and parts[2].startswith("season="):
            return source, _norm_comp_slug(parts[1]), _norm_season(parts[2]) or "unknown"
        if parts[1].startswith("season="):
            return source, "unknown", _norm_season(parts[1]) or "unknown"
        if len(parts) >= 2:
            return source, _norm_comp_slug(parts[1]), "unknown"

    # Caso 2: data/raw/<comp>/<source>/season=.../...
    if len(parts) >= 2 and parts[1] in SOURCES:
        comp_slug = _norm_comp_slug(parts[0])
        source    = parts[1]
        season = None
        i = 2
        while i < len(parts):
            if parts[i].startswith("season="):
                token = parts[i]
                j = i + 1
                while j < len(parts) and not (
                    parts[j].startswith("match_") or parts[j].startswith("batch_id=")
                    or parts[j].endswith(".csv") or parts[j].endswith(".json")
                ):
                    token += "_" + parts[j]
                    j += 1
                season = _norm_season(token)
                break
            i += 1
        return source, comp_slug, season or "unknown"

    return None


def _plan():
    moves, ignored = [], []
    for path in RAW.rglob("*"):
        if not path.is_file():
            continue
        cls = _classify(path)
        if not cls:
            ignored.append(path)
            continue
        source, comp_slug, season = cls
        season_dir = f"season={season}" if season != "unknown" else "season=unknown"
        target_dir = RAW / source / comp_slug / season_dir

        inner_parts = [p for p in path.relative_to(RAW).parts
                       if p.startswith("match_") or p.startswith("batch_id=")]
        inner_parts.append(path.name)
        target = target_dir.joinpath(*inner_parts)
        if path.resolve() == target.resolve():
            continue
        moves.append((path, target))
    return moves, ignored


def _apply(moves, keep_empty_dirs=False):
    moved = conflicts = 0
    for src, dst in moves:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            i = 1
            while True:
                cand = dst.with_name(f"{dst.stem}__dup{i}{dst.suffix}")
                if not cand.exists():
                    dst = cand; break
                i += 1
            conflicts += 1
        shutil.move(str(src), str(dst))
        moved += 1
    if not keep_empty_dirs:
        for d in sorted((p for p in RAW.rglob("*") if p.is_dir()),
                       key=lambda p: -len(p.parts)):
            try:
                d.rmdir()
            except OSError:
                pass
    return moved, conflicts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--apply", action="store_true")
    p.add_argument("--keep-empty-dirs", action="store_true")
    args = p.parse_args()

    print("=" * 70)
    print("  ORGANIZADOR DE data/raw/")
    print("=" * 70)
    print("  Convencion: data/raw/<source>/<comp_slug>/season=YYYY_YYYY/")
    print()

    moves, ignored = _plan()
    print(f"[plan] {len(moves)} archivos a mover")
    print(f"[plan] {len(ignored)} archivos ignorados")
    print()

    buckets = defaultdict(int)
    for src, dst in moves:
        try:
            key = dst.parent.relative_to(RAW)
        except ValueError:
            key = dst.parent
        buckets[str(key)] += 1
    print("[plan] Distribucion por destino:")
    for k in sorted(buckets):
        print(f"  {buckets[k]:>6}  data/raw/{k}/")

    if ignored:
        print()
        print("[plan] Ignorados (primeros 10):")
        for p_ in ignored[:10]:
            print(f"  - {p_.relative_to(PROJECT_ROOT)}")

    if not args.apply:
        print()
        print("Modo DRY-RUN. Lanza con --apply para mover de verdad.")
        return 0

    moved, conflicts = _apply(moves, keep_empty_dirs=args.keep_empty_dirs)
    print()
    print(f"[OK] {moved} archivos movidos | {conflicts} colisiones renombradas")
    return 0


if __name__ == "__main__":
    sys.exit(main())
