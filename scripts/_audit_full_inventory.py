"""Inventario clean + raw por competicion/temporada/fuente."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._audit_clean_coverage import EXPECTED, count_csv_rows, sources_for_comp
from wizard.competitions import WORKING_COMPETITION_NAMES, COMPETITIONS
from utils.data_paths import slugify_competition

SEASON = "2025_2026"


def _source_dirs(base: Path) -> dict[str, Path]:
    if not base.exists():
        return {}
    return {d.name: d for d in base.iterdir() if d.is_dir()}


def _raw_summary(src_dir: Path) -> str:
  if not src_dir.exists():
    return "0 files"
  files = [p for p in src_dir.rglob("*") if p.is_file()]
  json_n = sum(1 for p in files if p.suffix.lower() == ".json")
  csv_n = sum(1 for p in files if p.suffix.lower() == ".csv")
  match_dirs = len(list((src_dir / "matches").glob("*"))) if (src_dir / "matches").is_dir() else 0
  parts = [f"{len(files)} files"]
  if json_n:
    parts.append(f"{json_n} json")
  if csv_n:
    parts.append(f"{csv_n} csv")
  if match_dirs:
    parts.append(f"{match_dirs} match dirs")
  return ", ".join(parts)


def audit_layer(root: Path, layer: str) -> None:
    print("=" * 100)
    print(f"{layer}  |  {root.relative_to(PROJECT_ROOT)}  |  season {SEASON}")
    print("=" * 100)

    for comp in sorted(WORKING_COMPETITION_NAMES):
        slug = slugify_competition(comp)
        base = root / slug / SEASON
        configured = sources_for_comp(comp)
        present = _source_dirs(base)

        print(f"\n## {comp}")
        if not present:
            print("  (sin carpeta de temporada)")
            continue

        for src in sorted(set(configured) | set(present.keys())):
            in_cfg = src in configured
            d = present.get(src)
            tag = "" if in_cfg else " [extra/no config]"
            if d is None:
                if in_cfg:
                    print(f"  [{src}] FALTA carpeta{tag}")
                continue

            if layer == "CLEAN" and src in EXPECTED:
                missing = [f for f in EXPECTED[src] if not (d / f).exists()]
                if missing:
                    print(f"  [{src}] INCOMPLETO clean: faltan {missing}{tag}")
                else:
                    sizes = {f: count_csv_rows(d / f) for f in EXPECTED[src]}
                    print(
                        f"  [{src}] CLEAN OK: "
                        + ", ".join(f"{k}={v}" for k, v in sizes.items())
                        + tag
                    )
            elif layer == "RAW":
                print(f"  [{src}] RAW: {_raw_summary(d)}{tag}")
            else:
                print(f"  [{src}] presente ({len(list(d.rglob('*')))} nodes){tag}")


def other_competitions() -> None:
    print("\n" + "=" * 100)
    print("OTRAS COMPETICIONES EN competitions.py (no WORKING)")
    print("=" * 100)
    for name in sorted(COMPETITIONS):
        if name in WORKING_COMPETITION_NAMES:
            continue
        for layer, root in [("CLEAN", PROJECT_ROOT / "data" / "clean"), ("RAW", PROJECT_ROOT / "data" / "raw")]:
            slug = slugify_competition(name)
            base = root / slug / SEASON
            if base.exists():
                srcs = sorted(_source_dirs(base))
                print(f"  {name} [{layer}]: {', '.join(srcs) or 'empty'}")


def main() -> int:
    audit_layer(PROJECT_ROOT / "data" / "clean", "CLEAN")
    print()
    audit_layer(PROJECT_ROOT / "data" / "raw", "RAW")
    other_competitions()

    # All seasons under clean/raw for quick view
    print("\n" + "=" * 100)
    print("TEMPORADAS EXTRA (cualquier comp con mas de una season en clean)")
    print("=" * 100)
    clean = PROJECT_ROOT / "data" / "clean"
    for comp_dir in sorted(clean.iterdir()):
        if not comp_dir.is_dir():
            continue
        seasons = sorted(d.name for d in comp_dir.iterdir() if d.is_dir())
        if len(seasons) > 1 or (seasons and seasons != [SEASON]):
            srcs_by_season = {}
            for s in seasons:
                srcs = sorted(_source_dirs(comp_dir / s))
                srcs_by_season[s] = srcs
            print(f"  {comp_dir.name}: {srcs_by_season}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
