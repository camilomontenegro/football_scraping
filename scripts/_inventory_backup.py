"""Inventario de data/clean y data/raw en carpeta backup externa."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._audit_clean_coverage import EXPECTED, EXPECTED_MATCHES, count_csv_rows, sources_for_comp
from wizard.competitions import WORKING_COMPETITION_NAMES, COMPETITIONS
from utils.data_paths import slugify_competition

DEFAULT_BACKUP = Path(r"C:/Users/ivanm/Desktop/football_scraping_backup")
SRCS = ["sofascore", "transfermarkt", "understat", "whoscored", "statsbomb"]


def short_status(clean_root: Path, comp: str, season: str, src: str) -> str:
    cfg = sources_for_comp(comp)
    if src not in cfg:
        return "n/a"
    d = clean_root / slugify_competition(comp) / season / src
    if not d.is_dir():
        return "FALTA"
    missing = [f for f in EXPECTED[src] if not (d / f).exists()]
    if missing:
        short = ",".join(m.replace(".csv", "") for m in missing[:3])
        return f"INC({short})"
    sizes = {f: count_csv_rows(d / f) for f in EXPECTED[src]}
    if src == "sofascore":
        m = sizes.get("matches.csv", 0)
        if m == 0:
            return "VACIO"
        ref = EXPECTED_MATCHES.get(comp)
        if ref and m < ref * 0.5:
            return f"PAR({m})"
        if ref and m < ref * 0.85:
            return f"~OK({m})"
        return f"OK({m})"
    if src == "understat":
        m = sizes.get("matches.csv", 0)
        return "VACIO" if m == 0 else f"OK({m})"
    if src == "whoscored":
        ev = sizes.get("events.csv", 0)
        m = sizes.get("matches.csv", 0)
        if ev == 0:
            return f"INC(ev0,m{m})" if m else "VACIO"
        return f"OK({m})"
    if src == "transfermarkt":
        p = sizes.get("players.csv", 0)
        return "VACIO" if p == 0 else f"OK({p})"
    return "OK"


def dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024
    return f"{n:.1f}TB"


def discover_seasons(clean_root: Path) -> list[str]:
    seasons: set[str] = set()
    if not clean_root.is_dir():
        return []
    for comp in WORKING_COMPETITION_NAMES:
        p = clean_root / slugify_competition(comp)
        if p.is_dir():
            for s in p.iterdir():
                if s.is_dir():
                    seasons.add(s.name)
    for comp_dir in clean_root.iterdir():
        if comp_dir.is_dir():
            for s in comp_dir.iterdir():
                if s.is_dir():
                    seasons.add(s.name)
    return sorted(seasons)


def raw_match_dirs(raw_root: Path, slug: str, season: str, src: str) -> int:
    p = raw_root / slug / season / src / "matches"
    if not p.is_dir():
        return 0
    return sum(1 for x in p.iterdir() if x.is_dir())


def extra_comps(clean_root: Path) -> list[str]:
    working_slugs = {slugify_competition(c) for c in WORKING_COMPETITION_NAMES}
    return sorted(
        d.name for d in clean_root.iterdir() if d.is_dir() and d.name not in working_slugs
    )


def main() -> None:
    backup = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_BACKUP
    data = backup / "data"
    clean_root = data / "clean"
    raw_root = data / "raw"

    print(f"BACKUP: {backup}")
    print(f"  clean: {fmt_size(dir_size(clean_root))}  |  raw: {fmt_size(dir_size(raw_root))}")
    if (data / "exports").is_dir():
        print(f"  exports: {fmt_size(dir_size(data / 'exports'))}")
    print()

    seasons = discover_seasons(clean_root)
    print("TEMPORADAS en clean:", ", ".join(seasons) if seasons else "(ninguna)")
    extras = extra_comps(clean_root)
    if extras:
        print("Competiciones extra en clean (fuera WORKING):", ", ".join(extras))
    print()

    for season in seasons:
        print("=" * 95)
        print(f"CLEAN — TEMPORADA {season}")
        print("=" * 95)
        hdr = (
            f"{'Competición':<22} | {'SS':<14} | {'TM':<14} | {'US':<14} | "
            f"{'WS':<16} | {'SB':<8} | TM att"
        )
        print(hdr)
        print("-" * len(hdr))
        for comp in sorted(WORKING_COMPETITION_NAMES):
            base = clean_root / slugify_competition(comp) / season
            if not base.is_dir():
                print(f"{comp:<22} | — sin carpeta —")
                continue
            att = base / "transfermarkt" / "attendance.csv"
            att_s = "OK" if att.exists() and count_csv_rows(att) else "FALTA"
            cols = [short_status(clean_root, comp, season, s) for s in SRCS]
            print(
                f"{comp:<22} | {cols[0]:<14} | {cols[1]:<14} | {cols[2]:<14} | "
                f"{cols[3]:<16} | {cols[4]:<8} | {att_s}"
            )

    print()
    print("=" * 95)
    print("RAW — partidos scrapeados (carpetas en .../matches/)")
    print("=" * 95)
    if not raw_root.is_dir():
        print("  (sin data/raw)")
    else:
        for comp_dir in sorted(raw_root.iterdir()):
            if not comp_dir.is_dir():
                continue
            for season_dir in sorted(comp_dir.iterdir()):
                if not season_dir.is_dir():
                    continue
                parts: list[str] = []
                for src in SRCS:
                    n = raw_match_dirs(raw_root, comp_dir.name, season_dir.name, src)
                    if n:
                        parts.append(f"{src}:{n}")
                if parts:
                    print(f"  {comp_dir.name}/{season_dir.name}: {', '.join(parts)}")

    print()
    print("=" * 95)
    print("TAMAÑO clean por competición (todas las temporadas)")
    print("=" * 95)
    if clean_root.is_dir():
        for comp_dir in sorted(clean_root.iterdir()):
            if not comp_dir.is_dir():
                continue
            sz = dir_size(comp_dir)
            season_list = sorted(d.name for d in comp_dir.iterdir() if d.is_dir())
            print(f"  {comp_dir.name:<22} {fmt_size(sz):>10}  temporadas: {', '.join(season_list)}")

    print()
    print("Leyenda: OK(n)=completo; PAR/~OK=SS bajo; INC=CSV faltante; FALTA=sin carpeta; n/a=no aplica")


if __name__ == "__main__":
    main()
