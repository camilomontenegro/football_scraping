"""Inventario scrape completo vs falta por competición/fuente/temporada."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._audit_clean_coverage import (
    EXPECTED,
    EXPECTED_MATCHES,
    count_csv_rows,
    sources_for_comp,
)
from wizard.competitions import WORKING_COMPETITION_NAMES
from utils.data_paths import slugify_competition, CLEAN_ROOT, RAW_ROOT

SEASON = "2025_2026"


def _status_clean(comp: str, src: str) -> str:
    slug = slugify_competition(comp)
    d = CLEAN_ROOT / slug / SEASON / src
    if not d.is_dir():
        return "FALTA"
    if src not in EXPECTED:
        return "EXTRA"
    missing = [f for f in EXPECTED[src] if not (d / f).exists()]
    if missing:
        return f"INCOMPLETO ({', '.join(missing)})"
    sizes = {f: count_csv_rows(d / f) for f in EXPECTED[src]}
    if src == "sofascore" and "matches.csv" in sizes:
        ref = EXPECTED_MATCHES.get(comp)
        m = sizes.get("matches.csv", 0)
        if m == 0:
            return "VACIO"
        if ref and m < ref * 0.5:
            return f"PARCIAL ({m}/{ref} partidos)"
        if ref and m < ref * 0.85:
            return f"OK~ ({m}/{ref})"
    if src == "understat" and sizes.get("matches.csv", 0) == 0:
        return "VACIO"
    if src == "whoscored" and sizes.get("events.csv", 0) == 0:
        return "VACIO"
    key = "matches.csv" if "matches.csv" in sizes else list(sizes)[0]
    return f"OK ({sizes.get(key, 0)} filas)"


def _raw_ws(slug: str) -> str:
    p = RAW_ROOT / slug / SEASON / "whoscored" / "matches"
    if not p.is_dir():
        return "-"
    n = sum(1 for x in p.iterdir() if x.is_dir())
    return str(n) if n else "0"


def _tm_attendance(slug: str) -> str:
    f = CLEAN_ROOT / slug / SEASON / "transfermarkt" / "attendance.csv"
    if not f.exists():
        return "FALTA"
    n = count_csv_rows(f)
    return f"OK ({n})" if n else "VACIO"


def main() -> None:
    print(f"Inventario temporada {SEASON}\n")
    header = (
        f"{'Competición':<28} | {'SS':<14} | {'TM':<14} | {'US':<14} | "
        f"{'WS':<14} | {'SB':<6} | {'TM att':<10} | {'RAW WS':<8}"
    )
    print(header)
    print("-" * len(header))

    for comp in sorted(WORKING_COMPETITION_NAMES):
        slug = slugify_competition(comp)
        cfg = sources_for_comp(comp)
        cols = {
            "sofascore": _status_clean(comp, "sofascore") if "sofascore" in cfg else "n/a",
            "transfermarkt": _status_clean(comp, "transfermarkt") if "transfermarkt" in cfg else "n/a",
            "understat": _status_clean(comp, "understat") if "understat" in cfg else "n/a",
            "whoscored": _status_clean(comp, "whoscored") if "whoscored" in cfg else "n/a",
            "statsbomb": _status_clean(comp, "statsbomb") if "statsbomb" in cfg else "n/a",
        }
        print(
            f"{comp:<28} | {cols['sofascore']:<14} | {cols['transfermarkt']:<14} | "
            f"{cols['understat']:<14} | {cols['whoscored']:<14} | {cols['statsbomb']:<6} | "
            f"{_tm_attendance(slug):<10} | {_raw_ws(slug):<8}"
        )

    print("\nLeyenda: OK = CSV completos; PARCIAL = pocos partidos; FALTA = sin carpeta/CSV")
    print("TM att = attendance.csv (scrape aparte, no wizard TM normal)")


if __name__ == "__main__":
    main()
