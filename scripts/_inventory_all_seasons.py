"""Inventario scrape por competición/fuente en todas las temporadas en data/clean/."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._audit_clean_coverage import EXPECTED, EXPECTED_MATCHES, count_csv_rows, sources_for_comp
from wizard.competitions import WORKING_COMPETITION_NAMES
from utils.data_paths import slugify_competition, CLEAN_ROOT

SRCS = ["sofascore", "transfermarkt", "understat", "whoscored", "statsbomb"]


def short_status(comp: str, season: str, src: str) -> str:
    cfg = sources_for_comp(comp)
    if src not in cfg:
        return "n/a"
    slug = slugify_competition(comp)
    d = CLEAN_ROOT / slug / season / src
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
    if src == "statsbomb":
        m = sizes.get("matches.csv", 0)
        return "VACIO" if m == 0 else f"OK({m})"
    return "OK"


def discover_seasons() -> list[str]:
    seasons: set[str] = set()
    for comp in WORKING_COMPETITION_NAMES:
        slug = slugify_competition(comp)
        p = CLEAN_ROOT / slug
        if p.is_dir():
            for s in p.iterdir():
                if s.is_dir():
                    seasons.add(s.name)
    return sorted(seasons)


def classify(st: str) -> str:
    if st == "n/a":
        return "na"
    if st.startswith("OK") or st.startswith("~OK"):
        return "ok"
    return "bad"


def main() -> None:
    seasons = discover_seasons()
    print("TEMPORADAS en data/clean:", ", ".join(seasons))
    print()

    for season in seasons:
        print("=" * 95)
        print(f"TEMPORADA {season}")
        print("=" * 95)
        hdr = (
            f"{'Competición':<22} | {'SS':<14} | {'TM':<14} | {'US':<14} | "
            f"{'WS':<16} | {'SB':<8} | TM att"
        )
        print(hdr)
        print("-" * len(hdr))
        for comp in sorted(WORKING_COMPETITION_NAMES):
            slug = slugify_competition(comp)
            base = CLEAN_ROOT / slug / season
            if not base.is_dir():
                print(f"{comp:<22} | — sin carpeta temporada —")
                continue
            att = base / "transfermarkt" / "attendance.csv"
            att_s = "OK" if att.exists() and count_csv_rows(att) else "FALTA"
            cols = [short_status(comp, season, s) for s in SRCS]
            print(
                f"{comp:<22} | {cols[0]:<14} | {cols[1]:<14} | {cols[2]:<14} | "
                f"{cols[3]:<16} | {cols[4]:<8} | {att_s}"
            )

    print()
    print("=" * 95)
    print("RESUMEN por temporada (fuentes configuradas: OK vs problema)")
    print("=" * 95)
    for season in seasons:
        ok = prob = na = 0
        details: list[str] = []
        for comp in WORKING_COMPETITION_NAMES:
            slug = slugify_competition(comp)
            if not (CLEAN_ROOT / slug / season).is_dir():
                continue
            for src in SRCS:
                st = short_status(comp, season, src)
                c = classify(st)
                if c == "na":
                    na += 1
                elif c == "ok":
                    ok += 1
                else:
                    prob += 1
                    details.append(f"{comp}/{src}:{st}")
        print(f"{season}: OK={ok}  problemas={prob}  n/a={na}")
        for d in details:
            print(f"  - {d}")

    print()
    print(
        "Leyenda: OK(n)=completo; ~OK/ PAR=partidos SS bajos; INC=falta CSV o events vacío; "
        "FALTA=sin carpeta; n/a=fuente no disponible (Understat PT/NL, StatsBomb de pago)"
    )


if __name__ == "__main__":
    main()
