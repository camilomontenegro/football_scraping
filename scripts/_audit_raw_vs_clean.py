"""Compara raw vs clean: partidos por fuente."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SEASON = "2025_2026"
LEAGUES = [
    "la_liga", "premier_league", "bundesliga", "serie_a", "ligue_1",
    "eredivisie", "primeira_liga", "champions_league", "europa_league",
]


def count_match_dirs(slug: str, src: str) -> int:
    p = PROJECT_ROOT / "data" / "raw" / slug / SEASON / src / "matches"
    if not p.exists():
        return 0
    return sum(1 for x in p.iterdir() if x.is_dir())


def unique_in_csv(path: Path, col_candidates: list[str]) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    with path.open(encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        cols = r.fieldnames or []
        col = next((c for c in col_candidates if c in cols), cols[0] if cols else None)
        vals: set[str] = set()
        n = 0
        for row in r:
            n += 1
            if col:
                vals.add(row[col])
        return len(vals), n


def main() -> None:
    hdr = (
        f"{'liga':20} {'raw_ss':>7} {'cl_m':>6} {'sh_u':>6} {'sh_n':>7} "
        f"{'raw_ws':>7} {'ws_m':>6} {'ws_ev_kb':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    for slug in LEAGUES:
        raw_ss = count_match_dirs(slug, "sofascore")
        raw_ws = count_match_dirs(slug, "whoscored")
        base = PROJECT_ROOT / "data" / "clean" / slug / SEASON
        um, _ = unique_in_csv(base / "sofascore/matches.csv", ["id_sofascore"])
        us, ns = unique_in_csv(base / "sofascore/shots.csv", ["match_id_ss", "id_sofascore"])
        wmm, _ = unique_in_csv(base / "whoscored/matches.csv", ["match_id", "game_id", "id_whoscored"])
        we = base / "whoscored/events.csv"
        we_kb = we.stat().st_size // 1024 if we.exists() else 0
        print(
            f"{slug:20} {raw_ss:7} {um:6} {us:6} {ns:7} "
            f"{raw_ws:7} {wmm:6} {we_kb:8}"
        )


if __name__ == "__main__":
    main()
