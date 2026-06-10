"""Generate scrapers/sofascore_seasons.py from api.var11.com mirror."""
import json
import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from wizard.competitions import COMPETITIONS, WORKING_COMPETITION_NAMES

BASE = "https://api.var11.com/api/v1/unique-tournament/{tid}/seasons"
MIN_SEASON_YEAR = 2018
MAX_SEASON_YEAR = 2026

SINGLE_YEAR_TIDS = {1, 16, 133}

by_tid: dict[int, dict[str, int]] = {}
comp_by_tid: dict[int, str] = {}

for comp, cfg in COMPETITIONS.items():
    if comp not in WORKING_COMPETITION_NAMES:
        continue
    tid = cfg.get("sources", {}).get("sofascore", {}).get("tournament_id")
    if tid is None:
        continue
    comp_by_tid[tid] = comp
    r = requests.get(BASE.format(tid=tid), timeout=30)
    r.raise_for_status()
    mapped: dict[str, int] = {}
    for item in r.json().get("seasons", []):
        y = str(item.get("year") or "")
        sid = item.get("id")
        if sid is None:
            continue
        if tid in SINGLE_YEAR_TIDS and y.isdigit() and len(y) == 4:
            year = int(y)
            if MIN_SEASON_YEAR <= year <= MAX_SEASON_YEAR + 2:
                mapped[y] = sid
            continue
        if "/" in y and len(y.split("/")[0]) == 2:
            a, b = y.split("/")
            full = f"20{a}/20{b}"
            start = int(full.split("/")[0])
            if MIN_SEASON_YEAR <= start <= MAX_SEASON_YEAR:
                mapped[full] = sid
        elif "/" in y and len(y.split("/")[0]) == 4:
            start = int(y.split("/")[0])
            if MIN_SEASON_YEAR <= start <= MAX_SEASON_YEAR:
                mapped[y] = sid
    by_tid[tid] = dict(sorted(mapped.items()))

lines = [
    '"""SofaScore tournament/season ID registry and helpers."""',
    "from __future__ import annotations",
    "",
    "import re",
    "from typing import Optional",
    "",
    "# tournament_id -> canonical season label -> SofaScore season_id",
    "# Domestic leagues use YYYY/YYYY. International cups use single years (2024, 2026).",
    "# Regenerate: python scripts/discover_sofascore_seasons.py --write",
    "SOFASCORE_SEASON_IDS: dict[int, dict[str, int]] = {",
]
for tid in sorted(by_tid):
    comp = comp_by_tid.get(tid, str(tid))
    lines.append(f"    # {comp}")
    lines.append(f"    {tid}: {{")
    for season, sid in by_tid[tid].items():
        lines.append(f'        "{season}": {sid},')
    lines.append("    },")
lines.append("}")
lines.append("")
lines.append("TOURNAMENT_ID_BY_COMPETITION: dict[str, int] = {")
for comp, cfg in sorted(COMPETITIONS.items()):
    if comp not in WORKING_COMPETITION_NAMES:
        continue
    tid = cfg.get("sources", {}).get("sofascore", {}).get("tournament_id")
    if tid is not None:
        lines.append(f'    "{comp}": {tid},')
lines.append("}")
lines.append("")
lines.append("""
def _canonical_range_season(season: str) -> Optional[str]:
    season = str(season or "").strip()
    if re.fullmatch(r"\\d{4}/\\d{4}", season):
        return season
    m = re.fullmatch(r"(\\d{4})/(\\d{2})", season)
    if m:
        return f"{m.group(1)}/20{m.group(2)}"
    m = re.fullmatch(r"(\\d{2})/(\\d{2})", season)
    if m:
        return f"20{m.group(1)}/20{m.group(2)}"
    m = re.search(r"(\\d{2}/\\d{2})", season)
    if m:
        a, b = m.group(1).split("/")
        return f"20{a}/20{b}"
    return None


def _canonical_single_year(season: str) -> Optional[str]:
    season = str(season or "").strip()
    if re.fullmatch(r"\\d{4}", season):
        return season
    m = re.fullmatch(r"(\\d{4})/(\\d{4})", season)
    if m and m.group(1) == m.group(2):
        return m.group(1)
    return None


def season_lookup_keys(season_name: str) -> set[str]:
    keys = {str(season_name or "").strip()}
    canonical = _canonical_range_season(season_name)
    if canonical:
        keys.add(canonical)
        start, end = canonical.split("/")
        keys.add(f"{start[-2:]}/{end[-2:]}")
        keys.add(f"{start}/{end[-2:]}")
    single = _canonical_single_year(season_name)
    if single:
        keys.add(single)
    return {k.lower() for k in keys if k}


def get_fallback_season_id(tournament_id: int, season_name: str) -> tuple[Optional[int], Optional[str]]:
    table = SOFASCORE_SEASON_IDS.get(int(tournament_id), {})
    if not table:
        return None, None
    for key in season_lookup_keys(season_name):
        for label, sid in table.items():
            if key == label.lower() or key in label.lower():
                return sid, label
    canonical = _canonical_range_season(season_name) or _canonical_single_year(season_name)
    if canonical and canonical in table:
        return table[canonical], canonical
    return None, None


def default_seasons_for_competition(competition: str, start_year: int = 2020, end_year: int = 2025) -> list[str]:
    tid = TOURNAMENT_ID_BY_COMPETITION.get(competition)
    if tid is None:
        return [f"{y}/{y + 1}" for y in range(start_year, end_year + 1)]
    table = SOFASCORE_SEASON_IDS.get(tid, {})
    if not table:
        return [f"{y}/{y + 1}" for y in range(start_year, end_year + 1)]
    if tid in {1, 16, 133}:
        return sorted(k for k in table if k.isdigit() and start_year <= int(k) <= end_year + 1)
    out = []
    for y in range(start_year, end_year + 1):
        label = f"{y}/{y + 1}"
        if label in table:
            out.append(label)
    return out or [f"{y}/{y + 1}" for y in range(start_year, end_year + 1)]


def sofascore_season_available(competition: str, season: str) -> bool:
    tid = TOURNAMENT_ID_BY_COMPETITION.get(competition)
    if tid is None:
        return False
    sid, _ = get_fallback_season_id(tid, season)
    return sid is not None
""")

out_path = ROOT / "scrapers" / "sofascore_seasons.py"
out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Wrote {out_path} ({len(by_tid)} tournaments)")
