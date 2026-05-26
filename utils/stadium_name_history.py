"""utils/stadium_name_history.py - parser de eras de nombre de estadio."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

_FAR_FUTURE = 9999


@dataclass
class NameEra:
    name: str
    ranges: list = field(default_factory=list)

    def covers_year(self, season_start_year):
        return any(s <= season_start_year < e for s, e in self.ranges)


_ENTRY_RE = re.compile(r"^(.+?)\s*\(([^)]+)\)\s*$")
_BIS_RE = re.compile(r"^bis\s+(\d{4})$", re.IGNORECASE)
_FROM_ACT_RE = re.compile(r"^(\d{4})\s*-\s*act\.?$", re.IGNORECASE)
_YEAR_YEAR_RE = re.compile(r"^(\d{4})\s*-\s*(\d{4})$")
_FULL_DATES_RE = re.compile(r"^(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})$")
_RANGE_SPLIT_RE = re.compile(r"\s*(?:,|;| und | y )\s*")


def parse_name_eras(raw):
    if not raw:
        return []
    text = str(raw).strip()
    if not text:
        return []
    chunks = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r"\)\s*,\s*", line)
        for i, p in enumerate(parts):
            p = p.strip(" ,")
            if not p:
                continue
            if i < len(parts) - 1 and not p.endswith(")"):
                p = p + ")"
            chunks.append(p)
    eras = []
    for chunk in chunks:
        m = _ENTRY_RE.match(chunk)
        if not m:
            eras.append(NameEra(name=chunk.strip()))
            continue
        eras.append(NameEra(name=m.group(1).strip(), ranges=_parse_inside_parens(m.group(2).strip())))
    return eras


def _parse_inside_parens(inside):
    ranges = []
    for tok in _RANGE_SPLIT_RE.split(inside):
        tok = tok.strip()
        if not tok:
            continue
        m = _BIS_RE.match(tok)
        if m:
            ranges.append((0, int(m.group(1))))
            continue
        m = _FROM_ACT_RE.match(tok)
        if m:
            ranges.append((int(m.group(1)), _FAR_FUTURE))
            continue
        m = _YEAR_YEAR_RE.match(tok)
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2))
            if y2 < y1:
                y1, y2 = y2, y1
            ranges.append((y1, y2))
            continue
        m = _FULL_DATES_RE.match(tok)
        if m:
            d1 = datetime.strptime(m.group(1), "%d/%m/%Y").date()
            d2 = datetime.strptime(m.group(2), "%d/%m/%Y").date()
            ranges.append((d1.year, d2.year))
            continue
    return ranges


def _season_start_year(season):
    if season is None:
        return None
    if isinstance(season, int):
        return season
    s = str(season).strip().replace("_", "/")
    if not s:
        return None
    head = s.split("/")[0]
    if not head.isdigit():
        return None
    y = int(head)
    if y < 100:
        y += 2000
    return y


def name_for_season(current_name, eras, season):
    year = _season_start_year(season)
    if year is None:
        return current_name
    for era in eras:
        if era.covers_year(year):
            return era.name
    return current_name


def resolve_name(current_name, previous_names_raw, season):
    return name_for_season(current_name, parse_name_eras(previous_names_raw), season)
