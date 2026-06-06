"""SofaScore tournament/season ID registry and helpers."""
from __future__ import annotations

import re
from typing import Optional

# tournament_id -> canonical season label -> SofaScore season_id
# Domestic leagues use YYYY/YYYY. International cups use single years (2024, 2026).
# Regenerate: python scripts/discover_sofascore_seasons.py --write
SOFASCORE_SEASON_IDS: dict[int, dict[str, int]] = {
    # European Championship
    1: {
        "2021": 26542,
        "2024": 56953,
    },
    # Champions League
    7: {
        "2018/2019": 17351,
        "2019/2020": 23766,
        "2020/2021": 29267,
        "2021/2022": 36886,
        "2022/2023": 41897,
        "2023/2024": 52162,
        "2024/2025": 61644,
        "2025/2026": 76953,
    },
    # La Liga
    8: {
        "2018/2019": 18020,
        "2019/2020": 24127,
        "2020/2021": 32501,
        "2021/2022": 37223,
        "2022/2023": 42409,
        "2023/2024": 52376,
        "2024/2025": 61643,
        "2025/2026": 77559,
    },
    # FIFA World Cup
    16: {
        "2018": 15586,
        "2022": 41087,
        "2026": 58210,
    },
    # Premier League
    17: {
        "2018/2019": 17359,
        "2019/2020": 23776,
        "2020/2021": 29415,
        "2021/2022": 37036,
        "2022/2023": 41886,
        "2023/2024": 52186,
        "2024/2025": 61627,
        "2025/2026": 76986,
    },
    # Serie A
    23: {
        "2018/2019": 17932,
        "2019/2020": 24644,
        "2020/2021": 32523,
        "2021/2022": 37475,
        "2022/2023": 42415,
        "2023/2024": 52760,
        "2024/2025": 63515,
        "2025/2026": 76457,
    },
    # Ligue 1
    34: {
        "2018/2019": 17279,
        "2019/2020": 23872,
        "2020/2021": 28222,
        "2021/2022": 37167,
        "2022/2023": 42273,
        "2023/2024": 52571,
        "2024/2025": 61736,
        "2025/2026": 77356,
    },
    # Bundesliga
    35: {
        "2018/2019": 17597,
        "2019/2020": 23538,
        "2020/2021": 28210,
        "2021/2022": 37166,
        "2022/2023": 42268,
        "2023/2024": 52608,
        "2024/2025": 63516,
        "2025/2026": 77333,
    },
    # Eredivisie
    37: {
        "2018/2019": 17353,
        "2019/2020": 23873,
        "2020/2021": 29186,
        "2021/2022": 36890,
        "2022/2023": 42256,
        "2023/2024": 52554,
        "2024/2025": 61666,
        "2025/2026": 77012,
    },
    # Copa America
    133: {
        "2019": 22352,
        "2021": 26681,
        "2024": 57114,
    },
    # Primeira Liga
    238: {
        "2018/2019": 17714,
        "2019/2020": 24150,
        "2020/2021": 32456,
        "2021/2022": 37358,
        "2022/2023": 42655,
        "2023/2024": 52769,
        "2024/2025": 63670,
        "2025/2026": 77806,
    },
    # Europa League
    679: {
        "2018/2019": 17352,
        "2019/2020": 23755,
        "2020/2021": 29343,
        "2021/2022": 37725,
        "2022/2023": 44509,
        "2023/2024": 53654,
        "2024/2025": 61645,
        "2025/2026": 76984,
    },
    # Europa Conference League
    17015: {
        "2021/2022": 37074,
        "2022/2023": 42224,
        "2023/2024": 52327,
        "2024/2025": 61648,
        "2025/2026": 76960,
    },
}

TOURNAMENT_ID_BY_COMPETITION: dict[str, int] = {
    "Bundesliga": 35,
    "Champions League": 7,
    "Copa America": 133,
    "Eredivisie": 37,
    "Europa Conference League": 17015,
    "Europa League": 679,
    "European Championship": 1,
    "FIFA World Cup": 16,
    "La Liga": 8,
    "Ligue 1": 34,
    "Premier League": 17,
    "Primeira Liga": 238,
    "Serie A": 23,
}


def _canonical_range_season(season: str) -> Optional[str]:
    season = str(season or "").strip()
    if re.fullmatch(r"\d{4}/\d{4}", season):
        return season
    m = re.fullmatch(r"(\d{4})/(\d{2})", season)
    if m:
        return f"{m.group(1)}/20{m.group(2)}"
    m = re.fullmatch(r"(\d{2})/(\d{2})", season)
    if m:
        return f"20{m.group(1)}/20{m.group(2)}"
    m = re.search(r"(\d{2}/\d{2})", season)
    if m:
        a, b = m.group(1).split("/")
        return f"20{a}/20{b}"
    return None


def _canonical_single_year(season: str) -> Optional[str]:
    season = str(season or "").strip()
    if re.fullmatch(r"\d{4}", season):
        return season
    m = re.fullmatch(r"(\d{4})/(\d{4})", season)
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
    canonical = _canonical_range_season(season_name) or _canonical_single_year(season_name)
    if canonical and canonical in table:
        return table[canonical], canonical
    for key in season_lookup_keys(season_name):
        for label, sid in table.items():
            if key == label.lower():
                return sid, label
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

