"""
Build a master reference table with competition/season IDs and source URLs.

Output:
    data/reference/source_reference_ids.csv

The script refreshes volatile IDs from public endpoints when possible:
    - SofaScore seasons API
    - StatsBomb Open Data competitions.json

Static IDs/codes come from scripts.competitions and WHOSCORED_STAGES.
"""

from __future__ import annotations

import csv
import atexit
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import requests

from wizard.competitions import (
    COMPETITIONS,
    WORKING_COMPETITION_NAMES,
    get_competition_slug_transfermarkt,
    get_season_start_year,
)
from scrapers.whoscored_scraper import WHOSCORED_STAGES, build_season_urls


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = PROJECT_ROOT / "data" / "reference" / "source_reference_ids.csv"
STATSBOMB_COMPETITIONS_URL = (
    "https://raw.githubusercontent.com/statsbomb/open-data/master/data/competitions.json"
)
_SS_DRIVER = None

from scrapers.sofascore_seasons import SOFASCORE_SEASON_IDS


def _get_json(url: str) -> dict | list:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/136 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception:
        pass
    req = Request(url, headers=headers)
    with urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _season_labels(start: int = 2020, end: int = 2025) -> list[str]:
    return [f"{year}/{year + 1}" for year in range(start, end + 1)]


def _short_season(season: str) -> str:
    if "/" not in season:
        return season
    a, b = season.split("/", 1)
    return f"{a[-2:]}/{b[-2:]}"


def _statsbomb_index() -> dict[tuple[int, str], int]:
    try:
        rows = _get_json(STATSBOMB_COMPETITIONS_URL)
    except Exception:
        return {}
    index: dict[tuple[int, str], int] = {}
    for row in rows:
        comp_id = row.get("competition_id")
        season_name = row.get("season_name")
        season_id = row.get("season_id")
        if comp_id is not None and season_name and season_id is not None:
            index[(int(comp_id), str(season_name))] = int(season_id)
    return index


def _sofascore_seasons(tournament_id: int) -> dict[str, dict]:
    url = f"https://api.sofascore.com/api/v1/unique-tournament/{tournament_id}/seasons"
    try:
        data = _get_json(url)
    except Exception:
        data = None
    seasons = {}
    for item in (data or {}).get("seasons", []):
        year = str(item.get("year") or "")
        name = str(item.get("name") or "")
        if year:
            seasons[year] = item
        if "/" in year and len(year.split("/", 1)[0]) == 2:
            a, b = year.split("/", 1)
            seasons[f"20{a}/20{b}"] = item
        if name:
            seasons[name] = item
    for season, season_id in SOFASCORE_SEASON_IDS.get(tournament_id, {}).items():
        short = _short_season(season)
        item = {"id": season_id, "name": short, "year": short}
        seasons.setdefault(season, item)
        seasons.setdefault(short, item)
    return seasons


def _get_sofascore_json_with_browser(url: str) -> dict:
    global _SS_DRIVER
    if _SS_DRIVER is None:
        from scrapers.sofascore_scraper import create_driver
        _SS_DRIVER = create_driver(headless=True)
        atexit.register(lambda: _SS_DRIVER.quit() if _SS_DRIVER else None)
    from scrapers.sofascore_scraper import get_json
    return get_json(_SS_DRIVER, url, timeout=5)


def build_rows() -> list[dict]:
    statsbomb = _statsbomb_index()
    rows: list[dict] = []
    for competition, cfg in COMPETITIONS.items():
        if competition not in WORKING_COMPETITION_NAMES:
            continue
        sources = cfg.get("sources", {})
        from wizard.pipeline_runner import get_current_season
        seasons = _season_labels(2020, get_season_start_year(get_current_season()))
        if sources.get("whoscored", {}).get("season_format") == "single":
            seasons = sorted(
                s for c, s in WHOSCORED_STAGES
                if c == competition and s.isdigit() and int(s) >= 2020
            ) or seasons

        ss_cfg = sources.get("sofascore") or {}
        ss_tournament_id = ss_cfg.get("tournament_id")
        ss_seasons = _sofascore_seasons(int(ss_tournament_id)) if ss_tournament_id is not None else {}

        sb_cfg = sources.get("statsbomb") or {}
        sb_competition_id = sb_cfg.get("competition_id")

        for season in seasons:
            short = _short_season(season)

            tm_cfg = sources.get("transfermarkt") or {}
            tm_code = tm_cfg.get("league_code")
            tm_slug = get_competition_slug_transfermarkt(competition)
            if tm_code and tm_slug:
                tm_year = season.split("/", 1)[0]
                rows.append({
                    "competition": competition,
                    "season": season,
                    "source": "transfermarkt",
                    "competition_id": tm_code,
                    "season_id": tm_year,
                    "stage_ids": "",
                    "url": f"https://www.transfermarkt.es/{tm_slug}/teilnehmer/pokalwettbewerb/{tm_code}/saison_id/{tm_year}",
                })

            if ss_tournament_id is not None:
                item = ss_seasons.get(season) or ss_seasons.get(short)
                rows.append({
                    "competition": competition,
                    "season": season,
                    "source": "sofascore",
                    "competition_id": ss_tournament_id,
                    "season_id": item.get("id") if item else "",
                    "stage_ids": "",
                    "url": f"https://api.sofascore.com/api/v1/unique-tournament/{ss_tournament_id}/seasons",
                })

            us_cfg = sources.get("understat") or {}
            us_league = us_cfg.get("league")
            if us_league:
                year = season.split("/", 1)[0]
                rows.append({
                    "competition": competition,
                    "season": season,
                    "source": "understat",
                    "competition_id": us_league,
                    "season_id": year,
                    "stage_ids": "",
                    "url": f"https://understat.com/getLeagueData/{quote(us_league)}/{year}",
                })

            if sb_competition_id is not None:
                sb_season_id = statsbomb.get((int(sb_competition_id), season))
                rows.append({
                    "competition": competition,
                    "season": season,
                    "source": "statsbomb",
                    "competition_id": sb_competition_id,
                    "season_id": sb_season_id or "",
                    "stage_ids": "",
                    "url": STATSBOMB_COMPETITIONS_URL,
                })

            ws_cfg = sources.get("whoscored") or {}
            ws_tournament_id = ws_cfg.get("tournament_id")
            ws_seasons = [season]
            if "/" in season:
                a, b = season.split("/", 1)
                ws_seasons += [f"{a}/{b[-2:]}", _short_season(season)]
            ws_key = next(
                ((competition, s) for s in ws_seasons if (competition, s) in WHOSCORED_STAGES),
                None,
            )
            if ws_tournament_id is not None and ws_key in WHOSCORED_STAGES:
                ws = WHOSCORED_STAGES[ws_key]
                urls = build_season_urls(competition, ws_key[1])
                rows.append({
                    "competition": competition,
                    "season": season,
                    "source": "whoscored",
                    "competition_id": ws_tournament_id,
                    "season_id": ws.get("season_id", ""),
                    "stage_ids": "|".join(str(s) for s in ws.get("stages", [])),
                    "url": "|".join(urls),
                })
    return rows


def main() -> None:
    rows = build_rows()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fields = ["competition", "season", "source", "competition_id", "season_id", "stage_ids", "url"]
    with OUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Tabla maestra creada: {OUT_PATH} ({len(rows)} filas)")


if __name__ == "__main__":
    main()
