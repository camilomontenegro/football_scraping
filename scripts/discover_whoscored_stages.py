"""Discover WhoScored season_id + stage_ids (domestic main league / all cup stages)."""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests as cr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from wizard.competitions import COMPETITIONS, WORKING_COMPETITION_NAMES

SESSION = cr.Session(impersonate="chrome124")
WIZARD_RANGE = ["2020/21", "2021/22", "2022/23", "2023/24", "2024/25", "2025/26"]
WIZARD_SINGLE = ["2020", "2022", "2024", "2026"]

CONTINENTAL = {
    "Champions League", "Europa League", "Europa Conference League",
    "FIFA World Cup", "European Championship", "Copa America",
}

OVERRIDES: dict[str, dict] = {
    "Serie A": {"region_id": 108, "tournament_id": 5, "slug": "italy-serie-a"},
    "Ligue 1": {"region_id": 74, "tournament_id": 22, "slug": "france-ligue-1"},
    "Eredivisie": {"region_id": 155, "tournament_id": 13, "slug": "netherlands-eredivisie"},
    "Primeira Liga": {"region_id": 177, "tournament_id": 21, "slug": "portugal-liga"},
    "Europa League": {"slug": "europa-europa-league"},
    "Europa Conference League": {
        "region_id": 250, "tournament_id": 715, "slug": "europe-conference-league",
    },
}

STAGE_EXCLUDE = (
    "playoff", "relegation", "promotion", "qualification", "super cup",
    "knvb", "taça", "taca", "comeback", "ecl", "cup",
)


def ws_cfg(name: str) -> dict:
    base = dict((COMPETITIONS[name].get("sources", {}).get("whoscored") or {}))
    base.update(OVERRIDES.get(name, {}))
    return base


def fetch(url: str) -> str | None:
    try:
        r = SESSION.get(url, timeout=30)
        return r.text if r.status_code == 200 else None
    except Exception:
        return None


def label_to_key(label: str, season_format: str) -> str | None:
    label = label.strip()
    if season_format == "single":
        m = re.search(r"(20\d{2})", label)
        return m.group(1) if m else None
    m = re.match(r"(\d{4})/(\d{4})", label)
    if m:
        return f"{m.group(1)}/{m.group(2)[-2:]}"
    m = re.match(r"(\d{4})/(\d{2})", label)
    return f"{m.group(1)}/{m.group(2)}" if m else None


def parse_seasons(html: str) -> list[tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, str]] = []
    for opt in soup.select("select option"):
        value = (opt.get("value") or "").strip()
        label = opt.get_text(strip=True)
        m = re.search(r"/Seasons/(\d+)/", value, re.I)
        if m and label and "/Seasons/" in value and "/Stages/" not in value:
            out.append((label, m.group(1)))
    seen: set[str] = set()
    deduped: list[tuple[str, str]] = []
    for label, sid in out:
        if sid not in seen:
            seen.add(sid)
            deduped.append((label, sid))
    return deduped


def parse_labeled_stages(html: str, season_id: str) -> list[tuple[str, int]]:
    pat = re.compile(rf"/Seasons/{season_id}/Stages/(\d+)/", re.I)
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, int]] = []
    for opt in soup.select("select option"):
        value = (opt.get("value") or "").strip()
        label = opt.get_text(strip=True)
        m = pat.search(value)
        if m and label:
            out.append((label, int(m.group(1))))
    if out:
        return out
    for m in pat.finditer(html):
        out.append(("", int(m.group(1))))
    # dedupe by stage id
    seen: set[int] = set()
    deduped: list[tuple[str, int]] = []
    for lab, sid in out:
        if sid not in seen:
            seen.add(sid)
            deduped.append((lab, sid))
    return deduped


def pick_stages(competition: str, labeled: list[tuple[str, int]]) -> list[int]:
    if not labeled:
        return []
    if competition in CONTINENTAL:
        stages = [
            sid for lab, sid in labeled
            if "qualification" not in lab.lower()
        ]
        return sorted(set(stages)) if stages else sorted({sid for _, sid in labeled})

    name_l = competition.lower()
    aliases = {name_l, name_l.replace(" ", ""), "laliga" if name_l == "la liga" else name_l}
    for lab, sid in labeled:
        lab_l = lab.lower()
        if lab_l in aliases or lab_l.replace(" ", "") in aliases:
            return [sid]
    for lab, sid in labeled:
        lab_l = lab.lower()
        if lab_l.startswith(name_l) and not any(x in lab_l for x in STAGE_EXCLUDE):
            return [sid]

    good = [
        sid for lab, sid in labeled
        if lab and not any(x in lab.lower() for x in STAGE_EXCLUDE)
    ]
    if good:
        return [good[0]]
    return [labeled[0][1]]


def discover(name: str) -> dict[str, dict]:
    cfg = ws_cfg(name)
    region_id = cfg.get("region_id")
    tournament_id = cfg.get("tournament_id")
    slug = cfg.get("slug")
    season_format = cfg.get("season_format", "range")
    if not all([region_id, tournament_id, slug]):
        return {}

    html = fetch(f"https://www.whoscored.com/regions/{region_id}/tournaments/{tournament_id}")
    if not html:
        return {}

    seasons = parse_seasons(html)
    targets = set(WIZARD_SINGLE if season_format == "single" else WIZARD_RANGE)
    result: dict[str, dict] = {}

    for label, season_id in seasons:
        key = label_to_key(label, season_format)
        if not key or key not in targets:
            continue
        url = (
            f"https://www.whoscored.com/Regions/{region_id}/Tournaments/"
            f"{tournament_id}/Seasons/{season_id}/{slug}"
        )
        time.sleep(0.35)
        season_html = fetch(url)
        if not season_html:
            continue
        labeled = parse_labeled_stages(season_html, season_id)
        stages = pick_stages(name, labeled)
        result[key] = {"season_id": int(season_id), "stages": stages}
        print(f"  OK {name} {key}: season_id={season_id} stages={stages}")

    return result


def main() -> None:
    all_data: dict[str, dict[str, dict]] = {}
    for name in sorted(WORKING_COMPETITION_NAMES):
        ws = ws_cfg(name)
        if not ws.get("tournament_id"):
            continue
        print(f"\n=== {name} ===")
        data = discover(name)
        if data:
            all_data[name] = data

    out = Path("data/logs/whoscored_stages_discovered.json")
    out.write_text(json.dumps(all_data, indent=2), encoding="utf-8")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
