"""
scrapers/besoccer_injuries_scraper.py
========================================
Scraper de lesiones y sanciones desde BeSoccer (es.besoccer.com).

BeSoccer registra bajas más amplias que Transfermarkt: molestias, virus,
compromisos internacionales, contusiones, etc. La página agrupa entradas
por mes + sección "Estado actual".

Rutas estándar (`utils.data_paths`):
    data/raw/<comp>/<season>/besoccer/injuries/<team>.json
    data/clean/<comp>/<season>/besoccer/injuries.csv

Uso:
    python -m scrapers.besoccer_injuries_scraper
    python -m scrapers.besoccer_injuries_scraper --team espanyol-barcelona
    python -m scrapers.besoccer_injuries_scraper --season 2025
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests as cr

from scrapers.transfermarkt_scraper import LEAGUE_CODE, get_league_teams
from utils.batch import generate_batch_id
from utils.data_paths import save_clean_csv, save_raw_json

log = logging.getLogger(__name__)

BASE_URL = "https://es.besoccer.com/equipo/lesionados-sancionados"
REQUEST_TIMEOUT = 30
DELAY_MIN = 1.0
DELAY_MAX = 2.5

# Transfermarkt slug → BeSoccer slug (cuando la heurística falla).
TM_TO_BESOCCER: dict[str, str] = {
    "fc-barcelona": "barcelona",
    "espanyol-barcelona": "espanyol",
    "fc-villarreal": "villarreal",
    "real-sociedad-san-sebastian": "real-sociedad",
    "fc-valencia": "valencia-cf",
    "fc-girona": "girona-fc",
    "ca-osasuna": "osasuna",
    "real-betis-sevilla": "betis",
    "celta-vigo": "celta",
    "fc-getafe": "getafe",
    "fc-sevilla": "sevilla",
    "fc-elche": "elche",
    "ud-levante": "levante",
    "rcd-mallorca": "mallorca",
    "deportivo-alaves": "deportivo",
}


def _extract_player_id(href: str) -> Optional[int]:
    m = re.search(r"-(\d+)$", (href or "").rstrip("/"))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _fetch_page(url: str) -> Optional[str]:
    try:
        r = cr.get(url, impersonate="chrome124", timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            return r.text
        log.warning("HTTP %s para %s", r.status_code, url)
    except Exception as e:
        log.warning("Error al descargar %s: %s", url, e)
    return None


def _slug_candidates(tm_slug: str) -> list[str]:
    if tm_slug in TM_TO_BESOCCER:
        return [TM_TO_BESOCCER[tm_slug]]

    candidates: list[str] = []
    if tm_slug in TM_TO_BESOCCER.values():
        candidates.append(tm_slug)

    stripped = tm_slug
    for prefix in ("rcd-", "cd-", "ud-", "fc-"):
        if stripped.startswith(prefix):
            candidates.append(stripped[len(prefix):])
    candidates.append(tm_slug)

    # dedupe preserving order
    seen: set[str] = set()
    out: list[str] = []
    for slug in candidates:
        if slug and slug not in seen:
            seen.add(slug)
            out.append(slug)
    return out


def resolve_besoccer_slug(tm_slug: str) -> Optional[str]:
    """Resuelve el slug de BeSoccer probando variantes hasta obtener HTTP 200."""
    for slug in _slug_candidates(tm_slug):
        url = f"{BASE_URL}/{slug}"
        html = _fetch_page(url)
        if html is not None:
            return slug
        time.sleep(random.uniform(0.5, 1.0))
    return None


def parse_team_injuries(html: str) -> list[dict]:
    """Parsea todas las entradas `data-cy="injury"` con su sección mensual."""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict] = []

    for anchor in soup.select('a.item-box[data-cy="injury"]'):
        section_el = anchor.find_previous(["h2", "h3", "h4"])
        section = section_el.get_text(strip=True) if section_el else None
        href = anchor.get("href") or ""
        main = anchor.select_one(".main-text")
        sub1 = anchor.select_one(".sub-text1")
        sub2 = anchor.select_one(".sub-text2")
        rows.append({
            "player_name": main.get_text(strip=True) if main else None,
            "injury_type": sub1.get_text(strip=True) if sub1 else None,
            "expected_return": sub2.get_text(strip=True) if sub2 else None,
            "section": section,
            "player_id_bs": _extract_player_id(href),
            "player_url": href or None,
        })
    return rows


def fetch_team_injuries(besoccer_slug: str) -> list[dict]:
    url = f"{BASE_URL}/{besoccer_slug}"
    html = _fetch_page(url)
    if not html:
        return []
    return parse_team_injuries(html)


def transform_injuries(
    injuries_raw: list[dict],
    *,
    team_slug: str,
    besoccer_slug: str,
    season_label: str,
    batch_id: str,
) -> pd.DataFrame:
    rows = []
    for inj in injuries_raw:
        rows.append({
            "team_slug": team_slug,
            "besoccer_team_slug": besoccer_slug,
            "season": season_label.replace("_", "/"),
            "player_name": inj.get("player_name"),
            "player_id_bs": inj.get("player_id_bs"),
            "injury_type": inj.get("injury_type"),
            "expected_return": inj.get("expected_return"),
            "section": inj.get("section"),
            "player_url": inj.get("player_url"),
            "batch_id": batch_id,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["player_id_bs"] = pd.to_numeric(df["player_id_bs"], errors="coerce").astype("Int64")
    return df.reset_index(drop=True)


def scrape_besoccer_injuries(
    competition_name: str = "La Liga",
    season: int = 2025,
    season_label: Optional[str] = None,
    teams: Optional[dict[str, int]] = None,
    team_filter: Optional[str] = None,
) -> list[dict]:
    """
    Descarga lesiones BeSoccer para todos los equipos (o uno filtrado).

    Returns:
        Lista plana de dicts crudos con metadatos de equipo.
    """
    if season_label is None:
        season_label = f"{season}_{season + 1}"

    from scripts.competitions import get_competition_slug_transfermarkt

    tm_slug = get_competition_slug_transfermarkt(competition_name) or "laliga"
    if not teams:
        teams_list = get_league_teams(season, tm_slug, LEAGUE_CODE)
        teams = {t["team_slug"]: t["team_id"] for t in teams_list}

    if team_filter:
        if team_filter not in teams:
            raise ValueError(f"Equipo no encontrado en TM: {team_filter}")
        teams = {team_filter: teams[team_filter]}

    batch_id = generate_batch_id()
    scraped_at = datetime.now(timezone.utc).isoformat()
    all_injuries: list[dict] = []
    failed: list[str] = []

    print("=" * 55)
    print(f"  BeSoccer injuries — {competition_name} {season_label}")
    print("=" * 55)

    for tm_team_slug in teams:
        print(f"\n[INFO] Equipo TM: {tm_team_slug}")
        bs_slug = resolve_besoccer_slug(tm_team_slug)
        if not bs_slug:
            log.error("Sin slug BeSoccer para %s", tm_team_slug)
            failed.append(tm_team_slug)
            continue

        injuries = fetch_team_injuries(bs_slug)
        for row in injuries:
            row.update({
                "team_slug": tm_team_slug,
                "besoccer_team_slug": bs_slug,
                "season": season_label.replace("_", "/"),
                "batch_id": batch_id,
                "scraped_at": scraped_at,
            })
        all_injuries.extend(injuries)

        save_raw_json(
            competition_name, season_label, "besoccer",
            tm_team_slug,
            {
                "batch_id": batch_id,
                "scraped_at": scraped_at,
                "team_slug": tm_team_slug,
                "besoccer_team_slug": bs_slug,
                "injuries": injuries,
            },
            subdir="injuries",
        )
        print(f"  [OK] {bs_slug}: {len(injuries)} entradas")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    if failed:
        print(f"\n  [WARNING] Sin datos BeSoccer: {failed}")

    if all_injuries:
        by_team: dict[str, list[dict]] = {}
        for inj in all_injuries:
            by_team.setdefault(inj["team_slug"], []).append(inj)

        frames = [
            transform_injuries(
                rows,
                team_slug=team,
                besoccer_slug=rows[0]["besoccer_team_slug"],
                season_label=season_label,
                batch_id=batch_id,
            )
            for team, rows in by_team.items()
        ]
        df = pd.concat(frames, ignore_index=True)
        csv_path = save_clean_csv(
            competition_name, season_label, "besoccer", "injuries", df,
        )
        print(f"\n  CSV lesiones: {csv_path} ({len(df)} filas)")

    return all_injuries


def main() -> None:
    parser = argparse.ArgumentParser(description="Scraper de lesiones BeSoccer")
    parser.add_argument("--competition", default="La Liga")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--team", help="Slug TM del equipo (p.ej. espanyol-barcelona)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    scrape_besoccer_injuries(
        competition_name=args.competition,
        season=args.season,
        team_filter=args.team,
    )


if __name__ == "__main__":
    main()
