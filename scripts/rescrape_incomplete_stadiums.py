"""
Re-scrapea temporadas de estadios detectadas como incompletas en la auditoría RAW.

Escribe en Desktop/stadiums (raw + clean), no en football_scraping/data.

    python -m scripts.rescrape_incomplete_stadiums --dry-run
    python -m scripts.rescrape_incomplete_stadiums
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT = Path(__file__).resolve().parents[1]
_STADIUMS_ROOT = Path(r"C:\Users\Ivan\Desktop\stadiums")

# Parches de rutas antes de importar el scraper
import utils.data_paths as _dp

_dp.RAW_ROOT = _STADIUMS_ROOT / "raw"
_dp.CLEAN_ROOT = _STADIUMS_ROOT / "clean"
_dp.CACHE_ROOT = _STADIUMS_ROOT / ".cache"
_dp.DATA_ROOT = _STADIUMS_ROOT
_dp.CACHE_ROOT.mkdir(parents=True, exist_ok=True)

from scripts.competitions import COMPETITIONS, get_competition
from scrapers.transfermarkt_stadiums_scraper import scrape_transfermarkt_stadiums
from utils.data_paths import slugify_competition

log = logging.getLogger(__name__)

# (nombre competición, año inicio temporada)
INCOMPLETE: list[tuple[str, int]] = [
    ("Bundesliga", 2020),
    ("Primeira Liga", 2020),
    ("Champions League", 2024),
    ("Champions League", 2025),
    ("Europa League", 2025),
    ("Europa Conference League", 2025),
]

# Mínimos esperados para validar tras el scrape (domésticas)
_EXPECTED_DOMESTIC = {
    "la_liga": 20,
    "premier_league": 20,
    "bundesliga": 18,
    "serie_a": 20,
    "ligue_1": 18,
    "eredivisie": 18,
    "primeira_liga": 18,
}


def _count_raw(comp: str, season_label: str) -> int:
    slug = slugify_competition(comp)
    d = _STADIUMS_ROOT / "raw" / slug / season_label / "transfermarkt" / "stadiums"
    return len(list(d.glob("*.json"))) if d.exists() else 0


def _league_code(comp_name: str) -> str | None:
    cfg = get_competition(comp_name) or COMPETITIONS.get(comp_name)
    if not cfg:
        return None
    return cfg.get("sources", {}).get("transfermarkt", {}).get("league_code")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", nargs="*", help="Filtrar: 'Bundesliga:2020' 'Ligue 1:2026'")
    args = ap.parse_args()

    jobs = INCOMPLETE
    if args.only:
        filt = set(args.only)
        jobs = [
            (c, y) for c, y in INCOMPLETE
            if f"{c}:{y}" in filt or f"{slugify_competition(c)}:{y}" in filt
        ]

    print(f"Destino: {_STADIUMS_ROOT}")
    print(f"Trabajos: {len(jobs)}\n")

    results: list[tuple[str, int, int, int]] = []
    for comp_name, season_year in jobs:
        season_label = f"{season_year}_{season_year + 1}"
        before = _count_raw(comp_name, season_label)
        lc = _league_code(comp_name)
        if not lc:
            log.error("Sin league_code TM para %s", comp_name)
            continue

        print(f"── {comp_name} {season_label} (antes: {before} JSON) ──")
        if args.dry_run:
            results.append((comp_name, season_year, before, before))
            continue

        try:
            scrape_transfermarkt_stadiums(
                competition_name=comp_name,
                league_code=lc,
                season=season_year,
                season_label=season_label,
                full_refresh=True,
            )
        except Exception as exc:
            log.error("Fallo %s %s: %s", comp_name, season_label, exc)
            continue

        after = _count_raw(comp_name, season_label)
        results.append((comp_name, season_year, before, after))
        print(f"   → {after} JSON (+{after - before})\n")

    print("\n=== RESUMEN ===")
    for comp_name, sy, before, after in results:
        slug = slugify_competition(comp_name)
        exp = _EXPECTED_DOMESTIC.get(slug)
        flag = ""
        if exp and after < exp:
            flag = f"  [!] esperado ≥{exp}"
        print(f"  {comp_name} {sy}/{sy+1}: {before} → {after}{flag}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
