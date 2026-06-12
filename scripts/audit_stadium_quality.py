"""Auditoría de calidad dim_stadium — patrones de error."""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from loaders.common import engine
from loaders.stadium_loader import _looks_like_sponsor_label, city_looks_invalid
from scrapers.wikidata_stadium_enricher import _name_looks_like_club

_AUTO_GEO = re.compile(r"\(auto geocoded\)", re.I)
_STADIUM_WORDS = re.compile(
    r"\b(stadium|estadio|arena|park|field|ground|stadion|stade|stadio|"
    r"metropolitano|nou|olimpico|völlur|vollur|stadionul)\b",
    re.I,
)


def main() -> int:
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT s.*, t.canonical_name AS team_name
            FROM dim_stadium s
            LEFT JOIN dim_team t ON t.canonical_id = s.canonical_team_id
            ORDER BY s.team_slug, s.valid_from_season
        """)).mappings().all()

    issues: dict[str, list] = {
        "sponsor_city": [],
        "sponsor_address": [],
        "auto_geocoded": [],
        "club_as_name": [],
        "no_canonical_team": [],
        "no_coords": [],
        "city_equals_address_sponsor": [],
        "wiki_name_mismatch": [],
    }

    for r in rows:
        sid = r["stadium_id"]
        team = r["team_name"] or r["team_slug"] or ""
        name = (r["stadium_name"] or "").strip()
        city = (r["city"] or "").strip()
        addr = (r["address"] or "").strip()

        if r["canonical_team_id"] is None and r["id_transfermarkt_team"] and r["id_transfermarkt_team"] > 0:
            issues["no_canonical_team"].append((sid, r["team_slug"], r["id_transfermarkt_team"]))

        if r["latitude"] is None or r["longitude"] is None:
            issues["no_coords"].append((sid, r["team_slug"], name))

        if _AUTO_GEO.search(name):
            issues["auto_geocoded"].append((sid, r["team_slug"], name))

        if name and _name_looks_like_club(name, team) and not _STADIUM_WORDS.search(name):
            issues["club_as_name"].append((sid, r["team_slug"], name))

        if city and city_looks_invalid(city, name, addr):
            issues["sponsor_city"].append((sid, r["team_slug"], city, name))

        if addr and _looks_like_sponsor_label(addr):
            issues["sponsor_address"].append((sid, r["team_slug"], addr, name))

        if city and addr and city == addr and _looks_like_sponsor_label(city):
            issues["city_equals_address_sponsor"].append((sid, r["team_slug"], city))

    print(f"Total filas: {len(rows)}\n")
    for kind, items in issues.items():
        print(f"=== {kind}: {len(items)} ===")
        for item in items[:15]:
            print(f"  {item}")
        if len(items) > 15:
            print(f"  ... +{len(items)-15} más")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
