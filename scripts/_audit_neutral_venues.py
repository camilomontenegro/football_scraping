"""Auditoría: venue WhoScored vs estadios TM de local y visitante."""
from __future__ import annotations

import re
import sys
import unicodedata
from collections import Counter
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from loaders.common import engine

DOMESTIC = {
    "La Liga", "Premier League", "Bundesliga", "Serie A",
    "Ligue 1", "Eredivisie", "Primeira Liga",
}
EURO = {"Champions League", "Europa League", "Europa Conference League"}


def _normalize(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", "", ascii_str.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _names_match(a: str | None, b: str | None, threshold: float = 0.45) -> bool:
    na, nb = _normalize(a or ""), _normalize(b or "")
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    wa, wb = set(na.split()), set(nb.split())
    if not wa or not wb:
        return False
    return len(wa & wb) / len(wa | wb) >= threshold


def _team_stadium(stadiums: list[dict], team_id: int | None, season: str) -> dict | None:
    if not team_id:
        return None
    candidates = [s for s in stadiums if s["canonical_team_id"] == team_id]
    for s in candidates:
        if s["valid_from_season"] <= season <= s["valid_to_season"]:
            return s
    return max(candidates, key=lambda x: x["valid_to_season"]) if candidates else None


def main():
    with engine.connect() as conn:
        stadiums = [dict(r) for r in conn.execute(text("""
            SELECT stadium_name, canonical_team_id, valid_from_season, valid_to_season
            FROM dim_stadium WHERE stadium_name IS NOT NULL
        """)).mappings()]

        rows = [dict(r) for r in conn.execute(text("""
            SELECT m.match_id, m.match_date, m.season, m.venue_name,
                   m.home_team_id, m.away_team_id,
                   c.canonical_name AS competition,
                   ht.canonical_name AS home, at.canonical_name AS away
            FROM dim_match m
            LEFT JOIN dim_competition c ON c.canonical_id = m.competition_id
            LEFT JOIN dim_team ht ON ht.canonical_id = m.home_team_id
            LEFT JOIN dim_team at ON at.canonical_id = m.away_team_id
            WHERE m.venue_name IS NOT NULL
        """)).mappings()]

        stadium_id_all = conn.execute(text(
            "SELECT COUNT(*) FROM dim_match WHERE stadium_id IS NOT NULL"
        )).scalar()

    cats: Counter[str] = Counter()
    away_domestic: list[dict] = []
    neither_samples: list[dict] = []
    comp_neither: Counter[str] = Counter()

    for r in rows:
        season = r["season"] or ""
        hs = _team_stadium(stadiums, r["home_team_id"], season)
        aws = _team_stadium(stadiums, r["away_team_id"], season)
        venue = r["venue_name"]

        hm = _names_match(venue, hs["stadium_name"]) if hs else False
        am = _names_match(venue, aws["stadium_name"]) if aws else False
        tm_home_is_team = hs and _normalize(hs["stadium_name"]) == _normalize(r["home"])

        if hm:
            cats["en_estadio_local"] += 1
        elif am:
            cats["solo_estadio_visitante"] += 1
            if r["competition"] in DOMESTIC:
                away_domestic.append(r)
        else:
            comp_neither[r["competition"] or "?"] += 1
            if tm_home_is_team or (aws and _normalize(aws["stadium_name"]) == _normalize(r["away"])):
                cats["tercer_estadio_tm_mal_nombre"] += 1
            elif r["competition"] in EURO:
                cats["tercer_estadio_copa_europea"] += 1
            else:
                cats["tercer_estadio_revisar"] += 1
            if len(neither_samples) < 20:
                neither_samples.append({**r, "home_st": hs, "away_st": aws})

    print("=== COBERTURA ===")
    print("  dim_match total:           17603 (referencia)")
    print("  con venue_name (WhoScored): %d" % len(rows))
    print("  con stadium_id (FK):        %d  <- casi todo es fallback home team" % stadium_id_all)
    print()
    print("=== venue_name vs estadios TM (local / visitante) ===")
    for k, v in cats.most_common():
        print("  %s: %d" % (k, v))
    print()
    print("=== tercer estadio por competicion ===")
    for comp, n in comp_neither.most_common(12):
        print("  %s: %d" % (comp, n))
    print()
    print("=== LIGA: venue = estadio visitante (posible error o COVID) ===")
    print("  Total: %d" % len(away_domestic))
    for r in away_domestic[:8]:
        hs = _team_stadium(stadiums, r["home_team_id"], r["season"] or "")
        aws = _team_stadium(stadiums, r["away_team_id"], r["season"] or "")
        print("  %s %s vs %s" % (r["match_date"], r["home"], r["away"]))
        print("    venue WS: %s" % r["venue_name"])
        print("    TM local: %s | TM visitante: %s" % (
            hs["stadium_name"] if hs else "?",
            aws["stadium_name"] if aws else "?",
        ))
    print()
    print("=== Muestras tercer estadio (no local ni visitante) ===")
    for r in neither_samples[:8]:
        hs, aws = r["home_st"], r["away_st"]
        print("  %s [%s] %s vs %s" % (r["match_date"], r["competition"], r["home"], r["away"]))
        print("    venue WS: %s" % r["venue_name"])
        print("    TM local: %s | TM visitante: %s" % (
            hs["stadium_name"] if hs else "?",
            aws["stadium_name"] if aws else "?",
        ))


if __name__ == "__main__":
    main()
