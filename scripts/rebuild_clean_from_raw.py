"""Regenera data/clean desde data/raw sin volver a scrapear."""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts._audit_clean_coverage import EXPECTED, count_csv_rows, sources_for_comp
from utils.data_paths import RAW_ROOT, CLEAN_ROOT, raw_dir, save_clean_csv, slugify_competition
from wizard.competitions import COMPETITIONS, WORKING_COMPETITION_NAMES

log = logging.getLogger(__name__)


def _comp_from_slug(slug: str) -> str | None:
    for name in COMPETITIONS:
        if slugify_competition(name) == slug:
            return name
    return None


def _folder_to_ws(season_folder: str) -> str:
    parts = season_folder.split("_")
    if len(parts) == 2 and len(parts[1]) == 4:
        return f"{parts[0]}/{parts[1][-2:]}"
    return season_folder.replace("_", "/")


def _raw_match_dirs(slug: str, season: str, source: str) -> int:
    p = RAW_ROOT / slug / season / source / "matches"
    if not p.is_dir():
        return 0
    return sum(1 for x in p.iterdir() if x.is_dir())


def _clean_ok(comp: str, season: str, source: str) -> bool:
    if source not in sources_for_comp(comp):
        return True
    d = CLEAN_ROOT / slugify_competition(comp) / season / source
    if not d.is_dir():
        return False
    missing = [f for f in EXPECTED[source] if not (d / f).exists()]
    if missing:
        return False
    if source == "whoscored":
        return count_csv_rows(d / "events.csv") > 0
    if source == "sofascore":
        clean_m = count_csv_rows(d / "matches.csv")
        if clean_m == 0:
            return False
        slug = slugify_competition(comp)
        raw_n = _raw_match_dirs(slug, season, source)
        if raw_n and clean_m < raw_n * 0.85:
            return False
        return True
    if source == "understat":
        return count_csv_rows(d / "matches.csv") > 0
    if source == "transfermarkt":
        return count_csv_rows(d / "players.csv") > 0
    return True


def rebuild_sofascore(comp: str, season: str) -> bool:
    from scrapers.sofascore_scraper import (
        _collect_events_from_raw,
        _collect_shots_from_raw,
        extract_players,
        extract_teams,
        transform_events,
        transform_matches,
        transform_shots,
    )

    raw_base = raw_dir(comp, season, "sofascore")
    fixtures_path = raw_base / "fixtures.json"
    matches_dir = raw_base / "matches"
    if not matches_dir.is_dir() or not any(matches_dir.iterdir()):
        return False

    if fixtures_path.exists():
        payload = json.loads(fixtures_path.read_text(encoding="utf-8"))
        matches = payload if isinstance(payload, list) else payload.get("events", [])
    else:
        log.warning("  SS %s %s: sin fixtures.json, omito", comp, season)
        return False

    if not matches:
        return False

    all_shots = _collect_shots_from_raw(matches_dir)
    all_events = _collect_events_from_raw(matches_dir)
    df_matches = transform_matches(matches)
    df_shots = transform_shots(all_shots)
    df_events = transform_events(all_events)
    df_teams = extract_teams(matches)
    df_players = extract_players(
        df_shots, df_events, df_teams, competition=comp, season=season,
    )

    wrote = False
    for name, df in (
        ("matches", df_matches),
        ("shots", df_shots),
        ("events", df_events),
        ("teams", df_teams),
        ("players", df_players),
    ):
        if df is None or df.empty:
            continue
        save_clean_csv(comp, season, "sofascore", name, df)
        wrote = True
    if wrote:
        log.info("[OK] SofaScore %s %s → clean (%d partidos fixtures)", comp, season, len(df_matches))
    return wrote


def rebuild_whoscored(comp: str, season: str) -> bool:
    from scrapers.whoscored_scraper import (
        _collect_whoscored_events_from_raw,
        _write_clean_season,
        extract_players_from_match,
        transform_events,
    )

    season_ws = _folder_to_ws(season)
    events = _collect_whoscored_events_from_raw(comp, season_ws)
    if not events:
        return False

    clean_ws = CLEAN_ROOT / slugify_competition(comp) / season / "whoscored"
    matches_csv = clean_ws / "matches.csv"
    players_csv = clean_ws / "players.csv"
    teams_csv = clean_ws / "teams.csv"

    if matches_csv.exists() and count_csv_rows(matches_csv) > 0:
        df_events = transform_events(events)
        if df_events is None or df_events.empty:
            return False
        save_clean_csv(comp, season, "whoscored", "events", df_events)
        log.info(
            "[OK] WhoScored %s %s → events.csv (%d filas)",
            comp, season, len(df_events),
        )
        return True

    raw_base = raw_dir(comp, season, "whoscored") / "matches"
    matches: list[dict] = []
    players: list[dict] = []
    teams: dict[int, dict] = {}
    for match_dir in sorted(raw_base.iterdir()):
        if not match_dir.is_dir():
            continue
        centre = match_dir / "match_centre.json"
        if not centre.exists():
            continue
        try:
            data = json.loads(centre.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        mid = match_dir.name
        meta_path = match_dir / "match_meta.json"
        match_date = None
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                match_date = meta.get("match_date")
            except (json.JSONDecodeError, OSError):
                pass
        matches.append({
            "whoscored_match_id": mid,
            "season": season_ws,
            "match_date": match_date,
            "home_team": (data.get("home") or {}).get("name"),
            "away_team": (data.get("away") or {}).get("name"),
        })
        for p in extract_players_from_match(data, comp):
            players.append(p)
        for side_key in ("home", "away"):
            side = data.get(side_key) or {}
            tid = side.get("teamId")
            if tid and tid not in teams:
                teams[tid] = {
                    "whoscored_team_id": tid,
                    "team_name": side.get("name"),
                    "season": season_ws,
                }

    if not matches:
        return False

    _write_clean_season(
        comp, season_ws, matches, events,
        players, list(teams.values()),
    )
    return True


def rebuild_understat(comp: str, season: str) -> bool:
    from scrapers.understat_scraper import (
        extract_players,
        extract_teams,
        transform_shots,
    )

    raw_base = raw_dir(comp, season, "understat")
    matches_path = raw_base / "matches.json"
    if not matches_path.exists():
        return False

    matches = json.loads(matches_path.read_text(encoding="utf-8"))
    if not matches:
        return False

    all_shots: list[dict] = []
    shots_root = raw_base / "matches"
    if shots_root.is_dir():
        for match_dir in shots_root.iterdir():
            if not match_dir.is_dir():
                continue
            shots_file = match_dir / "shots.json"
            if not shots_file.exists():
                continue
            try:
                shots = json.loads(shots_file.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            year = int(season.split("_")[0])
            for s in shots:
                s["season"] = year
            all_shots.extend(shots)

    df_matches = pd.DataFrame(matches)
    df_shots = pd.DataFrame(all_shots) if all_shots else pd.DataFrame()
    df_shots_clean = transform_shots(df_shots, df_matches) if not df_shots.empty else df_shots
    df_teams = extract_teams(df_matches)
    df_players = extract_players(df_shots, competition=comp, season=season) if not df_shots.empty else pd.DataFrame()

    save_clean_csv(comp, season, "understat", "matches", df_matches)
    if not df_shots_clean.empty:
        save_clean_csv(comp, season, "understat", "shots", df_shots_clean)
    if not df_players.empty:
        save_clean_csv(comp, season, "understat", "players", df_players)
    if not df_teams.empty:
        save_clean_csv(comp, season, "understat", "teams", df_teams)

    log.info(
        "[OK] Understat %s %s → clean (%d partidos, %d tiros)",
        comp, season, len(df_matches), len(df_shots_clean),
    )
    return True


def discover_targets(only_missing: bool = True) -> list[tuple[str, str, str]]:
    targets: list[tuple[str, str, str]] = []
    if not RAW_ROOT.is_dir():
        return targets

    for comp_dir in sorted(RAW_ROOT.iterdir()):
        if not comp_dir.is_dir():
            continue
        comp = _comp_from_slug(comp_dir.name)
        if not comp or comp not in WORKING_COMPETITION_NAMES:
            continue
        for season_dir in sorted(comp_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            season = season_dir.name
            for source in ("sofascore", "whoscored", "understat"):
                if source not in sources_for_comp(comp):
                    continue
                raw_n = _raw_match_dirs(comp_dir.name, season, source)
                if source == "understat":
                    has_raw = (season_dir / "understat" / "matches.json").exists()
                else:
                    has_raw = raw_n > 0 or (season_dir / source / "fixtures.json").exists()
                if not has_raw:
                    continue
                if only_missing and _clean_ok(comp, season, source):
                    continue
                targets.append((comp, season, source))
    return targets


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenera clean desde raw local")
    parser.add_argument("--all", action="store_true", help="Incluir fuentes ya OK en clean")
    parser.add_argument("--competition", type=str, default=None)
    parser.add_argument("--season", type=str, default=None)
    parser.add_argument("--source", choices=["sofascore", "whoscored", "understat"], default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    targets = discover_targets(only_missing=not args.all)
    if args.competition:
        targets = [t for t in targets if t[0] == args.competition]
    if args.season:
        targets = [t for t in targets if t[1] == args.season]
    if args.source:
        targets = [t for t in targets if t[2] == args.source]

    if not targets:
        log.info("Nada que regenerar (raw→clean al día).")
        return 0

    log.info("Regenerando %d bloques raw→clean...", len(targets))
    ok = 0
    builders = {
        "sofascore": rebuild_sofascore,
        "whoscored": rebuild_whoscored,
        "understat": rebuild_understat,
    }
    for comp, season, source in targets:
        log.info("→ %s / %s / %s", comp, season, source)
        try:
            if builders[source](comp, season):
                ok += 1
        except Exception as e:
            log.error("  ERROR %s %s %s: %s", comp, season, source, e)

    log.info("Hecho: %d/%d bloques escritos en data/clean/", ok, len(targets))
    return 0 if ok == len(targets) else 1


if __name__ == "__main__":
    raise SystemExit(main())
