"""
Auditoría de data/raw/<comp>/<season>/ antes de regenerar clean/.

Comprueba fixtures, carpetas por partido y JSON mínimos por fuente.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from wizard.competitions import WORKING_COMPETITION_NAMES, get_competition
from utils.data_paths import slugify_competition, raw_dir
from wizard.pipeline_runner import _is_international

SEASON = "2025_2026"

# Partidos de referencia (temporada completa)
REF_MATCHES = {
    "La Liga": 380,
    "Premier League": 380,
    "Bundesliga": 306,
    "Serie A": 380,
    "Ligue 1": 306,
    "Primeira Liga": 306,
    "Eredivisie": 306,
    "Champions League": 189,
    "Europa League": 189,
    "Europa Conference League": 189,
}

MIN_JSON = {
    "shots.json": 50,
    "events.json": 200,
    "lineups.json": 100,
    "events.json_ws": 500,  # whoscored events suele ser grande
    "match_meta.json": 20,
}


def _load_fixtures(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("events", "matches", "fixtures"):
            if isinstance(data.get(key), list):
                return data[key]
    return []


def _fixture_ids(fixtures: list[dict], id_key: str = "id") -> set[str]:
    out: set[str] = set()
    for m in fixtures:
        mid = m.get(id_key) or m.get("matchId") or m.get("game_id")
        if mid is not None:
            out.add(str(mid))
    return out


def _audit_sofascore(comp: str, slug: str) -> dict:
    base = raw_dir(comp, SEASON, "sofascore")
    fixtures_path = base / "fixtures.json"
    matches_dir = base / "matches"

    fixtures = _load_fixtures(fixtures_path)
    fix_ids = _fixture_ids(fixtures)

    dirs: list[Path] = []
    if matches_dir.is_dir():
        dirs = [d for d in matches_dir.iterdir() if d.is_dir() and d.name.isdigit()]

    dir_ids = {d.name for d in dirs}
    has_shots = has_events = has_lineups = complete = 0
    empty_shots = []

    for d in dirs:
        sp = d / "shots.json"
        ep = d / "events.json"
        lp = d / "lineups.json"
        ok_s = sp.exists() and sp.stat().st_size >= MIN_JSON["shots.json"]
        ok_e = ep.exists() and ep.stat().st_size >= MIN_JSON["events.json"]
        ok_l = lp.exists() and lp.stat().st_size >= MIN_JSON["lineups.json"]
        if ok_s:
            has_shots += 1
        elif sp.exists() and sp.stat().st_size < MIN_JSON["shots.json"]:
            empty_shots.append(d.name)
        if ok_e:
            has_events += 1
        if ok_l:
            has_lineups += 1
        if ok_s and ok_e and ok_l:
            complete += 1

    only_dirs = dir_ids - fix_ids
    only_fix = fix_ids - dir_ids

    return {
        "base_exists": base.exists(),
        "fixtures_n": len(fixtures),
        "fixture_ids_n": len(fix_ids),
        "match_dirs_n": len(dirs),
        "with_shots": has_shots,
        "with_events": has_events,
        "with_lineups": has_lineups,
        "complete_triple": complete,
        "dirs_not_in_fixtures": len(only_dirs),
        "fixtures_without_dir": len(only_fix),
        "sample_empty_shots": empty_shots[:5],
        "ref": REF_MATCHES.get(comp),
    }


def _audit_whoscored(comp: str) -> dict:
    base = raw_dir(comp, SEASON, "whoscored")
    fixtures_path = base / "fixtures.json"
    matches_dir = base / "matches"

    fixtures = _load_fixtures(fixtures_path)
    fix_ids = set()
    for m in fixtures:
        mid = m.get("id") or m.get("game_id") or m.get("matchId")
        if mid is not None:
            fix_ids.add(str(mid))

    dirs: list[Path] = []
    if matches_dir.is_dir():
        dirs = [d for d in matches_dir.iterdir() if d.is_dir() and d.name.isdigit()]

    dir_ids = {d.name for d in dirs}
    has_events = has_lineups = has_meta = complete = 0
    small_events = []

    for d in dirs:
        ep = d / "events.json"
        lp = d / "lineups.json"
        mp = d / "match_meta.json"
        ok_e = ep.exists() and ep.stat().st_size >= MIN_JSON["events.json_ws"]
        ok_l = lp.exists() and lp.stat().st_size >= MIN_JSON["lineups.json"]
        ok_m = mp.exists() and mp.stat().st_size >= MIN_JSON["match_meta.json"]
        if ok_e:
            has_events += 1
        elif ep.exists():
            small_events.append((d.name, ep.stat().st_size))
        if ok_l:
            has_lineups += 1
        if ok_m:
            has_meta += 1
        if ok_e and ok_l and ok_m:
            complete += 1

    return {
        "base_exists": base.exists(),
        "fixtures_n": len(fixtures),
        "fixture_ids_n": len(fix_ids),
        "match_dirs_n": len(dirs),
        "with_events": has_events,
        "with_lineups": has_lineups,
        "with_meta": has_meta,
        "complete_triple": complete,
        "dirs_not_in_fixtures": len(dir_ids - fix_ids),
        "fixtures_without_dir": len(fix_ids - dir_ids),
        "sample_small_events": small_events[:5],
        "ref": REF_MATCHES.get(comp),
    }


def _audit_understat(comp: str) -> dict:
    base = raw_dir(comp, SEASON, "understat")
    if not base.exists():
        return {"base_exists": False}
    files = list(base.rglob("*.json")) + list(base.rglob("*.csv"))
    return {
        "base_exists": True,
        "files_n": len(files),
        "has_matches_json": any(p.name == "matches.json" for p in files),
        "top_level": sorted(p.name for p in base.iterdir())[:12],
    }


def _audit_transfermarkt(comp: str) -> dict:
    base = raw_dir(comp, SEASON, "transfermarkt")
    if not base.exists():
        return {"base_exists": False}
    players = list((base / "players").glob("*.json")) if (base / "players").is_dir() else []
    injuries = list((base / "injuries").glob("*.json")) if (base / "injuries").is_dir() else []
    return {
        "base_exists": True,
        "player_json_n": len(players),
        "injury_json_n": len(injuries),
    }


def _status(n: int, ref: int | None, pct_min: float = 0.85) -> str:
    if ref is None:
        return "—"
    if n == 0:
        return "VACIO"
    if n >= ref * pct_min:
        return "OK"
    if n >= ref * 0.5:
        return "PARCIAL"
    return "BAJO"


def main() -> int:
    print("=" * 100)
    print(f"AUDITORIA RAW  temporada {SEASON}")
    print("=" * 100)

    problems: list[str] = []

    for comp in sorted(WORKING_COMPETITION_NAMES):
        slug = slugify_competition(comp)
        ref = REF_MATCHES.get(comp)
        conf = get_competition(comp) or {}
        print(f"\n### {comp}  (ref ~{ref or '?'} partidos)")

        # Transfermarkt
        tm = _audit_transfermarkt(comp)
        if tm.get("base_exists"):
            print(
                f"  [transfermarkt] players={tm['player_json_n']} json, "
                f"injuries={tm['injury_json_n']} json"
            )
        else:
            print("  [transfermarkt] sin carpeta raw")
            problems.append(f"{comp}/transfermarkt: sin raw")

        # SofaScore
        ss = _audit_sofascore(comp, slug)
        if ss["base_exists"]:
            st = _status(ss["complete_triple"], ref)
            print(
                f"  [sofascore] fixtures={ss['fixtures_n']} | dirs={ss['match_dirs_n']} | "
                f"completos(shots+events+lineups)={ss['complete_triple']} [{st}]"
            )
            print(
                f"             shots={ss['with_shots']} events={ss['with_events']} "
                f"lineups={ss['with_lineups']}"
            )
            if ss["fixtures_without_dir"]:
                print(f"             fixtures sin carpeta: {ss['fixtures_without_dir']}")
            if ss["dirs_not_in_fixtures"]:
                print(f"             carpetas huérfanas: {ss['dirs_not_in_fixtures']}")
            if st not in ("OK", "—"):
                problems.append(
                    f"{comp}/sofascore: solo {ss['complete_triple']}/{ref} partidos completos"
                )
            # fixtures vs dirs mismatch
            if ss["fixtures_n"] and abs(ss["fixtures_n"] - ss["match_dirs_n"]) > 5:
                problems.append(
                    f"{comp}/sofascore: fixtures({ss['fixtures_n']}) != dirs({ss['match_dirs_n']})"
                )
        else:
            print("  [sofascore] sin carpeta raw")
            problems.append(f"{comp}/sofascore: sin raw")

        # WhoScored
        if conf.get("sources", {}).get("whoscored", {}).get("tournament_id") is not None:
            ws = _audit_whoscored(comp)
            if ws["base_exists"]:
                st = _status(ws["complete_triple"], ref)
                print(
                    f"  [whoscored] fixtures={ws['fixtures_n']} | dirs={ws['match_dirs_n']} | "
                    f"completos={ws['complete_triple']} [{st}]"
                )
                if ws["fixtures_without_dir"]:
                    print(f"             fixtures sin carpeta: {ws['fixtures_without_dir']}")
                if ws["sample_small_events"]:
                    print(f"             events pequeños: {ws['sample_small_events']}")
                if st not in ("OK", "—"):
                    problems.append(
                        f"{comp}/whoscored: solo {ws['complete_triple']}/{ref} partidos completos"
                    )
            else:
                print("  [whoscored] sin carpeta raw")
                problems.append(f"{comp}/whoscored: sin raw")

        # Understat (domestic)
        if conf.get("sources", {}).get("understat", {}).get("league") and not _is_international(conf):
            us = _audit_understat(comp)
            if us.get("base_exists"):
                print(f"  [understat] archivos={us['files_n']} top={us.get('top_level')}")
            else:
                print("  [understat] sin carpeta raw (puede estar solo en clean/)")
                # not always a problem if imported to clean directly

    print("\n" + "=" * 100)
    print("VEREDICTO RAW (antes de regenerar clean)")
    print("=" * 100)
    if not problems:
        print("Sin problemas graves detectados en raw.")
    else:
        for p in problems:
            print(f"  ! {p}")
    print("=" * 100)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
